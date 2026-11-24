//! The N|Solid agent thread.
//!
//! When `NSOLID_GRPC` or `NSOLID_SAAS` is set, the main thread starts one
//! thread with a `VirtualMachine` of its own. It runs
//! `internal/nsolid/agent` (src/js/internal/nsolid/agent.ts), which connects
//! to the N|Solid Console. Like the inspector's Debugger thread, it is never
//! joined, its VM is never torn down, and it does not keep the process alive.

use core::ffi::{CStr, c_char, c_void};
use core::sync::atomic::{AtomicBool, Ordering};

use bun_core::{String as BunString, StringView, env_var};

use crate::virtual_machine::VirtualMachine;
use crate::{self as jsc, JSGlobalObject, JSValue};

bun_core::declare_scope!(nsolid_agent, hidden);

static STARTED: AtomicBool = AtomicBool::new(false);

/// The main thread's `ScriptExecutionContext` identifier
/// (`ScriptExecutionContext::isMainThread()`).
const MAIN_CONTEXT_ID: u32 = 1;

/// The agent VM's, kept out of the sequence that numbers workers, so that
/// `worker.threadId` is the same with the agent as without it. `i32::MAX`
/// would ask for the next number of the sequence.
const AGENT_CONTEXT_ID: i32 = i32::MAX - 1;

unsafe extern "C" {
    safe fn Bun__NSolid__startAgent(
        global: &JSGlobalObject,
        main_path: &BunString,
        main_entry_ns: f64,
        agent_thread_ns: f64,
        agent_vm_ns: f64,
    );
    safe fn Bun__NSolid__registerThread(global: &JSGlobalObject, context_id: u32);
    safe fn Bun__NSolid__unregisterThread(context_id: u32);
    safe fn Bun__NSolid__noteRequestedCollection(full: bool);
    safe fn Bun__NSolid__recordFatalError(global: &JSGlobalObject, error: JSValue);
    #[cfg(unix)]
    fn Bun__NSolid__recordCrash(
        reason: *const u8,
        reason_len: usize,
        trace: *const u8,
        trace_len: usize,
    );
    fn Bun__NSolid__reportExit(code: i32, signal: *const c_char);
}

/// The process ends with `code` (and `signal`, when one ends it): the agent
/// reports it to the Console, and this waits until it has, 5 seconds at most.
pub fn report_exit(code: i32, signal: Option<&'static CStr>) {
    if STARTED.load(Ordering::Relaxed) {
        // SAFETY: `signal` is null or a 'static NUL-terminated string.
        unsafe { Bun__NSolid__reportExit(code, signal.map_or(core::ptr::null(), CStr::as_ptr)) };
    }
}

/// The main thread's uncaught exception, which its exit reports.
pub fn record_fatal_error(global: &JSGlobalObject, error: JSValue) {
    if STARTED.load(Ordering::Relaxed) {
        Bun__NSolid__recordFatalError(global, error);
    }
}

/// N|Solid reports an exit by SIGINT, SIGTERM or SIGHUP when nothing else
/// handles the signal, then lets the signal end the process. The handler only
/// writes the signal to a pipe and waits on another, which are
/// async-signal-safe; a thread of its own does the reporting. The handler
/// waits on that thread and on the agent's, so a signal the kernel gives
/// either of them goes on to the main thread. Masks can't keep it off the
/// agent's: its event loop waits with every signal unblocked.
///
/// A crash (a fault, an abort, a panic) is reported the same way: the crash
/// handler leaves its record in `CRASH` and sends `CRASHED` down the pipe.
#[cfg(unix)]
mod signals {
    use bun_collections::BoundedArray;
    use bun_sys::Fd;
    use core::cell::Cell;
    use core::ffi::{CStr, c_int};
    use core::sync::atomic::{AtomicI32, AtomicUsize, Ordering};

    const SIGNALS: [(c_int, &CStr); 3] = [
        (libc::SIGINT, c"SIGINT"),
        (libc::SIGTERM, c"SIGTERM"),
        (libc::SIGHUP, c"SIGHUP"),
    ];
    // The signals a crash ends the process with.
    const CRASH_SIGNALS: [(c_int, &CStr); 6] = [
        (libc::SIGSEGV, c"SIGSEGV"),
        (libc::SIGBUS, c"SIGBUS"),
        (libc::SIGILL, c"SIGILL"),
        (libc::SIGFPE, c"SIGFPE"),
        (libc::SIGTRAP, c"SIGTRAP"),
        (libc::SIGABRT, c"SIGABRT"),
    ];
    // What the pipe carries for a crash: no signal is 0.
    const CRASHED: u8 = 0;

    const CRASH_TEXT_BYTES: usize = 1024;
    struct Crash {
        signal: c_int,
        reason: BoundedArray<u8, CRASH_TEXT_BYTES>,
        trace: BoundedArray<u8, CRASH_TEXT_BYTES>,
    }
    static CRASH: bun_core::util::Once<Crash> = bun_core::util::Once::new();
    // A little longer than the report itself may take.
    const ACK_TIMEOUT_MS: c_int = 6_000;

    static TO_REPORTER: AtomicI32 = AtomicI32::new(-1);
    static FROM_REPORTER: AtomicI32 = AtomicI32::new(-1);
    static MAIN_THREAD: AtomicUsize = AtomicUsize::new(0);

    thread_local! {
        static WAITED_ON: Cell<bool> = const { Cell::new(false) };
    }

    /// Marks the calling thread as one the handler waits on.
    pub(super) fn handled_elsewhere() {
        WAITED_ON.set(true);
    }

    fn pipe() -> Option<[c_int; 2]> {
        let mut fds = [0; 2];
        // SAFETY: `fds` has room for both ends, which are then this module's.
        unsafe {
            if libc::pipe(fds.as_mut_ptr()) != 0 {
                return None;
            }
            for fd in fds {
                libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
            }
        }
        Some(fds)
    }

    pub(super) fn install() {
        let (Some(to_reporter), Some(from_reporter)) = (pipe(), pipe()) else {
            return;
        };
        TO_REPORTER.store(to_reporter[1], Ordering::Relaxed);
        FROM_REPORTER.store(from_reporter[0], Ordering::Relaxed);
        // SAFETY: no preconditions.
        MAIN_THREAD.store(unsafe { libc::pthread_self() } as usize, Ordering::Relaxed);
        let spawned = std::thread::Builder::new()
            .name("NSolidSignal".to_string())
            .spawn(move || report_signals(to_reporter[0], from_reporter[1]));
        if spawned.is_err() {
            return;
        }
        for (signal, _) in SIGNALS {
            let mut current: libc::sigaction = bun_core::ffi::zeroed();
            // SAFETY: a null new action only reads the current one into valid memory.
            if unsafe { libc::sigaction(signal, core::ptr::null(), &raw mut current) } != 0
                || current.sa_sigaction != libc::SIG_DFL
            {
                continue;
            }
            let mut action: libc::sigaction = bun_core::ffi::zeroed();
            action.sa_sigaction = on_signal as extern "C" fn(c_int) as usize;
            // SAFETY: both point to valid memory, and the handler is never unloaded.
            unsafe {
                libc::sigemptyset(&raw mut action.sa_mask);
                libc::sigaction(signal, &raw const action, core::ptr::null_mut());
            }
        }
    }

    /// Hands the reporter the crash and waits until it is reported, as the
    /// signal handler does. The crash handler calls it from the one thread
    /// left reporting; a crash of the agent's thread or the reporter's can't
    /// be reported.
    pub(super) fn report_crash(signal: c_int, reason: &[u8], trace: &[u8]) {
        if WAITED_ON.get() {
            return;
        }
        let mut crash = Crash {
            signal,
            reason: BoundedArray::default(),
            trace: BoundedArray::default(),
        };
        crash
            .reason
            .append_slice_assume_capacity(&reason[..reason.len().min(CRASH_TEXT_BYTES)]);
        crash
            .trace
            .append_slice_assume_capacity(&trace[..trace.len().min(CRASH_TEXT_BYTES)]);
        CRASH.get_or_init(|| crash);
        hand_to_reporter(CRASHED);
    }

    fn hand_to_reporter(byte: u8) {
        let to_reporter = Fd::from_native(TO_REPORTER.load(Ordering::Relaxed));
        // write(2) and poll(2) are async-signal-safe.
        if matches!(bun_sys::write(to_reporter, &[byte]), Ok(1)) {
            let mut ack = libc::pollfd {
                fd: FROM_REPORTER.load(Ordering::Relaxed),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: one pollfd.
            unsafe { libc::poll(&raw mut ack, 1, ACK_TIMEOUT_MS) };
        }
    }

    extern "C" fn on_signal(signal: c_int) {
        if WAITED_ON.get() {
            // SAFETY: pthread_kill(3) is async-signal-safe, and the main thread
            // outlives the process's signal handling.
            unsafe {
                libc::pthread_kill(
                    MAIN_THREAD.load(Ordering::Relaxed) as libc::pthread_t,
                    signal,
                )
            };
            return;
        }
        hand_to_reporter(signal as u8);
        // signal(2) and raise(3) are async-signal-safe.
        // SAFETY: no pointers.
        unsafe {
            libc::signal(signal, libc::SIG_DFL);
            libc::raise(signal);
        }
    }

    fn report_signals(from_handler: c_int, to_handler: c_int) {
        handled_elsewhere();
        let (from_handler, to_handler) =
            (Fd::from_native(from_handler), Fd::from_native(to_handler));
        let mut byte = [0u8];
        while matches!(bun_sys::read(from_handler, &mut byte), Ok(1)) {
            let mut signal = c_int::from(byte[0]);
            if byte[0] == CRASHED {
                let Some(crash) = CRASH.get() else { continue };
                signal = crash.signal;
                let (reason, trace) = (crash.reason.const_slice(), crash.trace.const_slice());
                // SAFETY: both slices are valid for their lengths.
                unsafe {
                    super::Bun__NSolid__recordCrash(
                        reason.as_ptr(),
                        reason.len(),
                        trace.as_ptr(),
                        trace.len(),
                    )
                };
            }
            let name = SIGNALS
                .iter()
                .chain(CRASH_SIGNALS.iter())
                .find(|(s, _)| *s == signal)
                .map(|(_, name)| *name);
            super::report_exit(signal, name);
            let _ = bun_sys::write(to_handler, &byte);
        }
    }
}

/// Lists a JavaScript thread for the agent's per-thread metrics, from that
/// thread, once its VM runs. Nothing is kept unless the agent was started.
pub fn register_thread(global: &JSGlobalObject, context_id: u32) {
    if STARTED.load(Ordering::Relaxed) {
        Bun__NSolid__registerThread(global, context_id);
    }
}

/// A collection that JS asked for on this thread (`Bun.gc`, `gc()`), which the
/// agent counts as N|Solid counts V8's forced ones.
pub fn note_requested_collection(sync: bool) {
    if STARTED.load(Ordering::Relaxed) {
        Bun__NSolid__noteRequestedCollection(sync);
    }
}

/// Takes a thread off the list, before its VM goes away.
pub fn unregister_thread(context_id: u32) {
    if STARTED.load(Ordering::Relaxed) {
        Bun__NSolid__unregisterThread(context_id);
    }
}

/// The startup marks the agent reports, in nanoseconds since the process
/// started, all on the clock `bun_core::start_time()` was read from.
#[derive(Clone, Copy)]
struct StartupMarks {
    main_entry: f64,
    agent_thread: f64,
    agent_vm: f64,
}

fn since_process_start_ns() -> f64 {
    (bun_core::time::nano_timestamp() - bun_core::start_time()).max(0) as f64
}

fn is_requested() -> bool {
    env_var::NSOLID_GRPC::get_not_empty().is_some()
        || env_var::NSOLID_SAAS::get_not_empty().is_some()
}

/// Starts the agent thread once, from the main thread, if the environment asks
/// for it. Workers and the agent's own VM never start one.
pub fn ensure_started(vm: &VirtualMachine) {
    if vm.is_main_thread.get() && is_requested() {
        start(vm);
    }
}

/// `nsolid.start()` with an endpoint, on the main thread: the agent starts if
/// the environment had not started it. The config reaches it through
/// `jsFunctionNSolidStart`.
#[unsafe(no_mangle)]
extern "C" fn Bun__NSolid__startFromScript(global: &JSGlobalObject) -> bool {
    let vm = global.bun_vm();
    if !vm.is_main_thread.get() {
        return false;
    }
    start(vm);
    true
}

/// When the process started, in milliseconds since the epoch.
#[unsafe(no_mangle)]
extern "C" fn Bun__NSolid__processStartMs() -> f64 {
    bun_core::start_time() as f64 / 1e6
}

fn start(vm: &VirtualMachine) {
    if STARTED.swap(true, Ordering::Relaxed) {
        return;
    }
    register_thread(vm.global(), MAIN_CONTEXT_ID);
    #[cfg(unix)]
    {
        signals::install();
        bun_core::Global::set_crash_report(signals::report_crash);
    }
    let main_entry = since_process_start_ns();
    let main_path = vm.main().unwrap_or_default().to_vec();
    // The agent VM builds its `process.env` from this copy, which is where it
    // reads the rest of its configuration.
    let env_map = match vm.env_loader().map.clone_with_allocator() {
        Ok(map) => map,
        Err(_) => return,
    };
    // A full `VirtualMachine::init` plus module load needs more than the
    // 2 MiB default stack, as for the Debugger thread.
    let spawned = std::thread::Builder::new()
        .name("NSolidAgent".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(move || agent_thread(env_map, main_path, main_entry));
    if spawned.is_err() {
        bun_core::scoped_log!(nsolid_agent, "could not spawn the agent thread");
    }
}

fn agent_thread(env_map: bun_dotenv::Map, main_path: Vec<u8>, main_entry: f64) {
    let agent_thread = since_process_start_ns();
    #[cfg(unix)]
    signals::handled_elsewhere();
    bun_core::asan::disable_leak_checks_on_current_thread();
    bun_core::Output::Source::configure_named_thread(bun_core::zstr!("NSolidAgent"));
    jsc::mark_binding();

    let mut log = bun_ast::Log::default();
    let mut env_loader = bun_dotenv::Loader::init_with_map(env_map);
    let options = crate::virtual_machine::InitOptions {
        env_loader: Some(core::ptr::NonNull::from(&mut env_loader)),
        is_main_thread: false,
        context_id: Some(AGENT_CONTEXT_ID),
        ..Default::default()
    };
    let Ok(vm) = VirtualMachine::init(&raw mut log, options) else {
        bun_core::scoped_log!(nsolid_agent, "could not create the agent VM");
        return;
    };
    vm.event_loop().ensure_waker();

    extern "C" fn start_trampoline(ctx: *mut c_void) {
        // SAFETY: `ctx` is `&mut slot` below; `hold_api_lock` calls this
        // synchronously on the same frame.
        let init = unsafe { (*ctx.cast::<Option<(Vec<u8>, StartupMarks)>>()).take() };
        let (main_path, marks) = init.expect("agent init");
        run(&main_path, marks);
    }
    let mut slot = Some((
        main_path,
        StartupMarks {
            main_entry,
            agent_thread,
            agent_vm: since_process_start_ns(),
        },
    ));
    #[allow(deprecated)]
    vm.global()
        .vm()
        .hold_api_lock((&raw mut slot).cast(), start_trampoline);
}

/// Runs inside `holdAPILock` on the agent thread: starts the JS agent with
/// the main thread's entry point, then spins this thread's event loop forever.
fn run(main_path: &[u8], marks: StartupMarks) -> ! {
    jsc::mark_binding();
    let this: &VirtualMachine = VirtualMachine::get();
    {
        let main_path = StringView::borrow_utf8(main_path);
        let _scope = this.enter_event_loop_scope();
        Bun__NSolid__startAgent(
            this.global(),
            &main_path,
            marks.main_entry,
            marks.agent_thread,
            marks.agent_vm,
        );
    }
    let _ = this.global().handle_rejected_promises();
    this.event_loop().tick();

    loop {
        while this.is_event_loop_alive() {
            this.tick();
            this.event_loop().auto_tick_active();
        }
        this.event_loop().tick_possibly_forever();
    }
}
