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
    safe fn Bun__NSolid__registerThread(context_id: u32);
    safe fn Bun__NSolid__unregisterThread(context_id: u32);
    safe fn Bun__NSolid__recordFatalError(global: &JSGlobalObject, error: JSValue);
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
/// async-signal-safe; a thread of its own does the reporting.
#[cfg(unix)]
mod signals {
    use core::ffi::{CStr, c_int, c_void};
    use core::sync::atomic::{AtomicI32, Ordering};

    const SIGNALS: [(c_int, &CStr); 3] =
        [(libc::SIGINT, c"SIGINT"), (libc::SIGTERM, c"SIGTERM"), (libc::SIGHUP, c"SIGHUP")];
    // A little longer than the report itself may take.
    const ACK_TIMEOUT_MS: c_int = 6_000;

    static TO_REPORTER: AtomicI32 = AtomicI32::new(-1);
    static FROM_REPORTER: AtomicI32 = AtomicI32::new(-1);

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
        let spawned = std::thread::Builder::new()
            .name("NSolidSignal".to_string())
            .spawn(move || report_signals(to_reporter[0], from_reporter[1]));
        if spawned.is_err() {
            return;
        }
        for (signal, _) in SIGNALS {
            // SAFETY: plain sigaction calls with zeroed, then filled, structs.
            unsafe {
                let mut current: libc::sigaction = core::mem::zeroed();
                if libc::sigaction(signal, core::ptr::null(), &mut current) != 0
                    || current.sa_sigaction != libc::SIG_DFL
                {
                    continue;
                }
                let mut action: libc::sigaction = core::mem::zeroed();
                action.sa_sigaction = on_signal as *const () as usize;
                libc::sigemptyset(&mut action.sa_mask);
                libc::sigaction(signal, &action, core::ptr::null_mut());
            }
        }
    }

    extern "C" fn on_signal(signal: c_int) {
        let byte = signal as u8;
        // SAFETY: write(2), poll(2), signal(2) and raise(3) are async-signal-safe.
        unsafe {
            if libc::write(TO_REPORTER.load(Ordering::Relaxed), (&raw const byte).cast::<c_void>(), 1) == 1 {
                let mut ack = libc::pollfd { fd: FROM_REPORTER.load(Ordering::Relaxed), events: libc::POLLIN, revents: 0 };
                libc::poll(&mut ack, 1, ACK_TIMEOUT_MS);
            }
            libc::signal(signal, libc::SIG_DFL);
            libc::raise(signal);
        }
    }

    fn report_signals(from_handler: c_int, to_handler: c_int) {
        let mut byte = 0u8;
        loop {
            // SAFETY: one byte into `byte`.
            if unsafe { libc::read(from_handler, (&raw mut byte).cast::<c_void>(), 1) } != 1 {
                return;
            }
            let signal = c_int::from(byte);
            let name = SIGNALS.iter().find(|(s, _)| *s == signal).map(|(_, name)| *name);
            super::report_exit(signal, name);
            // SAFETY: one byte from `byte`.
            unsafe { libc::write(to_handler, (&raw const byte).cast::<c_void>(), 1) };
        }
    }
}

/// Lists a JavaScript thread for the agent's per-thread metrics, from that
/// thread, once its VM runs. Nothing is kept unless the agent was started.
pub fn register_thread(context_id: u32) {
    if STARTED.load(Ordering::Relaxed) {
        Bun__NSolid__registerThread(context_id);
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
    register_thread(MAIN_CONTEXT_ID);
    #[cfg(unix)]
    signals::install();
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
