//! The N|Solid agent thread.
//!
//! When `NSOLID_GRPC` or `NSOLID_SAAS` is set, the main thread starts one
//! thread with a `VirtualMachine` of its own. It runs
//! `internal/nsolid/agent` (src/js/internal/nsolid/agent.ts), which connects
//! to the N|Solid Console. Like the inspector's Debugger thread, it is never
//! joined, its VM is never torn down, and it does not keep the process alive.

use core::ffi::c_void;
use core::sync::atomic::{AtomicBool, Ordering};

use bun_core::{String as BunString, StringView, env_var};

use crate::virtual_machine::VirtualMachine;
use crate::{self as jsc, JSGlobalObject};

bun_core::declare_scope!(nsolid_agent, hidden);

static STARTED: AtomicBool = AtomicBool::new(false);

unsafe extern "C" {
    safe fn Bun__NSolid__startAgent(
        global: &JSGlobalObject,
        main_path: &BunString,
        main_entry_ns: f64,
        agent_thread_ns: f64,
        agent_vm_ns: f64,
    );
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
    if !vm.is_main_thread.get() || !is_requested() {
        return;
    }
    if STARTED.swap(true, Ordering::Relaxed) {
        return;
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
    bun_core::asan::disable_leak_checks_on_current_thread();
    bun_core::Output::Source::configure_named_thread(bun_core::zstr!("NSolidAgent"));
    jsc::mark_binding();

    let mut log = bun_ast::Log::default();
    let mut env_loader = bun_dotenv::Loader::init_with_map(env_map);
    let options = crate::virtual_machine::InitOptions {
        env_loader: Some(core::ptr::NonNull::from(&mut env_loader)),
        is_main_thread: false,
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
