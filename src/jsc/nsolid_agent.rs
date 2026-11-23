//! The N|Solid agent thread.
//!
//! When `NSOLID_GRPC` or `NSOLID_SAAS` is set, the main thread starts one
//! thread with a `VirtualMachine` of its own. It runs
//! `internal/nsolid/agent` (src/js/internal/nsolid/agent.ts), which connects
//! to the N|Solid Console. Like the inspector's Debugger thread, it is never
//! joined, its VM is never torn down, and it does not keep the process alive.

use core::ffi::c_void;
use core::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use bun_core::{String as BunString, StringView, env_var};

use crate::virtual_machine::VirtualMachine;
use crate::{self as jsc, JSGlobalObject};

bun_core::declare_scope!(nsolid_agent, hidden);

static STARTED: AtomicBool = AtomicBool::new(false);
static AGENT_ID: OnceLock<[u8; 40]> = OnceLock::new();

unsafe extern "C" {
    safe fn Bun__NSolid__startAgent(global: &JSGlobalObject, agent_id: &BunString);
}

/// The id the Console knows this process by: 20 random bytes as 40 lowercase
/// hex digits, the same shape N|Solid uses. One per process.
pub fn agent_id() -> &'static [u8; 40] {
    AGENT_ID.get_or_init(|| {
        let mut raw = [0u8; 20];
        bun_boringssl::rand_bytes(&mut raw);
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut id = [0u8; 40];
        for (i, byte) in raw.iter().enumerate() {
            id[i * 2] = HEX[(byte >> 4) as usize];
            id[i * 2 + 1] = HEX[(byte & 0x0f) as usize];
        }
        id
    })
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
    // The agent VM builds its `process.env` from this copy, which is where it
    // reads the rest of its configuration.
    let env_map = match vm.env_loader().map.clone_with_allocator() {
        Ok(map) => map,
        Err(_) => return,
    };
    let id = agent_id();
    // A full `VirtualMachine::init` plus module load needs more than the
    // 2 MiB default stack, as for the Debugger thread.
    let spawned = std::thread::Builder::new()
        .name("NSolidAgent".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(move || agent_thread(env_map, id));
    if spawned.is_err() {
        bun_core::scoped_log!(nsolid_agent, "could not spawn the agent thread");
    }
}

fn agent_thread(env_map: bun_dotenv::Map, id: &'static [u8; 40]) {
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
        let id = unsafe { (*ctx.cast::<Option<&'static [u8; 40]>>()).take() };
        run(id.expect("agent id"));
    }
    let mut slot = Some(id);
    #[allow(deprecated)]
    vm.global()
        .vm()
        .hold_api_lock((&raw mut slot).cast(), start_trampoline);
}

/// Runs inside `holdAPILock` on the agent thread: hands the agent id to the
/// JS agent, then spins this thread's event loop forever.
fn run(id: &'static [u8; 40]) -> ! {
    jsc::mark_binding();
    let this: &VirtualMachine = VirtualMachine::get();
    {
        let id = StringView::borrow_utf8(id);
        let _scope = this.enter_event_loop_scope();
        Bun__NSolid__startAgent(this.global(), &id);
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
