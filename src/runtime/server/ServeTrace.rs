//! Publishes a `Bun.serve()` request to the `bun:serve:request:start` and
//! `bun:serve:request:end` channels of `node:diagnostics_channel`, while they
//! have subscribers. See `internal/serve_channels.ts`.

use core::cell::Cell;

use bun_jsc::{JSGlobalObject, JSValue, JsResult, Strong};

/// Keep in sync with `internal/serve_channels.ts`.
#[repr(i32)]
#[derive(Clone, Copy)]
enum Op {
    Call,
    Take,
}

/// The hook of the channels, while they have subscribers.
#[inline]
pub(crate) fn hook(global: &JSGlobalObject) -> Option<JSValue> {
    let hook = bun_jsc::cpp::Bun__serveDiagnosticsHook(global);
    (!hook.is_empty()).then_some(hook)
}

/// Calls the `fetch` handler as `handler.call(server, request, server)`, with
/// the start of the request published around the call. (Route handlers go
/// through the hook in `ServerRouteList.cpp`.)
pub(crate) fn call_fetch_handler(
    global: &JSGlobalObject,
    handler: JSValue,
    request: JSValue,
    server: JSValue,
) -> JsResult<JSValue> {
    match hook(global) {
        Some(hook) => hook.call(
            global,
            JSValue::UNDEFINED,
            &[op(Op::Call), handler, server, request, server],
        ),
        None => handler.call(global, server, &[request, server]),
    }
}

/// A request whose start was published: what its end is published with.
pub(crate) struct ServeTrace {
    context: Strong,
    status: Cell<u16>,
}

impl ServeTrace {
    /// The request that the last handler call started, if it was published.
    pub(crate) fn take(global: &JSGlobalObject) -> Option<Self> {
        let hook = hook(global)?;
        let context = hook
            .call(global, JSValue::UNDEFINED, &[op(Op::Take)])
            .unwrap_or_else(|err| global.take_exception(err));
        context.is_object().then(|| Self {
            context: Strong::create(context, global),
            status: Cell::new(0),
        })
    }

    /// The status the response was written with.
    pub(crate) fn note_status(&self, status: u16) {
        self.status.set(status);
    }

    /// The response is done with, sent or `aborted`: its end is published
    /// from a microtask, since the response can end where JS can't run.
    pub(crate) fn end(self, global: &JSGlobalObject, aborted: bool) {
        let Some(hook) = hook(global) else { return };
        let outcome = i32::from(self.status.get()) | (i32::from(aborted) << 16);
        global.queue_microtask(
            hook,
            &[self.context.get(), JSValue::js_number_from_int32(outcome)],
        );
    }
}

fn op(op: Op) -> JSValue {
    JSValue::js_number_from_int32(op as i32)
}
