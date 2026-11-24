// What Bun.serve() publishes to node:diagnostics_channel, while the channels have subscribers:
// - `bun:serve:request:start` with `{ request, server }`, as the request's handler is called. The stores bound to the
//   channel are entered for the call, as `runStores` enters them, so the handler runs in them.
// - `bun:serve:request:end` with the same object, once the response is sent or the request is aborted, and with
//   `statusCode` (0 if no status was written) and `aborted` set on it.
// The server calls the handler through `serveHook`, and publishes the end from a microtask (ServeTrace.rs).

const { builtinChannel } = require("internal/shared");

const onStart = builtinChannel("bun:serve:request:start");
const onEnd = builtinChannel("bun:serve:request:end");
const callInFrameOf = $newCppFunction("AsyncContextFrame.cpp", "jsFunctionCallInFrameOf", 2);

// Keep in sync with ServeTrace.rs
const enum Op {
  Call,
  Take,
}

interface ServeContext {
  request: Request;
  server: unknown;
  statusCode?: number;
  aborted?: boolean;
}

// The request whose handler was called last, until the server takes it.
let started: ServeContext | undefined;

function serveHook(
  op: Op | ServeContext,
  handlerOrOutcome?: unknown,
  thisArg?: unknown,
  request?: Request,
  server?: unknown,
) {
  if (typeof op === "object") {
    const outcome = handlerOrOutcome as number;
    op.statusCode = outcome & 0xffff;
    op.aborted = outcome >> 16 !== 0;
    onEnd.publish(op);
    return;
  }
  if (op === Op.Take) {
    const context = started;
    started = undefined;
    return context;
  }
  const context: ServeContext = { request: request!, server };
  started = context;
  // A handler made in an AsyncLocalStorage context comes wrapped with it, as the server keeps it: that context is
  // entered first, and the channel's stores over it.
  if ($isCallable(handlerOrOutcome)) return callHandler(handlerOrOutcome, context, thisArg, request!, server);
  return callInFrameOf(handlerOrOutcome, handler => callHandler(handler, context, thisArg, request!, server));
}

function callHandler(handler: Function, context: ServeContext, thisArg: unknown, request: Request, server: unknown) {
  const channel = onStart.channel;
  if (channel === undefined) return handler.$call(thisArg, request, server);
  const scope = channel.withStoreScope(context);
  try {
    return handler.$call(thisArg, request, server);
  } finally {
    scope[Symbol.dispose]();
  }
}

export default {
  serveHook() {
    return onStart.hasSubscribers || onEnd.hasSubscribers ? serveHook : undefined;
  },
};
