// `nsolid.otel`: N|Solid's bridge to the OpenTelemetry API (its lib/internal/otel). An application registers its
// `@opentelemetry/api`; while tracing is on, the API's global context manager and tracer provider are N|Solid's.
// The spans the application starts are then custom spans (N|Solid's type 16), which the agent exports with the
// rest, in the same traces as the HTTP and DNS spans: the context is the tracer's (internal/nsolid/tracing.ts).
//
// Only the API is bridged. OpenTelemetry instrumentations given to `registerInstrumentations` are enabled and
// disabled with tracing, but those that patch ES modules through loader hooks find no hooks to use in Bun.

const EventEmitter = require("node:events");
const tracing = require("internal/nsolid/tracing");

// OpenTelemetry's JS span kinds; OTLP's are one more.
const SPAN_KIND_INTERNAL = 0;
const INVALID_SPAN_CONTEXT = { traceId: "0".repeat(32), spanId: "0".repeat(16), traceFlags: 0 };
const LISTENER_ADDERS = ["addListener", "on", "once", "prependListener", "prependOnceListener"];

let registeredApi: any;
let instrumentations: any[] = [];

// The API the bridge works through: the application's, once registered.
function api() {
  return registeredApi;
}

// N|Solid's AsyncLocalStorage context manager, on the tracer's storage.
class ContextManager {
  #listeners = Symbol("OtListeners");

  active() {
    return tracing.storage.getStore() ?? registeredApi?.ROOT_CONTEXT ?? tracing.ROOT_CONTEXT;
  }

  with(context, fn, thisArg, ...args) {
    const callback = thisArg == null ? fn : fn.bind(thisArg);
    return tracing.storage.run(context, callback, ...args);
  }

  bind(context, target) {
    if (target instanceof EventEmitter) return this.#bindEmitter(context, target);
    if (typeof target === "function") return this.#bindFunction(context, target);
    return target;
  }

  enable() {
    return this;
  }

  // The storage also carries the HTTP and DNS spans, so it stays on.
  disable() {
    return this;
  }

  #bindFunction(context, target) {
    const manager = this;
    function contextWrapper(this: unknown, ...args) {
      return manager.with(context, () => target.$apply(this, args), undefined);
    }
    Object.defineProperty(contextWrapper, "length", {
      enumerable: false,
      configurable: true,
      writable: false,
      value: target.length,
    });
    return contextWrapper;
  }

  // Listeners added to `emitter` from now on run in `context`, and can still be removed.
  #bindEmitter(context, emitter) {
    if (emitter[this.#listeners] !== undefined) return emitter;
    emitter[this.#listeners] = { __proto__: null };
    const manager = this;
    for (const name of LISTENER_ADDERS) {
      const original = emitter[name];
      if (original === undefined) continue;
      emitter[name] = function (this: unknown, event, listener) {
        const map = (emitter[manager.#listeners] ??= { __proto__: null });
        const bound = manager.bind(context, listener);
        (map[event] ??= new WeakMap()).set(listener, bound);
        return original.$call(this, event, bound);
      };
    }
    for (const name of ["removeListener", "off"]) {
      const original = emitter[name];
      if (typeof original !== "function") continue;
      emitter[name] = function (this: unknown, event, listener) {
        const bound = emitter[manager.#listeners]?.[event]?.get(listener);
        return original.$call(this, event, bound || listener);
      };
    }
    const removeAll = emitter.removeAllListeners;
    if (typeof removeAll === "function") {
      emitter.removeAllListeners = function (this: unknown, event) {
        const map = emitter[manager.#listeners];
        if (map !== undefined) {
          if (arguments.length === 0) emitter[manager.#listeners] = { __proto__: null };
          else delete map[event];
        }
        return removeAll.$apply(this, arguments);
      };
    }
    return emitter;
  }
}

const contextManager = new ContextManager();

class Tracer {
  // N|Solid's: no span while tracing is off; a root decides sampling, a child takes its parent's.
  startSpan(name, options: any = {}, context?) {
    const otel = api();
    if (tracing.traceFlags === 0) return otel.trace.wrapSpanContext(INVALID_SPAN_CONTEXT);
    context ||= otel.context.active();
    let parent = options.root ? undefined : otel.trace.getSpanContext(context);
    if (!tracing.isValidSpanContext(parent)) parent = undefined;
    const span = new tracing.Span(
      String(name),
      (options.kind || SPAN_KIND_INTERNAL) + 1,
      tracing.SPAN_CUSTOM,
      parent,
      tracing.timeOf(options.startTime),
    );
    const { links, attributes } = options;
    if ($isArray(links)) for (const link of links) span.addLink(link);
    if (attributes) span.setAttributes(attributes);
    return span;
  }

  // As OpenTelemetry's: `fn` gets the span, and its result is returned.
  startActiveSpan(name, options, context, fn) {
    if (typeof options === "function") {
      fn = options;
      options = {};
      context = undefined;
    } else if (typeof context === "function") {
      fn = context;
      context = undefined;
    }
    const otel = api();
    const span = this.startSpan(name, options, context);
    const active = otel.trace.setSpan(context || otel.context.active(), span);
    return otel.context.with(active, fn, undefined, span);
  }
}

class TracerProvider {
  #tracers = new Map<string, Tracer>();

  getTracer(name, version = "") {
    const key = `${name}@${version}`;
    let tracer = this.#tracers.get(key);
    if (tracer === undefined) this.#tracers.set(key, (tracer = new Tracer()));
    return tracer;
  }
}

const tracerProvider = new TracerProvider();

function toggleInstrumentations(list: any[], on: boolean) {
  for (const instrumentation of list) {
    const toggle = on ? instrumentation?.enable : instrumentation?.disable;
    if (typeof toggle === "function") toggle.$call(instrumentation);
  }
}

// False when another OpenTelemetry SDK holds the API's globals.
function enableApi(): boolean {
  if (!registeredApi.context.setGlobalContextManager(contextManager)) return false;
  if (!registeredApi.trace.setGlobalTracerProvider(tracerProvider)) return false;
  toggleInstrumentations(instrumentations, true);
  return true;
}

function disableApi() {
  registeredApi.context.disable();
  registeredApi.trace.disable();
  toggleInstrumentations(instrumentations, false);
}

function register(otelApi) {
  if (registeredApi) {
    const error = new Error("OpenTelemetry JS API already registered");
    (error as any).code = "ERR_NSOLID_OTEL_API_ALREADY_REGISTERED";
    throw error;
  }
  registeredApi = otelApi;
  tracing.tracingListeners.push(on => {
    if (on) enableApi();
    else disableApi();
  });
  return tracing.traceFlags ? enableApi() : true;
}

function registerInstrumentations(list) {
  if (!list || typeof list !== "object") {
    throw $ERR_INVALID_ARG_TYPE("instrumentations", ["Array", "object"], list);
  }
  if (!$isArray(list)) list = [list];
  toggleInstrumentations(list, tracing.traceFlags !== 0);
  instrumentations = instrumentations.concat(list);
}

// The span `nsolid.logger` writes in: the registered API's active one, or the tracer's.
function activeSpanContext() {
  let context;
  if (registeredApi) context = registeredApi.trace.getSpanContext(registeredApi.context.active());
  else context = tracing.spanOf(tracing.activeContext())?.spanContext();
  return tracing.isValidSpanContext(context) ? context : undefined;
}

export default { register, registerInstrumentations, activeSpanContext };
