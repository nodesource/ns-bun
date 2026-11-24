// N|Solid's tracing, and its HTTP and DNS metrics, on each thread the agent watches (internal/nsolid/agent.ts).
// - The agent sets the trace flags and the sample rate on every thread, through `update(flags, sampleRate)`. The
//   flags are N|Solid's span types: a type's spans are made only while its bit is set.
// - Spans are made here and sampled at their root. Each ended span goes to the agent, which exports them over OTLP.
// - The HTTP and DNS counts and latencies are recorded whether or not tracing is on, as N|Solid's are.
// - The active span lives in one AsyncLocalStorage, under OpenTelemetry's context key, as in N|Solid.

const { AsyncLocalStorage } = require("node:async_hooks");
const diagnosticsChannel = require("node:diagnostics_channel");
const { threadId } = require("node:worker_threads");
const hooks = require("internal/nsolid/hooks");

const sendSpan = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidSendSpan", 1);
const record = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidRecord", 2);

// N|Solid's span types (nsolid.h, NSOLID_SPAN_TYPES), which `nsolid.span_type` carries.
const SPAN_DNS = 1;
const SPAN_HTTP_CLIENT = 4;
const SPAN_HTTP_SERVER = 8;
const SPAN_CUSTOM = 16;
// OTLP's span kinds.
const KIND_SERVER = 2;
const KIND_CLIENT = 3;
const STATUS_OK = 1;
const STATUS_ERROR = 2;
// Keep in sync with NSolidAgent.cpp
const enum Measure {
  HttpClient,
  HttpClientAbort,
  HttpServer,
  HttpServerAbort,
  DnsCall,
  DnsDone,
}
const SPAN_KEY = Symbol.for("OpenTelemetry Context Key SPAN");
// W3C trace context, as N|Solid reads it (lib/internal/nsolid_trace.js).
const TRACEPARENT = /^\s?((?!ff)[\da-f]{2})-((?![0]{32})[\da-f]{32})-((?![0]{16})[\da-f]{16})-([\da-f]{2})(-.*)?\s?$/;

let traceFlags = 0;
let sampleRate = 1;
let installed = false;
// Called when tracing turns on or off (nsolid.otel's bridge).
const tracingListeners: ((on: boolean) => void)[] = [];

class Context {
  _values: Map<symbol, unknown>;

  constructor(values?: Map<symbol, unknown>) {
    this._values = values ?? new Map();
  }

  getValue(key: symbol) {
    return this._values.get(key);
  }

  setValue(key: symbol, value: unknown) {
    const values = new Map(this._values);
    values.set(key, value);
    return new Context(values);
  }

  deleteValue(key: symbol) {
    const values = new Map(this._values);
    values.delete(key);
    return new Context(values);
  }
}

const ROOT_CONTEXT = new Context();
const storage = new AsyncLocalStorage<Context>();

function activeContext(): Context {
  return storage.getStore() ?? ROOT_CONTEXT;
}

function spanOf(context: Context): AnySpan | undefined {
  return context.getValue(SPAN_KEY) as AnySpan | undefined;
}

function withSpan(context: Context, span: AnySpan) {
  return context.setValue(SPAN_KEY, span);
}

interface SpanContext {
  traceId: string;
  spanId: string;
  traceFlags: number;
  isRemote?: boolean;
}

interface AnySpan {
  spanContext(): SpanContext;
}

const hexOf: string[] = [];
for (let i = 0; i < 256; i++) hexOf.push((i < 16 ? "0" : "") + i.toString(16));

function newId(bytes: number) {
  for (;;) {
    const random = crypto.getRandomValues(new Uint8Array(bytes));
    let id = "";
    let zero = true;
    for (let i = 0; i < bytes; i++) {
      id += hexOf[random[i]];
      if (random[i] !== 0) zero = false;
    }
    if (!zero) return id;
  }
}

// Wall-clock milliseconds since the epoch, from the monotonic clock.
function now() {
  return performance.timeOrigin + performance.now();
}

// A span from another process, from its `traceparent`.
class RemoteSpan implements AnySpan {
  #context: SpanContext;

  constructor(context: SpanContext) {
    this.#context = context;
  }

  spanContext() {
    return this.#context;
  }

  isRecording() {
    return false;
  }

  setAttribute() {
    return this;
  }

  setAttributes() {
    return this;
  }

  addEvent() {
    return this;
  }

  addLink() {
    return this;
  }

  setStatus() {
    return this;
  }

  updateName() {
    return this;
  }

  recordException() {}

  end() {}
}

function extract(traceparent: unknown): Context {
  const header = $isArray(traceparent) ? traceparent[0] : traceparent;
  if (typeof header !== "string") return ROOT_CONTEXT;
  const match = TRACEPARENT.exec(header);
  if (!match || (match[1] === "00" && match[5])) return ROOT_CONTEXT;
  const remote = new RemoteSpan({
    traceId: match[2],
    spanId: match[3],
    traceFlags: Number.parseInt(match[4], 16),
    isRemote: true,
  });
  return withSpan(ROOT_CONTEXT, remote);
}

function traceparentOf(span: Span) {
  return `00-${span.traceId}-${span.spanId}-${span.sampled ? "01" : "00"}`;
}

type Attributes = Record<string, unknown>;

class Span implements AnySpan {
  traceId: string;
  spanId: string;
  parentSpanId: string;
  sampled: boolean;
  name: string;
  kind: number;
  type: number;
  start: number;
  monotonicStart: number;
  attributes: Attributes = {};
  events: { name: string; time: number; attributes?: Attributes }[] = [];
  links: { traceId: string; spanId: string; attributes?: Attributes }[] = [];
  status: { code: number; message?: string } | undefined = undefined;
  ended = false;

  constructor(name: string, kind: number, type: number, parent: SpanContext | undefined, startTime?: number) {
    const valid = parent !== undefined && isValidSpanContext(parent);
    this.traceId = valid ? parent!.traceId.toLowerCase() : newId(16);
    this.parentSpanId = valid ? parent!.spanId.toLowerCase() : "";
    this.sampled = valid ? (parent!.traceFlags & 1) === 1 : Math.random() < sampleRate;
    this.spanId = newId(8);
    this.name = name;
    this.kind = kind;
    this.type = type;
    this.monotonicStart = performance.now();
    this.start = startTime ?? performance.timeOrigin + this.monotonicStart;
  }

  spanContext(): SpanContext {
    return { traceId: this.traceId, spanId: this.spanId, traceFlags: this.sampled ? 1 : 0, isRemote: false };
  }

  isRecording() {
    return !this.ended && this.sampled;
  }

  setAttribute(key: string, value: unknown) {
    if (!this.ended && value !== undefined && value !== null) this.attributes[key] = value;
    return this;
  }

  // As N|Solid merges them: a null value removes the key.
  setAttributes(attributes: Attributes) {
    if (this.ended || !attributes) return this;
    for (const key of Object.keys(attributes)) {
      const value = attributes[key];
      if (value === null) delete this.attributes[key];
      else if (value !== undefined) this.attributes[key] = value;
    }
    return this;
  }

  addEvent(name: string, attributesOrTime?: unknown, time?: unknown) {
    if (this.ended) return this;
    let attributes: Attributes | undefined;
    if (typeof attributesOrTime === "object" && attributesOrTime !== null && !$isArray(attributesOrTime)) {
      attributes = attributesOrTime as Attributes;
    } else if (attributesOrTime !== undefined) {
      time = attributesOrTime;
    }
    this.events.push({ name: String(name), time: timeOf(time) ?? now(), attributes });
    return this;
  }

  addLink(link: { context?: SpanContext; attributes?: Attributes }) {
    const context = link?.context;
    if (!this.ended && isValidSpanContext(context)) {
      this.links.push({ traceId: context!.traceId, spanId: context!.spanId, attributes: link.attributes });
    }
    return this;
  }

  setStatus(status: { code?: number; message?: unknown }) {
    if (this.ended || this.status?.code === STATUS_OK || !status) return this;
    const code = status.code;
    if (code === STATUS_OK) this.status = { code };
    else if (code === STATUS_ERROR) {
      this.status = typeof status.message === "string" ? { code, message: status.message } : { code };
    } else if (code === 0) this.status = undefined;
    return this;
  }

  updateName(name: string) {
    if (!this.ended) this.name = String(name);
    return this;
  }

  // N|Solid's recordException: an `exception` event, when there is a type or a message.
  recordException(error: unknown, time?: unknown) {
    const attributes: Attributes = {};
    if (typeof error === "string") attributes["exception.message"] = error;
    else if (error && typeof error === "object") {
      const { code, name, message, stack } = error as Record<string, unknown>;
      if (code) attributes["exception.type"] = String(code);
      else if (name) attributes["exception.type"] = String(name);
      if (message) attributes["exception.message"] = String(message);
      if (stack) attributes["exception.stacktrace"] = String(stack);
    }
    if (attributes["exception.type"] !== undefined || attributes["exception.message"] !== undefined) {
      this.addEvent("exception", attributes, time);
    }
  }

  end(endTime?: unknown) {
    if (this.ended) return;
    this.ended = true;
    const explicit = timeOf(endTime);
    const end = explicit ?? this.start + (performance.now() - this.monotonicStart);
    if (this.sampled && (traceFlags & this.type) !== 0) queue(this, Math.max(end, this.start));
  }
}

function isValidSpanContext(context: SpanContext | null | undefined): boolean {
  if (!context) return false;
  const { traceId, spanId } = context;
  return (
    typeof traceId === "string" &&
    /^[\da-f]{32}$/i.test(traceId) &&
    !/^0+$/.test(traceId) &&
    typeof spanId === "string" &&
    /^[\da-f]{16}$/i.test(spanId) &&
    !/^0+$/.test(spanId)
  );
}

// A time an OpenTelemetry API caller gives, as N|Solid reads it: performance.now() milliseconds (below half the time
// origin), epoch milliseconds, a Date, or [s, ns].
function timeOf(time: unknown): number | undefined {
  if (typeof time === "number") {
    if (!Number.isFinite(time) || time < 0) return undefined;
    return time < performance.timeOrigin / 2 ? performance.timeOrigin + time : time;
  }
  if (time instanceof Date) return time.getTime();
  if ($isArray(time) && time.length === 2) return time[0] * 1000 + time[1] / 1e6;
  return undefined;
}

function startSpan(
  name: string,
  kind: number,
  type: number,
  context: Context = activeContext(),
  options?: { root?: boolean; startTime?: number },
): Span {
  const parent = options?.root ? undefined : spanOf(context)?.spanContext?.();
  return new Span(name, kind, type, parent, options?.startTime);
}

function queue(span: Span, end: number) {
  let json: string;
  try {
    json = JSON.stringify({
      traceId: span.traceId,
      spanId: span.spanId,
      parentSpanId: span.parentSpanId,
      name: span.name,
      kind: span.kind,
      type: span.type,
      start: span.start,
      end,
      attributes: span.attributes,
      events: span.events,
      links: span.links,
      status: span.status,
      threadId,
    });
  } catch {
    // An attribute the application set can't be sent (a BigInt, a cycle): the span is dropped, as N|Solid drops
    // attributes it can't convert, rather than throwing into the application.
    return;
  }
  sendSpan(json);
}

const NO_SCOPE: Disposable = { [Symbol.dispose]() {} };

// Bun.serve's requests, from the channels it publishes (internal/serve_channels.ts).
const kServe = Symbol("nsolid.serve");

interface Exchange {
  started: number;
  span: Span | undefined;
  ended?: boolean;
}

function serveContextOf(message: any): Context {
  const { request } = message;
  const exchange: Exchange = { started: performance.now(), span: undefined };
  message[kServe] = exchange;
  if ((traceFlags & SPAN_HTTP_SERVER) === 0) return activeContext();
  const parent = extract(request.headers.get("traceparent"));
  const span = startSpan(`HTTP ${request.method}`, KIND_SERVER, SPAN_HTTP_SERVER, parent);
  span.setAttributes({ "http.method": request.method, "network.protocol.version": "1.1", "http.url": request.url });
  exchange.span = span;
  return withSpan(parent, span);
}

function serveEnded(message: any) {
  const exchange = message[kServe];
  if (exchange === undefined) return;
  record(message.aborted ? Measure.HttpServerAbort : Measure.HttpServer, performance.now() - exchange.started);
  const span = exchange.span;
  if (span === undefined) return;
  const { statusCode } = message;
  if (statusCode) span.setAttribute("http.status_code", statusCode);
  span.end();
}

// node:http's server: around the 'request' event.
// node:http keeps the agent's state of an exchange beside its objects, and calls the hooks below from its own code,
// so that a program sees no listener, property or read of the agent's on them.
const serverExchanges = new WeakMap<object, Exchange>();

function httpServerRequest(
  request: any,
  response: any,
  headersOf: (request: any) => Record<string, string> | undefined,
): Disposable {
  const exchange: Exchange = { started: performance.now(), span: undefined };
  serverExchanges.set(response, exchange);
  if ((traceFlags & SPAN_HTTP_SERVER) === 0) return NO_SCOPE;
  const headers = headersOf(request);
  const parent = extract(headers?.traceparent);
  const span = startSpan(`HTTP ${request.method}`, KIND_SERVER, SPAN_HTTP_SERVER, parent);
  const protocol = request.socket?.encrypted ? "https:" : "http:";
  span.setAttributes({
    "http.method": request.method,
    "network.protocol.version": request.httpVersion,
    "http.url": `${protocol}//${headers?.host || "localhost"}${request.url}`,
  });
  exchange.span = span;
  return storage.withScope(withSpan(parent, span));
}

function httpServerResponseEnd(response: any, closed: boolean, headersSent: (response: any) => boolean) {
  const exchange = serverExchanges.get(response);
  if (exchange === undefined || exchange.ended) return;
  exchange.ended = true;
  record(closed ? Measure.HttpServerAbort : Measure.HttpServer, performance.now() - exchange.started);
  const span = exchange.span;
  if (span === undefined) return;
  if (headersSent(response)) {
    const { statusCode, statusMessage } = response;
    span.setAttribute("http.status_code", statusCode);
    // Node fills in the status code's text where the application gave none.
    span.setAttribute("http.status_text", statusMessage || require("node:http").STATUS_CODES[statusCode]);
  }
  span.end();
}

// node:http's client, from its channels and httpClientResponseEnd.
const clientExchanges = new WeakMap<object, Exchange>();

function clientCreated({ request }: any) {
  const exchange: Exchange = { started: performance.now(), span: undefined };
  clientExchanges.set(request, exchange);
  if ((traceFlags & SPAN_HTTP_CLIENT) === 0) return;
  const span = startSpan(`HTTP ${request.method}`, KIND_CLIENT, SPAN_HTTP_CLIENT);
  const host = request.getHeader?.("host") ?? request.host;
  span.setAttributes({
    "http.method": request.method,
    "network.protocol.version": "1.1",
    "http.url": `${request.protocol}//${host}${request.path}`,
  });
  exchange.span = span;
  try {
    if (!request.hasHeader("traceparent")) request.setHeader("traceparent", traceparentOf(span));
  } catch {}
}

function clientResponse({ request, response }: any) {
  const exchange = clientExchanges.get(request);
  if (exchange === undefined) return;
  record(Measure.HttpClient, performance.now() - exchange.started);
  const span = exchange.span;
  if (span === undefined) return;
  span.setAttribute("http.status_code", response.statusCode);
  span.setAttribute("http.status_text", response.statusMessage);
}

function httpClientResponseEnd(request: any) {
  const exchange = clientExchanges.get(request);
  if (exchange === undefined || exchange.ended) return;
  exchange.ended = true;
  exchange.span?.end();
}

function clientError({ request, error }: any) {
  const exchange = clientExchanges.get(request);
  if (exchange === undefined || exchange.ended) return;
  exchange.ended = true;
  record(Measure.HttpClientAbort, 0);
  failed(exchange.span, error, { "http.error_name": error?.name, "http.error_message": error?.message });
}

function failed(span: Span | undefined, error: any, attributes?: Attributes) {
  if (span === undefined) return;
  if (attributes) span.setAttributes(attributes);
  span.setStatus({ code: STATUS_ERROR, message: error?.message });
  span.recordException(error);
  span.end();
}

// fetch(), from the undici channels Bun publishes (internal/undici_channels.ts).
const fetches = new WeakMap<object, Exchange>();

function fetchCreated({ request }: any) {
  const exchange: Exchange = { started: performance.now(), span: undefined };
  fetches.set(request, exchange);
  if ((traceFlags & SPAN_HTTP_CLIENT) === 0) return;
  const span = startSpan(`HTTP ${request.method}`, KIND_CLIENT, SPAN_HTTP_CLIENT);
  span.setAttributes({ "http.method": request.method, "http.url": `${request.origin}${request.path}` });
  exchange.span = span;
  try {
    request.addHeader("traceparent", traceparentOf(span));
  } catch {}
}

function fetchHeaders({ request, response }: any) {
  const exchange = fetches.get(request);
  const status = response?.statusCode;
  if (exchange === undefined || (status < 200 && status !== 101)) return;
  record(Measure.HttpClient, performance.now() - exchange.started);
  const span = exchange.span;
  if (span === undefined) return;
  span.setAttribute("http.status_code", status);
  if (status >= 400) span.setStatus({ code: STATUS_ERROR });
}

function fetchEnded({ request }: any) {
  const exchange = fetches.get(request);
  if (exchange === undefined) return;
  fetches.delete(request);
  exchange.span?.end();
}

function fetchError({ request, error }: any) {
  const exchange = fetches.get(request);
  if (exchange === undefined) return;
  fetches.delete(request);
  record(Measure.HttpClientAbort, 0);
  failed(exchange.span, error);
}

// node:dns, through the resolver its queries go to.
const DNS_OPERATIONS: Record<string, number> = {
  lookup: 0,
  lookupService: 1,
  resolve: 2,
  resolveAny: 2,
  resolveCaa: 2,
  resolveCname: 2,
  resolveMx: 2,
  resolveNaptr: 2,
  resolveNs: 2,
  resolvePtr: 2,
  resolveSoa: 2,
  resolveSrv: 2,
  resolveTxt: 2,
  reverse: 2,
};
const DNS_SPAN_NAMES = ["DNS lookup", "DNS lookup_service", "DNS resolve"];
const tracedResolvers = new WeakMap<object, object>();

function dnsResolver<T extends object>(resolver: T): T {
  let traced = tracedResolvers.get(resolver);
  if (traced === undefined) {
    traced = new Proxy(resolver, {
      get(target, key) {
        const value = Reflect.get(target, key);
        if (typeof value !== "function") return value;
        const operation = typeof key === "string" ? DNS_OPERATIONS[key] : undefined;
        if (operation === undefined) return value;
        return (...args: unknown[]) => traceQuery(operation, args, value.$apply(target, args));
      },
    });
    tracedResolvers.set(resolver, traced);
  }
  return traced as T;
}

function traceQuery(operation: number, args: unknown[], promise: Promise<unknown>) {
  record(Measure.DnsCall, 0);
  const started = performance.now();
  let span: Span | undefined;
  if ((traceFlags & SPAN_DNS) !== 0) {
    span = startSpan(DNS_SPAN_NAMES[operation], KIND_CLIENT, SPAN_DNS);
    span.setAttribute("dns.op_type", operation);
    if (operation === 1) {
      span.setAttribute("dns.address", JSON.stringify(args[0]));
      span.setAttribute("dns.port", Number(args[1]));
    } else span.setAttribute("dns.hostname", String(args[0]));
  }
  // The query's own promise goes back, watched from the side: a promise resolved with the answer would look up a
  // `then` on it once more, which code can see. node:dns chains on every query, so a rejection still reaches the
  // promise the application holds.
  promise.$then(
    result => {
      record(Measure.DnsDone, performance.now() - started);
      if (span === undefined) return;
      try {
        if (operation === 0) span.setAttribute("dns.address", JSON.stringify(lookupAnswer(result, args[1])));
        else if (operation === 1) span.setAttribute("dns.hostname", String((result as unknown[])?.[0]));
        else span.setAttribute("dns.address", JSON.stringify(result));
      } catch {}
      span.end();
    },
    () => {
      record(Measure.DnsDone, performance.now() - started);
      span?.end();
    },
  );
  return promise;
}

// A lookup's answer as N|Solid records it: the address the caller gets (node:dns orders the resolver's list by the
// lookup's `order`, then takes the first), or with `all` every address with its family.
function lookupAnswer(result: unknown, options: any) {
  if (!$isArray(result) || result.length === 0) return result;
  if (options?.all) return result.map(entry => ({ address: entry?.address, family: entry?.family }));
  const order = options?.order;
  let first = result[0];
  if (order === "ipv4first" || order === "ipv6first") {
    const family = order === "ipv4first" ? 4 : 6;
    first = result.find(entry => entry?.family === family) ?? first;
  }
  return first?.address;
}

function install() {
  installed = true;
  const serveStart = diagnosticsChannel.channel("bun:serve:request:start");
  serveStart.bindStore(storage, serveContextOf);
  diagnosticsChannel.subscribe("bun:serve:request:end", serveEnded);
  hooks.httpServerRequest = httpServerRequest;
  hooks.httpServerResponseEnd = httpServerResponseEnd;
  hooks.httpClientResponseEnd = httpClientResponseEnd;
  diagnosticsChannel.subscribe("http.client.request.created", clientCreated);
  diagnosticsChannel.subscribe("http.client.response.finish", clientResponse);
  diagnosticsChannel.subscribe("http.client.request.error", clientError);
  diagnosticsChannel.subscribe("undici:request:create", fetchCreated);
  diagnosticsChannel.subscribe("undici:request:headers", fetchHeaders);
  diagnosticsChannel.subscribe("undici:request:trailers", fetchEnded);
  diagnosticsChannel.subscribe("undici:request:error", fetchError);
  hooks.dnsResolver = dnsResolver;
}

/** From the agent: N|Solid's trace flags (span types), and the share of traces to sample. */
function update(flags: number, rate: number) {
  const was = traceFlags;
  traceFlags = flags >>> 0;
  sampleRate = rate;
  if (!installed) install();
  if (!was !== !traceFlags) for (const listener of tracingListeners) listener(traceFlags !== 0);
}

export default {
  update,
  ROOT_CONTEXT,
  storage,
  activeContext,
  spanOf,
  Span,
  isValidSpanContext,
  timeOf,
  get traceFlags() {
    return traceFlags;
  },
  tracingListeners,
  SPAN_CUSTOM,
};
