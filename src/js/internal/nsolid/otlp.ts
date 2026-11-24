// The agent's metrics as OTLP `ResourceMetrics`, the way N|Solid's gRPC agent
// sends them: the metric names and units of the Console's catalog, in the
// `nsolid` instrumentation scope, counters as cumulative monotonic sums,
// thread metrics with `thread.id` and `thread.name`.

const os = require("node:os");

const SCOPE = { name: "nsolid", version: `nsolid-bun-v${process.versions.nsolid_bun}+bun-v${Bun.version}` };
const CUMULATIVE = 2;
// OpenTelemetry's names, where they differ from Node's.
const HOST_ARCH: Record<string, string> = { x64: "amd64", ia32: "x86", arm: "arm32" };
const OS_TYPE: Record<string, string> = { win32: "windows", sunos: "solaris" };

type Kind = "counter" | "gauge";
type ValueType = "int" | "double";

// [name, kind, unit, value type], in the catalog's order.
const processMetrics: [string, Kind, string, ValueType][] = [
  ["uptime", "counter", "s", "int"],
  ["systemUptime", "counter", "s", "int"],
  ["freeMem", "gauge", "byte", "int"],
  ["totalMem", "gauge", "byte", "int"],
  ["blockInputOpCount", "counter", "", "int"],
  ["blockOutputOpCount", "counter", "", "int"],
  ["ctxSwitchInvoluntaryCount", "counter", "", "int"],
  ["ctxSwitchVoluntaryCount", "counter", "", "int"],
  ["ipcReceivedCount", "counter", "", "int"],
  ["ipcSentCount", "counter", "", "int"],
  ["pageFaultHardCount", "counter", "", "int"],
  ["pageFaultSoftCount", "counter", "", "int"],
  ["signalCount", "counter", "", "int"],
  ["swapCount", "counter", "", "int"],
  ["rss", "gauge", "byte", "int"],
  ["load1m", "gauge", "", "double"],
  ["load5m", "gauge", "", "double"],
  ["load15m", "gauge", "", "double"],
  ["cpuUserPercent", "gauge", "", "double"],
  ["cpuSystemPercent", "gauge", "", "double"],
  ["cpuPercent", "gauge", "", "double"],
];

// JavaScriptCore's figures where they mean what N|Solid's do. V8's other heap
// figures (heap size limit, executable size, malloced memory, contexts) have
// no counterpart and are not sent.
const threadMetrics: [string, string, ValueType][] = [
  ["heapTotal", "byte", "int"],
  ["heapUsed", "byte", "int"],
  ["externalMem", "byte", "int"],
];

// N|Solid's event-loop and GC thread metrics, with the kinds and units it
// sends them with (src/nsolid.h). The collection durations go out as the
// `gcDurUs` summary.
const loopAndGcMetrics: [string, Kind, string, ValueType][] = [
  ["gcCount", "counter", "", "int"],
  ["gcForcedCount", "counter", "", "int"],
  ["gcFullCount", "counter", "", "int"],
  ["gcMajorCount", "counter", "", "int"],
  ["loopIdleTime", "gauge", "ns", "int"],
  ["loopIterations", "counter", "", "int"],
  ["loopIterWithEvents", "counter", "", "int"],
  ["eventsProcessed", "counter", "", "int"],
  ["eventsWaiting", "gauge", "", "int"],
  ["providerDelay", "gauge", "ns", "int"],
  ["processingDelay", "gauge", "ns", "int"],
  ["loopTotalCount", "counter", "", "int"],
  ["loopUtilization", "gauge", "", "double"],
  ["res5s", "gauge", "", "double"],
  ["res1m", "gauge", "", "double"],
  ["res5m", "gauge", "", "double"],
  ["res15m", "gauge", "", "double"],
  ["loopAvgTasks", "gauge", "", "double"],
  ["loopEstimatedLag", "gauge", "ms", "double"],
  ["loopIdlePercent", "gauge", "", "double"],
  ["dnsCount", "counter", "", "int"],
  ["httpClientAbortCount", "counter", "", "int"],
  ["httpClientCount", "counter", "", "int"],
  ["httpServerAbortCount", "counter", "", "int"],
  ["httpServerCount", "counter", "", "int"],
  ["activeHandles", "gauge", "", "int"],
  ["activeRequests", "gauge", "", "int"],
  ["tcpServerCreatedCount", "counter", "", "int"],
  ["tcpServerDestroyedCount", "counter", "", "int"],
  ["tcpSocketCreatedCount", "counter", "", "int"],
  ["tcpSocketDestroyedCount", "counter", "", "int"],
  ["udpSocketCreatedCount", "counter", "", "int"],
  ["udpSocketDestroyedCount", "counter", "", "int"],
  ["pipeServerCreatedCount", "counter", "", "int"],
  ["pipeServerDestroyedCount", "counter", "", "int"],
  ["pipeSocketCreatedCount", "counter", "", "int"],
  ["pipeSocketDestroyedCount", "counter", "", "int"],
  ["fsHandlesOpenedCount", "counter", "", "int"],
  ["fsHandlesClosedCount", "counter", "", "int"],
];

// The thread summaries N|Solid sends, as [name, unit, the figure its median is in, its 99th percentile's].
const summaries: [string, string, string, string][] = [
  ["gcDurUs", "us", "gcDurUsMedian", "gcDurUs99Ptile"],
  ["dns", "ms", "dnsMedian", "dns99Ptile"],
  ["httpClient", "ms", "httpClientMedian", "httpClient99Ptile"],
  ["httpServer", "ms", "httpServerMedian", "httpServer99Ptile"],
];
// What N|Solid's exporter keeps of a span: opentelemetry-cpp's default limit.
const MAX_SPAN_ATTRIBUTES = 128;
const SEVERITY_TEXT: Record<number, string> = { 5: "DEBUG", 9: "INFO", 13: "WARN", 17: "ERROR", 21: "FATAL" };

const nanos = (ms: number) => BigInt(Math.round(ms)) * 1_000_000n;

// Epoch milliseconds with a fraction, to the nanosecond: a double can't hold the product.
function preciseNanos(ms: number): bigint {
  const whole = Math.floor(ms);
  return BigInt(whole) * 1_000_000n + BigInt(Math.round((ms - whole) * 1e6));
}

function attribute(key: string, value: string | number) {
  return {
    key,
    value: typeof value === "number" ? { int_value: value } : { string_value: value },
  };
}

function point(value: number, valueType: ValueType, timeMs: number, startMs: number, attributes?: object[]) {
  const dataPoint: Record<string, any> = {
    start_time_unix_nano: nanos(startMs),
    time_unix_nano: nanos(timeMs),
  };
  if (valueType === "int") dataPoint.as_int = Math.max(0, Math.round(value));
  else dataPoint.as_double = value;
  if (attributes) dataPoint.attributes = attributes;
  return dataPoint;
}

function metric(name: string, kind: Kind, unit: string, dataPoints: object[]) {
  return kind === "counter"
    ? { name, unit, sum: { data_points: dataPoints, aggregation_temporality: CUMULATIVE, is_monotonic: true } }
    : { name, unit, gauge: { data_points: dataPoints } };
}

export interface Identity {
  agentId: string;
  app: string;
  appVersion: string | undefined;
  hostname: string;
  main: string;
  nodeEnv: string;
  tags: string[];
  processStartMs: number;
}

// N|Solid's resource (agents/otlp/src/otlp_common.cc), with Bun as the
// runtime. Until the Console reads a runtime of its own, nothing names
// N|Solid's version or Node's.
function resource(identity: Identity) {
  const cpus = os.cpus();
  const attributes = [
    attribute("service.name", identity.app),
    attribute("service.instance.id", identity.agentId),
    attribute("host.name", identity.hostname),
    attribute("host.arch", HOST_ARCH[process.arch] ?? process.arch),
    attribute("os.type", OS_TYPE[process.platform] ?? process.platform),
    attribute("process.pid", process.pid),
    attribute("process.executable.path", process.execPath),
    attribute("process.runtime.name", "bun"),
    attribute("process.runtime.version", Bun.version),
    attribute("process.creation.time", new Date(identity.processStartMs).toISOString()),
    attribute("process.title", process.title),
    attribute("main", identity.main),
    attribute("deployment.environment.name", identity.nodeEnv),
    attribute("cpuCores", cpus.length),
    attribute("host.cpu.model.name", cpus[0]?.model ?? ""),
    attribute("tagsString", identity.tags.join(",")),
  ];
  const { appVersion } = identity;
  if (appVersion) attributes.push(attribute("service.version", appVersion));
  try {
    attributes.push(attribute("process.owner", String(os.userInfo().username)));
  } catch {}
  return { attributes };
}

function threadAttributes(thread: Record<string, any>) {
  return [attribute("thread.id", thread.threadId), attribute("thread.name", thread.threadName)];
}

function resourceMetrics(identity: Identity, proc: Record<string, any>, threads: Record<string, any>[]) {
  const start = identity.processStartMs;
  const metrics: object[] = [];
  for (const [name, kind, unit, valueType] of processMetrics) {
    metrics.push(metric(name, kind, unit, [point(proc[name], valueType, proc.timestamp, start)]));
  }
  const withHeap = threads.filter(thread => thread.heapTotal !== undefined);
  for (const [name, unit, valueType] of threadMetrics) {
    const points = withHeap.map(thread =>
      point(
        thread[name === "externalMem" ? "external" : name],
        valueType,
        thread.timestamp,
        start,
        threadAttributes(thread),
      ),
    );
    if (points.length) metrics.push(metric(name, "gauge", unit, points));
  }
  const withLoop = threads.filter(thread => thread.loopIterations !== undefined);
  for (const [name, kind, unit, valueType] of loopAndGcMetrics) {
    const points = withLoop.map(thread =>
      point(thread[name], valueType, thread.timestamp, start, threadAttributes(thread)),
    );
    if (points.length) metrics.push(metric(name, kind, unit, points));
  }
  if (withLoop.length) {
    for (const [name, unit, median, p99] of summaries) {
      const points = withLoop.map(thread => ({
        start_time_unix_nano: nanos(start),
        time_unix_nano: nanos(thread.timestamp),
        count: 0n,
        sum: 0,
        quantile_values: [
          { quantile: 0.5, value: thread[median] },
          { quantile: 0.99, value: thread[p99] },
        ],
        attributes: threadAttributes(thread),
      }));
      metrics.push({ name, unit, summary: { data_points: points } });
    }
  }
  return { resource: resource(identity), scope_metrics: [{ scope: SCOPE, metrics }] };
}

// An attribute's value as N|Solid's exporter keeps it: a string, a boolean, a number, or an array of one of those.
// Anything else (objects, mixed or nested arrays, empty arrays) is left out.
function anyValue(value: unknown): object | undefined {
  switch (typeof value) {
    case "string":
      return { string_value: value };
    case "boolean":
      return { bool_value: value };
    case "number":
      return Number.isSafeInteger(value) ? { int_value: value } : { double_value: value };
  }
  if (!$isArray(value) || value.length === 0) return undefined;
  const type = typeof value[0];
  if (type !== "string" && type !== "boolean" && type !== "number") return undefined;
  for (const element of value) if (typeof element !== type) return undefined;
  if (type === "number" && !value.every(element => Number.isSafeInteger(element))) {
    return { array_value: { values: value.map(element => ({ double_value: element })) } };
  }
  return { array_value: { values: value.map(anyValue) } };
}

function keyValues(attributes: Record<string, unknown> | undefined, limit = Infinity) {
  const list: object[] = [];
  let dropped = 0;
  if (attributes) {
    for (const key of Object.keys(attributes)) {
      const value = anyValue(attributes[key]);
      if (!value) continue;
      if (list.length < limit) list.push({ key, value });
      else dropped++;
    }
  }
  return { list, dropped };
}

const bytesOf = (hex: string) => Buffer.from(hex, "hex");

export interface EndedSpan {
  traceId: string;
  spanId: string;
  parentSpanId: string;
  name: string;
  kind: number;
  type: number;
  start: number;
  end: number;
  attributes: Record<string, unknown>;
  events: { name: string; time: number; attributes?: Record<string, unknown> }[];
  links: { traceId: string; spanId: string; attributes?: Record<string, unknown> }[];
  status?: { code: number; message?: string };
  threadId: number;
}

// A span as N|Solid's exporter sends it, with its thread and N|Solid's span type last. A thread's name is sent
// only when it has one.
function span(ended: EndedSpan, threadName: string) {
  const { list: attributes, dropped } = keyValues(ended.attributes, MAX_SPAN_ATTRIBUTES - 3);
  if (threadName) attributes.push(attribute("thread.name", threadName));
  attributes.push(attribute("thread.id", ended.threadId), attribute("nsolid.span_type", ended.type));
  return {
    trace_id: bytesOf(ended.traceId),
    span_id: bytesOf(ended.spanId),
    parent_span_id: ended.parentSpanId ? bytesOf(ended.parentSpanId) : undefined,
    name: ended.name,
    kind: ended.kind,
    start_time_unix_nano: preciseNanos(ended.start),
    end_time_unix_nano: preciseNanos(ended.end),
    attributes,
    dropped_attributes_count: dropped || undefined,
    events: ended.events.map(event => ({
      time_unix_nano: preciseNanos(event.time),
      name: event.name,
      attributes: keyValues(event.attributes).list,
    })),
    links: ended.links.map(link => ({
      trace_id: bytesOf(link.traceId),
      span_id: bytesOf(link.spanId),
      attributes: keyValues(link.attributes).list,
    })),
    status: ended.status,
  };
}

function tracesRequest(identity: Identity, spans: object[]) {
  return { resource_spans: [{ resource: resource(identity), scope_spans: [{ scope: SCOPE, spans }] }] };
}

export interface QueuedLog {
  body: string;
  severity: number;
  time: string;
  traceId: string;
  spanId: string;
  traceFlags: number;
}

// A `nsolid.logger` line as N|Solid sends it: the body as a string, and the span it was written in, if any.
function logRecord(log: QueuedLog) {
  const time = BigInt(log.time);
  const record: Record<string, unknown> = {
    time_unix_nano: time,
    observed_time_unix_nano: time,
    severity_number: log.severity,
    severity_text: SEVERITY_TEXT[log.severity],
    body: { string_value: log.body },
  };
  const { traceId, spanId } = log;
  if (traceId && spanId) {
    record.trace_id = bytesOf(traceId);
    record.span_id = bytesOf(spanId);
    record.flags = log.traceFlags;
  }
  return record;
}

// N|Solid's log resource is the small one: the app, the agent, the app's version.
function logsRequest(identity: Identity, records: object[]) {
  const attributes = [attribute("service.name", identity.app), attribute("service.instance.id", identity.agentId)];
  const { appVersion } = identity;
  if (appVersion) attributes.push(attribute("service.version", appVersion));
  return { resource_logs: [{ resource: { attributes }, scope_logs: [{ scope: SCOPE, log_records: records }] }] };
}

export default { resourceMetrics, span, tracesRequest, logRecord, logsRequest };
