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

const nanos = (ms: number) => BigInt(Math.round(ms)) * 1_000_000n;

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

function resourceMetrics(identity: Identity, proc: Record<string, any>, threads: Record<string, any>[]) {
  const start = identity.processStartMs;
  const metrics: object[] = [];
  for (const [name, kind, unit, valueType] of processMetrics) {
    metrics.push(metric(name, kind, unit, [point(proc[name], valueType, proc.timestamp, start)]));
  }
  for (const [name, unit, valueType] of threadMetrics) {
    const points = threads.map(thread =>
      point(thread[name === "externalMem" ? "external" : name], valueType, thread.timestamp, start, [
        attribute("thread.id", thread.threadId),
        attribute("thread.name", thread.threadName),
      ]),
    );
    if (points.length) metrics.push(metric(name, "gauge", unit, points));
  }
  return { resource: resource(identity), scope_metrics: [{ scope: SCOPE, metrics }] };
}

export default { resourceMetrics };
