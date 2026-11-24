// The N|Solid agent. It runs on a thread of its own, in a VM of its own,
// started by src/jsc/nsolid_agent.rs when NSOLID_GRPC or NSOLID_SAAS is set,
// and talks to the N|Solid Console over gRPC (`grpcagent.NSolidService`).
//
// The Console drives the agent through the bidirectional `Command` stream: it
// sends a `CommandRequest` and expects an empty `CommandResponse` back at once,
// which it also uses to tell a live agent from a dead one. Each command is
// then answered with the unary or client-streaming RPC that carries its
// result, echoing the request's id.

const crypto = require("node:crypto");
const fs = require("node:fs");
const http2 = require("node:http2");
const path = require("node:path");
const tls = require("node:tls");
const protobuf = require("internal/nsolid/protobuf");
const nsolidConfig = require("internal/nsolid/config");
const nsolidMetrics = require("internal/nsolid/metrics");
const nsolidProcess = require("internal/nsolid/process");
const requests = require("internal/nsolid/requests");
const nsolidPackages = require("internal/nsolid/packages");
const nsolidInfo = require("internal/nsolid/info");
const otlp = require("internal/nsolid/otlp");
const profiles = require("internal/nsolid/profiles");

const onExit = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidOnExit", 1);
const exitSent = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidExitSent", 0);
const publishConfig = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidPublishConfig", 1);
const onStart = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidOnStart", 1);
const processStart = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidProcessStart", 0);
const requestLoadedModules = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidRequestLoadedModules", 2);
const nsolidThreads = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidThreads", 0);
const startProfile = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidStartProfile", 2);
const stopProfile = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidStopProfile", 2);
const takeSnapshot = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidTakeSnapshot", 4);
const checkLoops = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidCheckLoops", 1);
const loopStack = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidLoopStack", 2);
const onApiCommand = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidOnApiCommand", 1);
const setTracing = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidSetTracing", 2);
const takeSpans = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidTakeSpans", 0);
const takeLogs = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidTakeLogs", 0);
const scriptOf = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidScript", 2);

// The main thread's `ScriptExecutionContext` identifier: its packages are the
// ones reported, as in N|Solid.
const MAIN_CONTEXT_ID = 1;
// A main thread that does not answer in time leaves `required` as it was.
const LOADED_MODULES_TIMEOUT_MS = 1000;

const SERVICE = "grpcagent.NSolidService";
const METRICS_SERVICE = "opentelemetry.proto.collector.metrics.v1.MetricsService";
const TRACE_SERVICE = "opentelemetry.proto.collector.trace.v1.TraceService";
const LOGS_SERVICE = "opentelemetry.proto.collector.logs.v1.LogsService";
const SAAS_KEY_LENGTH = 40;
const SAAS_CONSOLE_ID_LENGTH = 36;
// N|Solid's reconnect rules (agents/grpc/src/grpc_agent.cc): the delay starts
// at half a second and doubles up to a minute; the 20th UNAUTHENTICATED close
// stops the agent. A Command stream that stayed up for a minute resets both.
// The Console's `retry-after` (seconds) lengthens a delay.
const RETRY_MIN_MS = 500;
const RETRY_MAX_MS = 60_000;
const MAX_AUTH_RETRIES = 20;
const MAX_RETRY_AFTER_S = 3600;
const DEFAULT_CLIENT_CERT_REFRESH_S = 60;
const GRPC_UNKNOWN = 2;
const GRPC_UNAVAILABLE = 14;
const GRPC_UNAUTHENTICATED = 16;
const EMPTY_FRAME = protobuf.frame(new Uint8Array(0));
// N|Solid's errors (agents/grpc/src/grpc_errors.h). NOT_AVAILABLE is what the
// agent answers for what it can't provide.
const IN_PROGRESS = { code: 409, message: "Operation already in progress(1001)" };
const THREAD_GONE = { code: 410, message: "Thread already gone(1002)" };
const ASSET_FAILED = { code: 500, message: "Profile/Snapshot creation failure(1003)" };
const SNAPSHOTS_DISABLED = { code: 500, message: "Heap Snapshots disabled(1004)" };
const OUT_OF_MEMORY = { code: 500, message: "Internal Runtime Error(1005)" };
const NOT_AVAILABLE = { code: 404, message: "Resource not available(1006)" };
const RUNTIME_ERROR = { code: 500, message: "Internal Runtime Error(1007)" };
const ASSETS_DISABLED = { code: 500, message: "Assets collection disabled(1008)" };
// `source_code` while the Console has not turned source reads on. N|Solid has no such setting yet: this is the next
// of its runtime error codes.
const SOURCE_READS_DISABLED = { code: 500, message: "Source code reads disabled(1009)" };
// An asset goes to the Console in chunks of this many bytes.
const ASSET_CHUNK_BYTES = 1 << 20;
// How long a thread may take to start or stop a profile: a thread in a long
// native call answers only once it is back in JS.
const PROFILE_START_TIMEOUT_MS = 10_000;
const PROFILE_STOP_TIMEOUT_MS = 60_000;
// How long a thread may take to answer with a heap snapshot: a large heap
// takes minutes. The Console waits 15.
const SNAPSHOT_TIMEOUT_MS = 10 * 60_000;
// N|Solid checks the threads' loops this often for one blocked past
// `blockedLoopThreshold`. A blocked thread's stack that has not come within
// BLOCKED_LOOP_STACK_WAIT_MS is sent empty: the thread is blocked outside
// JavaScript, where N|Solid would send no event at all.
const BLOCKED_LOOP_INTERVAL_MS = 100;
const BLOCKED_LOOP_STACK_WAIT_MS = 1000;
// N|Solid's span types that tracing turns on: dns, http_client, http_server and custom.
const TRACED_SPAN_TYPES = 1 | 4 | 8 | 16;
// N|Solid exports the spans every second, and sooner once more than 1,000 wait. One request carries 1,000 spans
// or log lines at most, which keeps it far below the Console's size limit.
const SPAN_EXPORT_MS = 1000;
const SPAN_EXPORT_COUNT = 1000;
const OTLP_BATCH = 1000;

// What the agent does, as `InfoBody.capabilities` reports it: the commands it
// answers, and the `ReconfigureBody` settings it applies.
const CAPABILITIES = {
  commands: ["info", "metrics", "packages", "profile", "reconfigure", "snapshot", "source_code", "startup_times"],
  settings: [
    "assetsEnabled",
    "blockedLoopThreshold",
    "contCpuProfile",
    "interval",
    "pauseMetrics",
    "redactSnapshots",
    "sourceCodeEnabled",
    "tags",
    "traceSampleRate",
    "tracingEnabled",
    "tracingModulesBlacklist",
  ],
};

// How long a continuous profile's thread may take to start or stop it: past that, the window is given up.
const CONTINUOUS_PROFILE_TIMEOUT_MS = 60_000;

// A script's URL as a path: profiles name files by `file://` URL, stacks by path.
function filePathOf(url: string): string {
  if (!url.startsWith("file://")) return url;
  try {
    return Bun.fileURLToPath(url);
  } catch {
    return url;
  }
}

// The RPC that carries each command's answer.
const replyRpc: Record<string, string> = {
  info: "ExportInfo",
  metrics: "ExportMetrics",
  packages: "ExportPackages",
  reconfigure: "ExportReconfigure",
  startup_times: "ExportStartupTimes",
  source_code: "ExportSourceCode",
  profile: "ExportAsset",
  heap_profile: "ExportAsset",
  heap_sampling: "ExportAsset",
  snapshot: "ExportAsset",
};

interface Endpoint {
  authority: string;
  secure: boolean;
  saasToken: string | undefined;
}

interface StartupMarks {
  mainEntryNs: number;
  agentThreadNs: number;
  agentVmNs: number;
}

// A running JavaScript thread by its id (0 for the main thread).
function threadOf(threadId: number): { contextId: number; threadId: number; name: string } | undefined {
  return (nsolidThreads() as { contextId: number; threadId: number; name: string }[]).find(
    thread => thread.threadId === threadId,
  );
}

function warn(message: string) {
  process.stderr.write(message + "\n");
}

function isTrue(value: string | undefined): boolean {
  const v = (value ?? "").toLowerCase();
  return v === "1" || v === "true";
}

// The same rules as N|Solid's `GrpcAgent::parse_saas_token`: a 40-character
// key, then `<console id>.<host>:<port>`. Production and staging tokens dial
// NodeSource's endpoints; a "testing" token dials what follows its last dot.
function parseSaasToken(token: string): { authority: string; testing: boolean } | undefined {
  if (token.length <= SAAS_KEY_LENGTH) return;
  const [host = "", port = ""] = token.slice(SAAS_KEY_LENGTH).split(":");
  if (!host || !port) return;
  const dot = host.indexOf(".");
  const consoleId = dot === -1 ? host : host.slice(0, dot);
  if (consoleId.length !== SAAS_CONSOLE_ID_LENGTH) return;

  if (token.includes("staging")) return { authority: `${consoleId}.grpc.staging.nodesource.io:443`, testing: false };
  if (token.includes("testing")) {
    const lastDot = token.lastIndexOf(".");
    const authority = lastDot !== -1 && lastDot + 1 < token.length ? token.slice(lastDot + 1) : "localhost:50051";
    return { authority, testing: true };
  }
  return { authority: `${consoleId}.grpc.nodesource.io:443`, testing: false };
}

function resolveEndpoint(env: Record<string, string | undefined>): Endpoint | undefined {
  const insecure = isTrue(env.NSOLID_GRPC_INSECURE);
  const saasToken = env.NSOLID_SAAS;
  if (saasToken) {
    const saas = parseSaasToken(saasToken);
    // Plaintext is only for a testing token, never for NodeSource's endpoints.
    if (saas) return { authority: saas.authority, secure: !(saas.testing && insecure), saasToken };
  }
  const grpc = env.NSOLID_GRPC;
  if (grpc) {
    const authority = /^\d+$/.test(grpc) ? `localhost:${grpc}` : grpc;
    return { authority, secure: !insecure, saasToken: undefined };
  }
}

// gRPC's mapping of an HTTP status that isn't 200 to a status code.
function grpcStatusOfHttp(status: number): number {
  switch (status) {
    case 400:
      return 13;
    case 401:
      return GRPC_UNAUTHENTICATED;
    case 403:
      return 7;
    case 404:
      return 12;
    case 429:
    case 502:
    case 503:
    case 504:
      return GRPC_UNAVAILABLE;
    default:
      return GRPC_UNKNOWN;
  }
}

// NSOLID_GRPC_CERTS: a CA bundle that replaces the default roots. One that
// can't be read, or is empty, leaves the default roots.
function readCa(env: Record<string, string | undefined>): Buffer | undefined {
  const file = env.NSOLID_GRPC_CERTS;
  if (file === undefined) return;
  try {
    const ca = fs.readFileSync(file);
    if (ca.length) return ca;
  } catch (err) {
    $debug(`cannot read NSOLID_GRPC_CERTS (${file}):`, (err as Error)?.message);
  }
}

// NSOLID_GRPC_KEYLOG: the TLS secrets go to ./nsolid-tls-keylog-<pid>.log,
// which starts out empty.
function keylogFile(env: Record<string, string | undefined>): string | undefined {
  const value = env.NSOLID_GRPC_KEYLOG;
  if (!value || value === "0") return;
  const file = path.resolve(`nsolid-tls-keylog-${process.pid}.log`);
  try {
    fs.unlinkSync(file);
  } catch {}
  return file;
}

// NSOLID_GRPC_CLIENT_CERT and NSOLID_GRPC_CLIENT_KEY: the certificate the
// agent presents. Both files are read again every
// NSOLID_GRPC_CLIENT_CERT_REFRESH_INTERVAL seconds (60 by default), so a
// renewed certificate is used from the next connection on.
class ClientIdentity {
  cert: Buffer | undefined;
  key: Buffer | undefined;

  static fromEnv(env: Record<string, string | undefined>): ClientIdentity | undefined {
    const certFile = env.NSOLID_GRPC_CLIENT_CERT;
    const keyFile = env.NSOLID_GRPC_CLIENT_KEY;
    if (!certFile && !keyFile) return;
    if (!certFile || !keyFile) {
      const [set, unset] = certFile
        ? ["NSOLID_GRPC_CLIENT_CERT", "NSOLID_GRPC_CLIENT_KEY"]
        : ["NSOLID_GRPC_CLIENT_KEY", "NSOLID_GRPC_CLIENT_CERT"];
      warn(`N|Solid warning: ${set} is set without ${unset}, so the agent connects without a client certificate`);
      return;
    }
    const interval = env.NSOLID_GRPC_CLIENT_CERT_REFRESH_INTERVAL ?? "";
    const refreshS =
      /^\d+$/.test(interval) && +interval > 0 && +interval <= 0xffffffff ? +interval : DEFAULT_CLIENT_CERT_REFRESH_S;
    return new ClientIdentity(certFile, keyFile, refreshS);
  }

  constructor(
    public certFile: string,
    public keyFile: string,
    refreshS: number,
  ) {
    // Files that can't be read yet are not an error: they may appear later.
    const error = this.load();
    if (error) {
      warn(
        `N|Solid warning: the client certificate (${certFile}) or its key (${keyFile}) cannot be used yet: ${error}`,
      );
    }
    setInterval(() => this.load(), Math.min(refreshS * 1000, 2 ** 31 - 1)).unref();
  }

  // Reads both files; a pair that makes a TLS context replaces the last one.
  // Returns why the files' contents can't be used.
  load(): string | undefined {
    let cert: Buffer, key: Buffer;
    try {
      cert = fs.readFileSync(this.certFile);
      key = fs.readFileSync(this.keyFile);
    } catch {
      return;
    }
    try {
      tls.createSecureContext({ cert, key });
    } catch (err) {
      return (err as Error)?.message ?? String(err);
    }
    this.cert = cert;
    this.key = key;
  }
}

function appendKeylog(file: string, line: Buffer) {
  try {
    fs.appendFileSync(file, line, { mode: 0o600 });
  } catch (err) {
    $debug("cannot write the TLS keylog:", (err as Error)?.message);
  }
}

function recorded() {
  const ms = Date.now();
  return { seconds: Math.floor(ms / 1000), nanoseconds: (ms % 1000) * 1_000_000 };
}

class Agent {
  config;
  metadata: Record<string, string>;
  url: string;
  host: string;
  port: number;
  secure: boolean;
  ca: Buffer | undefined;
  identity: ClientIdentity | undefined;
  keylog: string | undefined;
  session: any = null;
  connected = false;
  stopped = false;
  retryMinMs = RETRY_MIN_MS;
  retryMaxMs = RETRY_MAX_MS;
  retryInterval = RETRY_MIN_MS;
  authRetries = 0;
  processStartMs: number;
  probe;
  metricsTimer: ReturnType<typeof setInterval> | undefined;
  // The last periodic sample, which the `metrics` command answers with, as
  // N|Solid does: CPU percentages stay measured over a whole interval.
  lastMetrics: object | undefined;
  // The CPU profiles running for the Console, by thread id.
  profiles = new Map<number, { request; receivedAt: number }>();
  // The threads a heap snapshot is being taken of.
  snapshots = new Set<number>();
  // Each thread's blocked-loop events, sent in order.
  loopEvents = new Map<number, Promise<void>>();
  inventory;
  // One `packages` answer at a time: a second waits for the scan the first started.
  packagesRun: Promise<unknown> = Promise.resolve();
  // The trace flags and sample rate the threads were last given.
  traceFlags = 0;
  traceSampleRate = 1;
  // Spans the threads ended, waiting for the next export.
  spans: any[] = [];
  // Continuous CPU profiles: each thread's running window, and the threads one is starting on.
  windows = new Map<number, { contextId: number; startEpochMs: number; startPerf: number; intervalMs: number }>();
  startingWindows = new Set<number>();

  constructor(
    endpoint: Endpoint,
    public agentId: string,
    mainPath: string,
    public marks: StartupMarks,
    updates: object[],
  ) {
    const env = process.env;
    this.config = nsolidConfig.createConfig(env, mainPath);
    for (const update of updates) nsolidConfig.applyUpdate(this.config, update);
    publishConfig(JSON.stringify(this.config));
    this.processStartMs = processStart();
    this.probe = new nsolidProcess.ProcessProbe(this.processStartMs);
    this.inventory = new nsolidPackages.Inventory(
      this.config.main,
      !this.config.disablePackageScan,
      this.config.trackGlobalPackages ? (require("node:module").globalPaths ?? []) : [],
    );
    this.secure = endpoint.secure;
    this.url = `${endpoint.secure ? "https" : "http"}://${endpoint.authority}`;
    const url = new URL(this.url);
    this.host = url.hostname.replace(/^\[(.*)\]$/, "$1");
    this.port = Number(url.port) || (endpoint.secure ? 443 : 80);
    if (endpoint.secure) {
      this.ca = readCa(env);
      this.identity = ClientIdentity.fromEnv(env);
      this.keylog = keylogFile(env);
    }
    // Tests reconnect at a fixed, short interval.
    const testingRetryMs = Number(env.BUN_NSOLID_RETRY_MS_FOR_TESTING);
    if (Number.isSafeInteger(testingRetryMs) && testingRetryMs > 0) {
      this.retryMinMs = this.retryMaxMs = this.retryInterval = testingRetryMs;
    }
    this.metadata = {
      "content-type": "application/grpc",
      "te": "trailers",
      "grpc-accept-encoding": "identity",
      "nsolid-agent-id": agentId,
      "nsolid-version": `nsolid-bun-v${process.versions.nsolid_bun}+bun-v${Bun.version}`,
    };
    const { saasToken } = endpoint;
    if (saasToken) this.metadata["nsolid-saas"] = saasToken;
    this.scheduleMetrics();
    setInterval(() => this.checkLoops(), BLOCKED_LOOP_INTERVAL_MS);
    setInterval(() => this.exportSpans(), SPAN_EXPORT_MS);
    setInterval(() => this.profileContinuously(), BLOCKED_LOOP_INTERVAL_MS);
    this.applyTracing();
  }

  // N|Solid's continuous profiling: while `contCpuProfile` is on and assets are enabled, every thread is profiled
  // in back-to-back windows of `contCpuProfileInterval` ms, each sent on its own `ExportContinuousProfile` stream.
  // A thread whose profiler is taken (an on-demand profile) starts its window once it is free. A window that is
  // running when profiling turns off still ends on time and is sent.
  profileContinuously() {
    const { config, windows, startingWindows } = this;
    const on = !!config.contCpuProfile && config.assetsEnabled !== false;
    const threads = nsolidThreads() as { contextId: number; threadId: number }[];
    const now = performance.now();
    for (const [threadId, window] of windows) {
      if (!threads.some(thread => thread.threadId === threadId)) windows.delete(threadId);
      else if (now - window.startPerf >= window.intervalMs) this.endWindow(threadId, window);
    }
    if (!on) return;
    for (const { contextId, threadId } of threads) {
      if (windows.has(threadId) || startingWindows.has(threadId) || this.profiles.has(threadId)) continue;
      startingWindows.add(threadId);
      const intervalMs = config.contCpuProfileInterval;
      requests
        .ask(startProfile, contextId, CONTINUOUS_PROFILE_TIMEOUT_MS)
        .then(started => {
          if (started?.startedAt) {
            windows.set(threadId, {
              contextId,
              startEpochMs: started.startedAt / 1000,
              startPerf: performance.now(),
              intervalMs,
            });
          }
        })
        .finally(() => startingWindows.delete(threadId));
    }
  }

  endWindow(threadId: number, window: { contextId: number; startEpochMs: number; startPerf: number }) {
    this.windows.delete(threadId);
    // Taken by the next tick only once this one's profile has stopped.
    this.startingWindows.add(threadId);
    requests
      .ask(stopProfile, window.contextId, CONTINUOUS_PROFILE_TIMEOUT_MS)
      .then(answer => {
        if (!answer?.profile) return;
        const profile = profiles.completeProfile(answer.profile, answer.stoppedAt, answer.collections, answer.waits);
        const elapsedMs = performance.now() - window.startPerf;
        const base = {
          common: { requestId: crypto.randomBytes(20).toString("hex"), command: "profile", recorded: recorded() },
          thread_id: threadId,
          metadata: { fields: {} },
        };
        const data = Buffer.from(profile);
        const messages: object[] = [];
        for (let offset = 0; offset < data.length; offset += ASSET_CHUNK_BYTES) {
          messages.push({ ...base, data: data.subarray(offset, offset + ASSET_CHUNK_BYTES) });
        }
        messages.push({
          ...base,
          complete: true,
          duration: Math.floor(elapsedMs),
          start_ts: window.startEpochMs,
          end_ts: window.startEpochMs + elapsedMs,
        });
        return this.sendStream("ExportContinuousProfile", messages);
      })
      .catch((err: Error) => $debug("continuous profile failed:", err?.message))
      .finally(() => this.startingWindows.delete(threadId));
  }

  // The `source_code` command: a script a thread loaded, by its id and its path (a profile's `file://` URL or a
  // stack's path), read from disk when it is asked for, as N|Solid reads it. Only while the Console has turned
  // source reads on.
  async sourceCode(request) {
    const args = request.args?.source_code ?? {};
    const threadId = Number(args.thread_id ?? 0);
    const path = String(args.path ?? "");
    const reply = (fields: { error?: { code: number; message: string }; code?: Uint8Array; is_esm?: boolean }) =>
      this.send("ExportSourceCode", {
        common: this.common(request, fields.error),
        thread_id: threadId,
        path,
        code: fields.code,
        is_esm: fields.is_esm,
      });
    if (this.config.sourceCodeEnabled !== true) return reply({ error: SOURCE_READS_DISABLED });
    const thread = threadOf(threadId);
    if (!thread) return reply({ error: THREAD_GONE });
    const script = scriptOf(thread.contextId, Number(args.script_id ?? 0)) as
      { url: string; isEsm: boolean; code?: string } | undefined;
    if (!script || filePathOf(script.url) !== filePathOf(path)) return reply({ error: RUNTIME_ERROR });
    const { code: kept } = script;
    let code: Uint8Array;
    if (kept !== undefined) code = Buffer.from(kept);
    else {
      try {
        code = fs.readFileSync(filePathOf(script.url));
      } catch {
        return reply({ error: RUNTIME_ERROR });
      }
    }
    return reply({ code, is_esm: script.isEsm });
  }

  // N|Solid's trace flags: every span type while tracing is on, less the ones the blacklist names. The threads get
  // them, with the sample rate, whenever either changes.
  applyTracing() {
    const { config } = this;
    const flags = config.tracingEnabled ? TRACED_SPAN_TYPES & ~(config.tracingModulesBlacklist >>> 0) : 0;
    const rate = config.traceSampleRate;
    if (flags === this.traceFlags && rate === this.traceSampleRate) return;
    this.traceFlags = flags;
    this.traceSampleRate = rate;
    setTracing(flags, rate);
  }

  // The spans the threads queued since the last call.
  takeSpans() {
    for (const json of takeSpans() as string[]) {
      try {
        this.spans.push(JSON.parse(json));
      } catch {}
    }
    if (this.spans.length > SPAN_EXPORT_COUNT) this.exportSpans();
  }

  // Like N|Solid, spans that can't be sent now (no connection, tracing turned off since) are dropped.
  exportSpans() {
    const ended = this.spans;
    if (!ended.length) return;
    this.spans = [];
    if (!this.session || !this.traceFlags) return;
    const names = new Map<number, string>();
    for (const thread of nsolidThreads() as { threadId: number; name: string }[])
      names.set(thread.threadId, thread.name);
    const identity = this.otlpIdentity();
    for (let i = 0; i < ended.length; i += OTLP_BATCH) {
      const spans = ended.slice(i, i + OTLP_BATCH).map(span => otlp.span(span, names.get(span.threadId) ?? ""));
      this.send("Export", otlp.tracesRequest(identity, spans), TRACE_SERVICE).catch((err: Error) =>
        $debug("trace export failed:", err?.message),
      );
    }
  }

  // `nsolid.logger` lines, sent as N|Solid sends them: all that are queued, at once, and dropped while there is no
  // connection.
  exportLogs() {
    const logs = takeLogs() as any[];
    if (!logs.length || !this.session) return;
    const identity = this.otlpIdentity();
    for (let i = 0; i < logs.length; i += OTLP_BATCH) {
      const records = logs.slice(i, i + OTLP_BATCH).map(otlp.logRecord);
      this.send("Export", otlp.logsRequest(identity, records), LOGS_SERVICE).catch((err: Error) =>
        $debug("log export failed:", err?.message),
      );
    }
  }

  otlpIdentity() {
    return {
      agentId: this.agentId,
      app: this.config.app,
      appVersion: this.config.appVersion,
      hostname: this.config.hostname,
      main: this.config.main,
      nodeEnv: this.config.nodeEnv,
      tags: this.config.tags,
      processStartMs: this.processStartMs,
    };
  }

  // N|Solid's blocked-loop watchdog: `loop_blocked` once a thread's loop has
  // run one iteration for `blockedLoopThreshold` ms, with the thread's stack,
  // then `loop_unblocked` with how long that iteration ran.
  checkLoops() {
    const { loopEvents } = this;
    for (const event of checkLoops(this.config.blockedLoopThreshold)) {
      const { threadId } = event;
      const body = {
        thread_id: threadId,
        blocked_for: event.blockedFor,
        loop_id: event.loopId,
        callback_cntr: event.callbacks,
      };
      const common = {
        requestId: crypto.randomBytes(20).toString("hex"),
        command: event.blocked ? "loop_blocked" : "loop_unblocked",
        recorded: recorded(),
      };
      const stack = event.blocked
        ? requests.ask(loopStack, event.contextId, BLOCKED_LOOP_STACK_WAIT_MS)
        : Promise.resolve(undefined);
      const previous = loopEvents.get(threadId) ?? Promise.resolve();
      const sent = previous
        .then(() => stack)
        .then(frames =>
          event.blocked
            ? this.send("ExportBlockedLoop", { common, body: { ...body, stack: frames ?? [] } })
            : this.send("ExportUnblockedLoop", { common, body }),
        )
        .catch((err: Error) => $debug("loop event failed:", err?.message));
      loopEvents.set(threadId, sent);
      sent.then(() => {
        if (loopEvents.get(threadId) === sent) loopEvents.delete(threadId);
      });
    }
  }

  // Metrics go to the Console every `interval` over OTLP, unless paused.
  scheduleMetrics() {
    clearInterval(this.metricsTimer);
    this.metricsTimer = setInterval(() => {
      if (this.config.pauseMetrics || !this.session) return;
      this.collectMetrics()
        .then(resourceMetrics => {
          this.lastMetrics = resourceMetrics;
          return this.send("Export", { resource_metrics: [resourceMetrics] }, METRICS_SERVICE);
        })
        .catch((err: Error) => $debug("metrics export failed:", err?.message));
    }, this.config.interval);
  }

  async collectMetrics() {
    const proc = this.probe.sample();
    const threads = await nsolidMetrics.threadMetrics();
    return otlp.resourceMetrics(this.otlpIdentity(), proc, threads);
  }

  connectOptions() {
    if (!this.secure) return undefined;
    const tlsOptions: Record<string, any> = {};
    const { ca } = this;
    if (ca) tlsOptions.ca = ca;
    const { cert, key } = this.identity ?? {};
    if (cert && key) {
      tlsOptions.cert = cert;
      tlsOptions.key = key;
    }
    const keylog = this.keylog;
    if (!keylog) return tlsOptions;
    // The keylog is an event of the TLS socket, so the agent makes its own.
    return {
      createConnection: () => {
        const socket = tls.connect({ ...tlsOptions, host: this.host, port: this.port, ALPNProtocols: ["h2"] });
        socket.on("keylog", (line: Buffer) => appendKeylog(keylog, line));
        return socket;
      },
    };
  }

  connect() {
    if (this.stopped) return;
    let session;
    try {
      session = http2.connect(this.url, this.connectOptions());
    } catch (err) {
      $debug("cannot connect:", (err as Error)?.message);
      return this.commandStreamClosed(GRPC_UNAVAILABLE, 0, 0);
    }
    this.session = session;
    // The Command stream's grpc-status, the Console's retry-after, and when
    // the stream was accepted.
    let status: number | undefined;
    let retryAfterMs = 0;
    let establishedAt = 0;
    let closed = false;
    let stream;
    // The call is over once the gateway ends its side, with or without a
    // status: the agent's side is still open, and is cancelled.
    const onClosed = () => {
      if (closed) return;
      closed = true;
      this.connected = false;
      if (this.session === session) this.session = null;
      if (stream && !stream.closed) stream.close(http2.constants.NGHTTP2_CANCEL);
      session.close();
      this.commandStreamClosed(status ?? GRPC_UNAVAILABLE, establishedAt, retryAfterMs);
    };
    const readStatus = (headers: Record<string, string>) => {
      if (headers["grpc-status"] !== undefined) status = Number(headers["grpc-status"]);
      const retryAfter = Number(headers["retry-after"]);
      if (retryAfter > 0) retryAfterMs = Math.min(retryAfter, MAX_RETRY_AFTER_S) * 1000;
    };

    session.on("connect", () => {
      this.connected = true;
    });
    session.on("error", (err: Error) => $debug("session error:", err?.message));
    session.on("close", onClosed);

    stream = session.request(
      { ":method": "POST", ":path": `/${SERVICE}/Command`, ...this.metadata },
      { endStream: false },
    );
    stream.on("response", (headers: Record<string, string>) => {
      const httpStatus = Number(headers[":status"]);
      if (httpStatus !== 200) status = grpcStatusOfHttp(httpStatus);
      readStatus(headers);
      if (status === undefined) establishedAt = performance.now();
    });
    stream.on("trailers", readStatus);
    stream.on("error", (err: Error) => $debug("command stream error:", err?.message));
    stream.on("end", onClosed);
    stream.on("close", onClosed);

    const frames = new protobuf.FrameReader();
    stream.on("data", (chunk: Uint8Array) => {
      for (const message of frames.push(chunk)) {
        stream.write(EMPTY_FRAME);
        let request;
        try {
          request = protobuf.decode("grpcagent.CommandRequest", message);
        } catch (err) {
          $debug("undecodable command:", (err as Error)?.message);
          continue;
        }
        this.handle(request).catch((err: Error) => $debug(`command ${request.command} failed:`, err?.message));
      }
    });
  }

  commandStreamClosed(code: number, establishedAt: number, retryAfterMs: number) {
    if (establishedAt && performance.now() - establishedAt >= RETRY_MAX_MS) {
      this.retryInterval = this.retryMinMs;
      this.authRetries = 0;
    }
    if (code === GRPC_UNAUTHENTICATED && ++this.authRetries === MAX_AUTH_RETRIES) {
      warn(`N|Solid warning: ${this.agentId} Unable to authenticate, please verify your token and network connection!`);
      this.stopped = true;
      return;
    }
    const interval = this.retryInterval;
    const delay = Math.max(interval - Math.floor(Math.random() * (Math.floor(interval / 2) + 1)), retryAfterMs);
    this.retryInterval = Math.min(interval * 2, this.retryMaxMs);
    $debug(`Command stream closed (${code}). Retrying in ${delay} ms`);
    setTimeout(() => this.connect(), delay);
  }

  // Sends one message on a unary or client-streaming RPC, and waits for the
  // call to end.
  send(rpc: string, message: object, service: string = SERVICE): Promise<void> {
    return this.sendStream(rpc, [message], service);
  }

  // Sends messages on a client-streaming RPC, and waits for the call to end.
  sendStream(rpc: string, messages: object[], service: string = SERVICE): Promise<void> {
    const session = this.session;
    if (!session || session.closed || session.destroyed) return Promise.resolve();
    const [type] = protobuf.services[service][rpc];
    const { promise, resolve } = Promise.withResolvers<void>();
    let status: string | undefined;
    const stream = session.request({ ":method": "POST", ":path": `/${service}/${rpc}`, ...this.metadata });
    stream.on("response", (headers: Record<string, string>) => {
      status = headers["grpc-status"] ?? status;
    });
    stream.on("trailers", (trailers: Record<string, string>) => {
      status = trailers["grpc-status"] ?? status;
    });
    stream.on("data", () => {});
    stream.on("error", (err: Error) => $debug(`${rpc} error:`, err?.message));
    stream.on("close", () => {
      if (status !== undefined && status !== "0") $debug(`${rpc} answered grpc-status ${status}`);
      resolve();
    });
    for (const message of messages) stream.write(protobuf.frame(protobuf.encode(type, message)));
    stream.end();
    return promise;
  }

  // An asset (a CPU profile, a heap snapshot) for a command: its data in
  // chunks on one `ExportAsset` stream, then a message that marks it complete
  // with the milliseconds since the command arrived. Each message carries the
  // request's id, thread and metadata, as N|Solid's do. `data` may come in
  // chunks already.
  sendAsset(request, threadId: number, data: Uint8Array | Uint8Array[], durationMs: number) {
    const base = { common: this.common(request), thread_id: threadId, metadata: request.args?.profile?.metadata };
    const messages: object[] = [];
    for (const chunk of $isArray(data) ? data : [data]) {
      for (let offset = 0; offset < chunk.length; offset += ASSET_CHUNK_BYTES) {
        messages.push({ ...base, data: chunk.subarray(offset, offset + ASSET_CHUNK_BYTES) });
      }
    }
    messages.push({ ...base, complete: true, duration: Math.max(0, Math.round(durationMs)) });
    return this.sendStream("ExportAsset", messages);
  }

  sendAssetError(request, threadId: number, error: { code: number; message: string }) {
    return this.send("ExportAsset", {
      common: this.common(request, error),
      thread_id: threadId,
      metadata: request.args?.profile?.metadata,
    });
  }

  // The `profile` command: a CPU profile of one thread for `duration` ms,
  // with N|Solid's checks in N|Solid's order.
  async profile(request) {
    const args = request.args?.profile ?? {};
    const threadId = Number(args.thread_id ?? 0);
    const receivedAt = performance.now();
    if (this.config.assetsEnabled === false) return this.sendAssetError(request, threadId, ASSETS_DISABLED);
    if (this.config.contCpuProfile || this.profiles.has(threadId)) {
      return this.sendAssetError(request, threadId, IN_PROGRESS);
    }
    const thread = threadOf(threadId);
    if (!thread) return this.sendAssetError(request, threadId, THREAD_GONE);
    const entry = { request, receivedAt };
    this.profiles.set(threadId, entry);
    const started = await requests.ask(startProfile, thread.contextId, PROFILE_START_TIMEOUT_MS);
    if (started?.inUse || (!started && !threadOf(threadId))) {
      this.profiles.delete(threadId);
      return this.sendAssetError(request, threadId, started ? IN_PROGRESS : THREAD_GONE);
    }
    // A profile that started late still stops on time.
    const stopIn = receivedAt + Number(args.duration ?? 0) - performance.now();
    setTimeout(
      () => {
        this.finishProfile(threadId, thread.contextId).catch((err: Error) => $debug("profile failed:", err?.message));
      },
      Math.max(0, stopIn),
    );
  }

  async finishProfile(threadId: number, contextId: number) {
    const entry = this.profiles.get(threadId);
    if (!entry) return;
    const answer = await requests.ask(stopProfile, contextId, PROFILE_STOP_TIMEOUT_MS);
    // The process's exit may have sent it meanwhile.
    if (this.profiles.get(threadId) !== entry) return;
    this.profiles.delete(threadId);
    if (!answer?.profile) return this.sendAssetError(entry.request, threadId, ASSET_FAILED);
    return this.sendProfile(entry, threadId, answer);
  }

  // The `snapshot` command: a heap snapshot of one thread, redacted if the
  // request or the `redactSnapshots` setting asks for it, with N|Solid's
  // checks in N|Solid's order.
  async snapshot(request) {
    const args = request.args?.profile ?? {};
    const threadId = Number(args.thread_id ?? 0);
    const receivedAt = performance.now();
    if (this.config.assetsEnabled === false) return this.sendAssetError(request, threadId, ASSETS_DISABLED);
    if (this.config.disableSnapshots) return this.sendAssetError(request, threadId, SNAPSHOTS_DISABLED);
    if (this.snapshots.has(threadId)) return this.sendAssetError(request, threadId, IN_PROGRESS);
    const thread = threadOf(threadId);
    if (!thread) return this.sendAssetError(request, threadId, THREAD_GONE);
    const redact = !!args.heap_snapshot?.redacted || !!this.config.redactSnapshots;
    this.snapshots.add(threadId);
    let answer: { chunks?: Uint8Array[]; error?: string } | undefined;
    try {
      answer = await requests.ask(
        (contextId: number, requestId: number) => takeSnapshot(contextId, requestId, redact, ASSET_CHUNK_BYTES),
        thread.contextId,
        SNAPSHOT_TIMEOUT_MS,
      );
    } finally {
      this.snapshots.delete(threadId);
    }
    if (answer?.error === "memory") return this.sendAssetError(request, threadId, OUT_OF_MEMORY);
    if (!answer?.chunks) {
      return this.sendAssetError(request, threadId, threadOf(threadId) ? ASSET_FAILED : THREAD_GONE);
    }
    return this.sendAsset(request, threadId, answer.chunks, performance.now() - receivedAt);
  }

  // `nsolid.profile()`, `nsolid.snapshot()` and `nsolid.profileEnd()` from a
  // thread's code: taken and sent as the Console's commands are, marked as
  // N|Solid marks them.
  apiCommand(command: string, threadId: number, value) {
    if (command === "spans") return this.takeSpans();
    if (command === "logs") return this.exportLogs();
    const requestId = crypto.randomBytes(20).toString("hex");
    const metadata = { fields: { reason: { string_value: "Agent API" } } };
    const failed = (err: Error) => $debug(`${command} from the API failed:`, err?.message);
    switch (command) {
      case "profile":
        return this.profile({
          requestId,
          command,
          args: { profile: { thread_id: threadId, duration: value, metadata, cpu_profile: {} } },
        }).catch(failed);
      case "snapshot":
        return this.snapshot({
          requestId,
          command,
          args: { profile: { thread_id: threadId, metadata, heap_snapshot: {} } },
        }).catch(failed);
      case "profileEnd": {
        const entry = this.profiles.get(threadId);
        if (!entry || !value) return;
        this.profiles.delete(threadId);
        return this.sendProfile(entry, threadId, value).catch(failed);
      }
    }
  }

  sendProfile(entry: { request; receivedAt: number }, threadId: number, answer) {
    const profile = profiles.completeProfile(answer.profile, answer.stoppedAt, answer.collections, answer.waits);
    return this.sendAsset(entry.request, threadId, Buffer.from(profile), performance.now() - entry.receivedAt);
  }

  // The process is ending; the thread it ends on waits for `exitSent()`, 5
  // seconds at most. Like N|Solid, an agent that is not connected sends
  // nothing.
  // `mainProfile` is the main thread's running profile, which ended with it:
  // like N|Solid, the agent sends it first and names it in the exit.
  reportExit(
    code: number,
    signal: string | undefined,
    message: string | undefined,
    stack: string | undefined,
    mainProfile: { profile: string; stoppedAt: number; collections: number[]; waits: number[] } | undefined,
  ) {
    if (!this.connected) return exitSent();
    this.takeSpans();
    this.exportSpans();
    this.exportLogs();
    const common = { requestId: crypto.randomBytes(20).toString("hex"), command: "exit", recorded: recorded() };
    const error = message === undefined ? undefined : { message, stack: stack ?? "" };
    const entry = this.profiles.get(0);
    let profileSent = Promise.resolve();
    let profile = "";
    if (mainProfile && entry) {
      this.profiles.delete(0);
      profile = entry.request.requestId;
      profileSent = this.sendProfile(entry, 0, mainProfile).catch(() => {});
    }
    profileSent
      .then(() => this.send("ExportExit", { common, body: { code, signal: signal ?? "", error, profile } }))
      .then(
        () => exitSent(),
        () => exitSent(),
      );
  }

  common(request, error?: { code: number; message: string }) {
    return { requestId: request.requestId, command: request.command, recorded: recorded(), error };
  }

  async handle(request) {
    const command = request.command;
    const rpc = replyRpc[command];
    if (!rpc) {
      $debug("unknown command:", command);
      return;
    }
    switch (command) {
      case "info":
        return this.send(rpc, { common: this.common(request), body: this.info() });
      case "startup_times":
        return this.send(rpc, { common: this.common(request), body: this.startupTimes() });
      case "reconfigure": {
        const interval = this.config.interval;
        nsolidConfig.applyReconfigure(this.config, request.args?.reconfigure);
        publishConfig(JSON.stringify(this.config));
        if (this.config.interval !== interval) this.scheduleMetrics();
        this.applyTracing();
        return this.send(rpc, { common: this.common(request), body: nsolidConfig.reconfigureBody(this.config) });
      }
      case "packages": {
        const modules = await requests.ask(requestLoadedModules, MAIN_CONTEXT_ID, LOADED_MODULES_TIMEOUT_MS);
        const run = this.packagesRun.then(() => this.inventory.packagesLater(modules ?? []));
        this.packagesRun = run.catch(() => {});
        const packages = await run;
        return this.send(rpc, { common: this.common(request), body: { packages } });
      }
      case "metrics": {
        const resourceMetrics = this.lastMetrics ?? (await this.collectMetrics());
        return this.send(rpc, { common: this.common(request), body: { resource_metrics: [resourceMetrics] } });
      }
      case "profile":
        return this.profile(request);
      case "snapshot":
        return this.snapshot(request);
      case "source_code":
        return this.sourceCode(request);
      default: {
        const message: Record<string, any> = { common: this.common(request, NOT_AVAILABLE) };
        if (rpc === "ExportAsset") message.thread_id = request.args?.profile?.thread_id ?? 0;
        return this.send(rpc, message);
      }
    }
  }

  info() {
    return {
      ...nsolidInfo.infoObject(this.config, this.agentId, this.processStartMs),
      runtime: { name: "bun", version: Bun.version, engine: "jsc", agent_version: process.versions.nsolid_bun },
      capabilities: CAPABILITIES,
    };
  }

  // `nsolid.start(config)` from the application, after the agent started.
  update(update: object) {
    const interval = this.config.interval;
    nsolidConfig.applyUpdate(this.config, update);
    publishConfig(JSON.stringify(this.config));
    if (this.config.interval !== interval) this.scheduleMetrics();
    this.applyTracing();
  }

  // Nanoseconds since the process started; the Console measures each mark
  // from `timeOrigin`.
  startupTimes() {
    return {
      timeOrigin: 0,
      main_module_start: Math.round(this.marks.mainEntryNs),
      nsolid_agent_thread_start: Math.round(this.marks.agentThreadNs),
      nsolid_agent_ready: Math.round(this.marks.agentVmNs),
    };
  }
}

export default function start(
  agentId: string,
  mainPath: string,
  mainEntryNs: number,
  agentThreadNs: number,
  agentVmNs: number,
) {
  let agent: Agent | undefined;
  onApiCommand((command: string, threadId: number, value: unknown) => {
    if (agent) return agent.apiCommand(command, threadId, value);
    // Nothing exports what the threads queued: it is let go, so that they queue again.
    if (command === "spans") takeSpans();
    else if (command === "logs") takeLogs();
  });
  const updates: object[] = onStart((json: string) => {
    const update = JSON.parse(json);
    if (agent) agent.update(update);
    else updates.push(update);
  }).map((json: string) => JSON.parse(json));
  // `nsolid.start({ grpc })` or `nsolid.start({ saas })` names the endpoint
  // when the environment does not.
  const env = { ...process.env };
  for (const update of updates as Record<string, any>[]) {
    if (update?.grpc) env.NSOLID_GRPC = String(update.grpc);
    if (update?.saas) env.NSOLID_SAAS = String(update.saas);
  }
  const endpoint = resolveEndpoint(env);
  if (!endpoint) {
    $debug("no usable NSOLID_GRPC or NSOLID_SAAS");
    return;
  }
  agent = new Agent(endpoint, agentId, mainPath, { mainEntryNs, agentThreadNs, agentVmNs }, updates);
  onExit((code: number, signal?: string, message?: string, stack?: string, mainProfile?) =>
    agent.reportExit(code, signal, message, stack, mainProfile),
  );
  agent.connect();
}
