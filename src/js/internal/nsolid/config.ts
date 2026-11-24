// The agent's configuration, read the way N|Solid reads it: NSOLID_*
// environment variables first, then the "nsolid" key of the application's
// package.json, then defaults. The Console reads it back and changes it with
// `reconfigure`.

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const EBPF_PROFILING_UNSUPPORTED = 2;

type Env = Record<string, string | undefined>;

interface Config {
  app: string;
  appVersion: string | undefined;
  hostname: string;
  main: string;
  nodeEnv: string;
  blockedLoopThreshold: number;
  interval: number;
  pauseMetrics: boolean;
  promiseTracking: boolean;
  redactSnapshots: boolean;
  statsd: string | undefined;
  statsdBucket: string | undefined;
  statsdTags: string | undefined;
  tags: string[];
  tracingEnabled: boolean;
  tracingModulesBlacklist: number;
  contCpuProfile: boolean;
  assetsEnabled: boolean;
  traceSampleRate: number;
  sourceCodeEnabled: boolean;
}

function parseBool(value: unknown, fallback: boolean): boolean {
  if (typeof value === "boolean") return value;
  const v = String(value ?? "").toLowerCase();
  if (v === "1" || v === "true") return true;
  if (v === "0" || v === "false") return false;
  return fallback;
}

function parseNumber(value: unknown, fallback: number): number {
  if (value === undefined || value === null || value === "") return fallback;
  const n = Number(value);
  return Number.isFinite(n) && n >= 0 ? n : fallback;
}

// N|Solid's tag rules: a comma-separated list, each tag trimmed, sorted, no
// duplicates.
function parseTags(value: unknown): string[] {
  const list = Array.isArray(value) ? value : String(value ?? "").split(",");
  const tags = new Set<string>();
  for (const tag of list) {
    const t = String(tag).trim();
    if (t) tags.add(t);
  }
  return [...tags].sort();
}

// NSOLID_PACKAGE_JSON, or the nearest package.json above the main script (or
// the working directory when there is none).
function readPackageJson(env: Env, main: string): Record<string, any> {
  const read = (file: string) => {
    try {
      const value = JSON.parse(fs.readFileSync(file, "utf8"));
      return value && typeof value === "object" ? value : undefined;
    } catch {
      return undefined;
    }
  };
  const { NSOLID_PACKAGE_JSON } = env;
  if (NSOLID_PACKAGE_JSON) return read(NSOLID_PACKAGE_JSON) ?? {};
  let dir = main ? path.dirname(main) : process.cwd();
  for (;;) {
    const file = path.join(dir, "package.json");
    if (fs.existsSync(file)) return read(file) ?? {};
    const parent = path.dirname(dir);
    if (parent === dir) return {};
    dir = parent;
  }
}

// `mainPath` is the main thread's entry point; this VM has no main module of
// its own.
function createConfig(env: Env, mainPath: string): Config {
  const main = path.isAbsolute(mainPath) ? mainPath : "";
  const pkg = readPackageJson(env, main);
  const fromPkg = pkg.nsolid && typeof pkg.nsolid === "object" ? pkg.nsolid : {};
  const pick = (envName: string, key: string) => env[envName] ?? fromPkg[key];

  return {
    app: String(env.NSOLID_APP || env.NSOLID_APPNAME || fromPkg.app || pkg.name || process.title || "untitled"),
    appVersion: typeof pkg.version === "string" ? pkg.version : undefined,
    hostname: String(env.NSOLID_HOSTNAME || fromPkg.hostname || os.hostname()),
    main,
    // Read through `env`: built-in code sees `process.env.NODE_ENV` replaced
    // with a constant at build time.
    nodeEnv: env.NODE_ENV ?? "",
    blockedLoopThreshold: parseNumber(pick("NSOLID_BLOCKED_LOOP_THRESHOLD", "blockedLoopThreshold"), 200),
    interval: parseNumber(pick("NSOLID_INTERVAL", "interval"), 5000),
    pauseMetrics: parseBool(fromPkg.pauseMetrics, false),
    promiseTracking: parseBool(pick("NSOLID_PROMISE_TRACKING", "promiseTracking"), false),
    redactSnapshots: parseBool(pick("NSOLID_REDACT_SNAPSHOTS", "redactSnapshots"), false),
    statsd: pick("NSOLID_STATSD", "statsd") || undefined,
    statsdBucket: pick("NSOLID_STATSD_BUCKET", "statsdBucket") || undefined,
    statsdTags: pick("NSOLID_STATSD_TAGS", "statsdTags") || undefined,
    tags: parseTags(pick("NSOLID_TAGS", "tags")),
    tracingEnabled: parseBool(pick("NSOLID_TRACING_ENABLED", "tracingEnabled"), false),
    tracingModulesBlacklist: parseNumber(fromPkg.tracingModulesBlacklist, 0),
    contCpuProfile: parseBool(pick("NSOLID_CONT_CPU_PROFILE", "contCpuProfile"), false),
    assetsEnabled: parseBool(pick("NSOLID_ASSETS_ENABLED", "assetsEnabled"), true),
    traceSampleRate: parseNumber(pick("NSOLID_TRACE_SAMPLE_RATE", "traceSampleRate"), 1),
    sourceCodeEnabled: false,
  };
}

const reconfigurable = [
  "blockedLoopThreshold",
  "interval",
  "pauseMetrics",
  "promiseTracking",
  "redactSnapshots",
  "statsd",
  "statsdBucket",
  "statsdTags",
  "tracingEnabled",
  "tracingModulesBlacklist",
  "contCpuProfile",
  "assetsEnabled",
  "traceSampleRate",
  "sourceCodeEnabled",
] as const;

// Applies a `ReconfigureBody`: only the fields it carries change. An empty tag
// list is the same as no tags field, since a repeated field has no presence.
function applyReconfigure(config: Config, body: Record<string, any> | undefined) {
  if (!body) return;
  for (const key of reconfigurable) {
    if (body[key] !== undefined && body[key] !== null) (config as any)[key] = body[key];
  }
  const { tags } = body;
  if (Array.isArray(tags) && tags.length) config.tags = parseTags(tags);
}

// The whole configuration as a `ReconfigureBody`. The Console compares every
// key it manages, so each is always reported; eBPF profiling is not available.
function reconfigureBody(config: Config): Record<string, any> {
  const body: Record<string, any> = { tags: config.tags, ebpfProfiling: EBPF_PROFILING_UNSUPPORTED };
  for (const key of reconfigurable) body[key] = config[key];
  return body;
}

export default { createConfig, applyReconfigure, reconfigureBody };
