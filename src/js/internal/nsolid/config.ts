// The agent's configuration, read the way N|Solid reads it (lib/nsolid.js,
// `initializeConfig`): NSOLID_* environment variables first, then the "nsolid"
// key of the application's package.json, then defaults. The Console reads it
// back and changes it with `reconfigure`, which applies N|Solid's
// `updateConfig` rules.

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const EBPF_PROFILING_UNSUPPORTED = 2;
const DEFAULT_APPNAME = "untitled application";
const DEFAULT_INTERVAL = 5000;
const DEFAULT_CONT_CPU_PROFILE_INTERVAL = 30000;
const DEFAULT_BLOCKED_LOOP_THRESHOLD = 200;
const DEFAULT_TRACE_SAMPLE_RATE = 1;
const DEFAULT_STATSD_BUCKET = "nsolid.${env}.${app}.${hostname}.${shortId}";
const MAX_TAGS = 20;
const TAG_MIN_BYTES = 2;
const TAG_MAX_BYTES = 140;
// N|Solid's span types (src/nsolid.h): kSpanDns, kSpanHttpClient | kSpanHttpServer.
const TRACING_MODULES: Record<string, number> = { dns: 1 << 0, http: (1 << 2) | (1 << 3) };

type Env = Record<string, string | undefined>;

interface Config {
  app: string;
  appVersion: string | undefined;
  hostname: string;
  main: string;
  nodeEnv: string;
  env: string;
  blockedLoopThreshold: number;
  interval: number;
  pauseMetrics: boolean;
  promiseTracking: boolean;
  disableSnapshots: boolean;
  redactSnapshots: boolean;
  statsd: string | undefined;
  statsdBucket: string;
  statsdTags: string | undefined;
  tags: string[];
  tracingEnabled: boolean;
  tracingModulesBlacklist: number;
  contCpuProfile: boolean;
  contCpuProfileInterval: number;
  assetsEnabled: boolean;
  traceSampleRate: number;
  sourceCodeEnabled: boolean;
  disablePackageScan: boolean;
  trackGlobalPackages: boolean;
}

function warn(message: string) {
  process.stderr.write(message + "\n");
}

function envToBool(value: unknown): boolean {
  return !!value && value !== "0" && value !== "false";
}

function optionToBool(value: unknown): boolean | undefined {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  if (typeof value === "string") return envToBool(value);
  return undefined;
}

function toNumber(value: unknown): number | undefined {
  if (typeof value === "number") return value;
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed === "" ? undefined : +trimmed;
}

function parseInterval(value: unknown): number | undefined {
  const n = toNumber(value);
  return n !== undefined && Number.isSafeInteger(n) && n > 0 ? n : undefined;
}

function parseTraceSampleRate(value: unknown): number | undefined {
  const n = toNumber(value);
  return n !== undefined && Number.isFinite(n) && n >= 0 && n <= 1 ? n : undefined;
}

function parseContCpuProfileInterval(value: unknown): number | undefined {
  if (value === undefined || value === "") return undefined;
  const n = +(value as any);
  if (!Number.isSafeInteger(n) || n <= 0) {
    warn("N|Solid warning: invalid contCpuProfileInterval: expected a positive integer");
    return undefined;
  }
  return n;
}

function parseTracingModules(value: unknown): number {
  const list = typeof value === "string" ? value.split(",") : Array.isArray(value) ? value : [];
  let mask = 0;
  for (const name of list) mask += TRACING_MODULES[String(name).trim()] || 0;
  return mask;
}

// N|Solid's `getTags`: a comma-separated list or an array, each tag trimmed,
// sorted, empty ones dropped. A tag is 2 to 140 bytes: shorter ones are
// dropped and longer ones cut at a character boundary, with a warning. At most
// 20 are kept.
function parseTags(value: unknown): string[] {
  if (Array.isArray(value)) value = value.join(",");
  if (typeof value !== "string") return [];
  let tags = value
    .split(",")
    .map(tag => tag.trim())
    .sort()
    .filter(tag => tag);

  const violations: string[] = [];
  for (let i = 0; i < tags.length; i++) {
    const bytes = Buffer.from(tags[i]);
    if (bytes.length < TAG_MIN_BYTES) {
      violations.push(tags[i]);
    } else if (bytes.length > TAG_MAX_BYTES) {
      violations.push(tags[i]);
      let end = TAG_MAX_BYTES;
      while ((bytes[end] & 0xc0) === 0x80) end--;
      tags[i] = bytes.subarray(0, end).toString();
    }
  }
  if (violations.length) {
    warn("Tags must be between 2 to 140 bytes. The following are violations:\n  " + violations.join("\n  "));
  }

  tags = tags.filter(tag => Buffer.byteLength(tag) >= TAG_MIN_BYTES);
  if (tags.length > MAX_TAGS) {
    warn(`A maximum of 20 tags are allowed but ${tags.length} were provided. Only using the first 20 tags`);
    tags = tags.slice(0, MAX_TAGS);
  }
  return tags;
}

function withLocalhost(address: unknown): string | undefined {
  if (!address) return undefined;
  const value = String(address);
  return +value ? `localhost:${value}` : value;
}

function readJson(file: string): Record<string, any> | undefined {
  try {
    const value = JSON.parse(fs.readFileSync(file, "utf8"));
    return value && typeof value === "object" ? value : undefined;
  } catch {
    return undefined;
  }
}

// N|Solid's `loadMainPackageJson`: NSOLID_PACKAGE_JSON alone when it is set
// (an empty value means none), or the nearest package.json above the main
// script, or above the working directory when there is none.
function readPackageJson(env: Env, main: string): Record<string, any> {
  const file = env.NSOLID_PACKAGE_JSON;
  if (file !== undefined) return (file && readJson(file)) || {};
  let dir: string;
  try {
    dir = fs.realpathSync(main ? path.dirname(main) : process.cwd());
  } catch {
    return {};
  }
  for (;;) {
    const json = readJson(path.join(dir, "package.json"));
    if (json) return json;
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
  const nsolid = pkg.nsolid && typeof pkg.nsolid === "object" ? pkg.nsolid : {};

  const traceSampleRate =
    parseTraceSampleRate(env.NSOLID_TRACE_SAMPLE_RATE) ??
    parseTraceSampleRate(nsolid.traceSampleRate) ??
    DEFAULT_TRACE_SAMPLE_RATE;

  return {
    app: String(env.NSOLID_APPNAME || env.NSOLID_APP || nsolid.app || pkg.name || DEFAULT_APPNAME),
    appVersion: typeof pkg.version === "string" ? pkg.version : undefined,
    hostname: String(env.NSOLID_HOSTNAME || nsolid.hostname || os.hostname()),
    main,
    // Read through `env`: built-in code sees `process.env.NODE_ENV` replaced
    // with a constant at build time.
    nodeEnv: env.NODE_ENV ?? "",
    env: String(env.NODE_ENV || nsolid.env || "prod"),
    blockedLoopThreshold:
      +(env.NSOLID_BLOCKED_LOOP_THRESHOLD as any) || +nsolid.blockedLoopThreshold || DEFAULT_BLOCKED_LOOP_THRESHOLD,
    interval: parseInterval(env.NSOLID_INTERVAL) ?? parseInterval(nsolid.interval) ?? DEFAULT_INTERVAL,
    pauseMetrics: false,
    promiseTracking: envToBool(env.NSOLID_PROMISE_TRACKING) || !!nsolid.promiseTracking,
    // Neither can be turned off by a later source once one turns it on.
    disableSnapshots: envToBool(env.NSOLID_DISABLE_SNAPSHOTS) || !!nsolid.disableSnapshots,
    redactSnapshots: envToBool(env.NSOLID_REDACT_SNAPSHOTS) || !!nsolid.redactSnapshots,
    statsd: withLocalhost(env.NSOLID_STATSD || nsolid.statsd),
    statsdBucket: String(env.NSOLID_STATSD_BUCKET || nsolid.statsdBucket || DEFAULT_STATSD_BUCKET),
    statsdTags: env.NSOLID_STATSD_TAGS || nsolid.statsdTags || undefined,
    tags: parseTags(env.NSOLID_TAGS || nsolid.tags),
    tracingEnabled: envToBool(env.NSOLID_TRACING_ENABLED) || !!nsolid.tracingEnabled,
    tracingModulesBlacklist: parseTracingModules(
      env.NSOLID_TRACING_MODULES_BLACKLIST || nsolid.tracingModulesBlacklist,
    ),
    contCpuProfile: envToBool(env.NSOLID_CONT_CPU_PROFILE) || !!nsolid.contCpuProfile,
    contCpuProfileInterval:
      parseContCpuProfileInterval(env.NSOLID_CONT_CPU_PROFILE_INTERVAL) ??
      parseContCpuProfileInterval(nsolid.contCpuProfileInterval) ??
      DEFAULT_CONT_CPU_PROFILE_INTERVAL,
    assetsEnabled: optionToBool(env.NSOLID_ASSETS_ENABLED) ?? optionToBool(nsolid.assetsEnabled) ?? true,
    traceSampleRate,
    sourceCodeEnabled: false,
    disablePackageScan: envToBool(env.NSOLID_DISABLE_PACKAGE_SCAN) || !!nsolid.disablePackageScan,
    trackGlobalPackages: envToBool(env.NSOLID_TRACK_GLOBAL_PACKAGES) || !!nsolid.trackGlobalPackages,
  };
}

// The `ReconfigureBody` keys besides tags and ebpfProfiling, each with how
// N|Solid's `updateConfig` takes a new value; `undefined` leaves the key as it
// is.
const reconfigurable: Record<string, (value: any) => unknown> = {
  blockedLoopThreshold: Number,
  interval: parseInterval,
  pauseMetrics: Boolean,
  promiseTracking: Boolean,
  redactSnapshots: Boolean,
  statsd: String,
  statsdBucket: String,
  statsdTags: String,
  tracingEnabled: Boolean,
  tracingModulesBlacklist: Number,
  contCpuProfile: envToBool,
  assetsEnabled: optionToBool,
  traceSampleRate: parseTraceSampleRate,
  sourceCodeEnabled: Boolean,
};

// Applies a `ReconfigureBody`: only the fields it carries change. An empty tag
// list is the same as no tags field, since a repeated field has no presence.
function applyReconfigure(config: Config, body: Record<string, any> | undefined) {
  if (!body) return;
  for (const key in reconfigurable) {
    if (body[key] === undefined || body[key] === null) continue;
    const value = reconfigurable[key](body[key]);
    if (value !== undefined) (config as any)[key] = value;
  }
  if (Array.isArray(body.tags) && body.tags.length) config.tags = parseTags(body.tags);
}

// The whole configuration as a `ReconfigureBody`. The Console compares every
// key it manages, so each is always reported; eBPF profiling is not available.
function reconfigureBody(config: Config): Record<string, any> {
  const body: Record<string, any> = { tags: config.tags, ebpfProfiling: EBPF_PROFILING_UNSUPPORTED };
  for (const key in reconfigurable) body[key] = (config as any)[key];
  return body;
}

export default { createConfig, applyReconfigure, reconfigureBody };
