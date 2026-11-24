// The public N|Solid API: `require("nsolid")`, which is an alias of
// `bun:nsolid`, as in N|Solid for Node.js (lib/nsolid.js). The module is
// `start`, with the rest of the API on it.
//
// The configuration lives on the agent's thread, which publishes it whenever
// it or the Console changes it. Without an agent, the module keeps its own,
// read the same way from the environment and package.json.

const os = require("node:os");
const { isMainThread, threadId } = require("node:worker_threads");
const nsolidConfig = require("internal/nsolid/config");
const nsolidInfo = require("internal/nsolid/info");
const nsolidProcess = require("internal/nsolid/process");
const nsolidPackages = require("internal/nsolid/packages");
const { validateBoolean, validateFunction, validateNumber, validateObject } = require("internal/validators");

const agentId = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidAgentId", 0);
const sharedConfig = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidSharedConfig", 0);
const publishConfig = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidPublishConfig", 1);
const startAgent = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidStart", 1);
const processStart = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidProcessStart", 0);
const loadedModules = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidLoadedModules", 0);
const apiCommand = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidApiCommand", 2);
const apiProfileEnd = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidApiProfileEnd", 0);

const DEFAULT_APPNAME = "untitled application";
// What a profile from code runs for at most, as in N|Solid.
const DEFAULT_PROFILE_MS = 600_000;
// N|Solid's statuses (grpc_errors.h): what is not there, and assets or
// snapshots turned off.
const NOT_AVAILABLE = 404;
const DISABLED = 500;

const id: string = agentId();
const mainPath: string = Bun.main;

// What `start(config)` was given, in order, while no agent was running: an
// agent started later applies them all.
const localUpdates: object[] = [];
let localConfig;

function currentConfig() {
  const shared = sharedConfig();
  if (shared !== undefined) return JSON.parse(shared);
  if (!localConfig) {
    localConfig = nsolidConfig.createConfig(process.env, mainPath);
    for (const update of localUpdates) nsolidConfig.applyUpdate(localConfig, update);
  }
  return localConfig;
}

// An agent runs, or will once its thread is up, when the environment names
// an endpoint or the agent has published its configuration.
function agentRequested(): boolean {
  return sharedConfig() !== undefined || !!process.env.NSOLID_GRPC || !!process.env.NSOLID_SAAS;
}

/**
 * Applies `config` (N|Solid's `updateConfig` rules) and starts the agent when
 * `config` names an endpoint (`grpc` or `saas`) and none runs yet. Returns
 * the module; off the main thread it does nothing and returns undefined.
 */
function start(config?: Record<string, unknown>) {
  if (!isMainThread) return undefined;
  const update = config && typeof config === "object" ? config : {};
  updateConfig(update);
  if (!agentRequested() && ((update as any).grpc || (update as any).saas)) {
    for (const each of localUpdates) startAgent(JSON.stringify(each));
  }
  return start;
}

// N|Solid's `updateConfig`: the agent, if one runs, takes the update too,
// and every thread reads the result at once.
function updateConfig(update: Record<string, unknown>) {
  localUpdates.push(update);
  if (localConfig) nsolidConfig.applyUpdate(localConfig, update);
  const shared = sharedConfig();
  if (shared !== undefined) {
    const config = JSON.parse(shared);
    nsolidConfig.applyUpdate(config, update);
    publishConfig(JSON.stringify(config));
  }
  if (agentRequested()) startAgent(JSON.stringify(update));
}

function enableAssets() {
  if (currentConfig().assetsEnabled !== true) updateConfig({ assetsEnabled: true });
}

function disableAssets() {
  if (currentConfig().assetsEnabled !== false) updateConfig({ assetsEnabled: false });
}

// N|Solid's asset calls: an error with N|Solid's message and, as its `code`,
// the status; given to the callback on the next tick, or thrown.
function settle(status: number, message: string, cb: ((err?: Error) => void) | undefined) {
  let err: Error | undefined;
  if (status !== 0) {
    err = new Error(message);
    (err as unknown as { code: number }).code = status;
  }
  if (cb) process.nextTick(() => cb(err));
  else if (err) throw err;
}

function assetStatus(command: "profile" | "snapshot", value: number) {
  const config = currentConfig();
  if (config.assetsEnabled === false || (command === "snapshot" && config.disableSnapshots)) return DISABLED;
  return apiCommand(command, value);
}

/**
 * A CPU profile of the calling thread for `timeout` ms (10 minutes by
 * default), or until `profileEnd()`; the agent sends it to the Console.
 */
function profile(timeout?: number | ((err?: Error) => void), cb?: (err?: Error) => void) {
  if (typeof timeout === "function") {
    cb = timeout;
    timeout = undefined;
  }
  timeout ||= DEFAULT_PROFILE_MS;
  validateNumber(timeout, "timeout");
  if (cb !== undefined) validateFunction(cb, "profileCallback");
  settle(assetStatus("profile", timeout), "CPU profile could not be started", cb);
}

function profileEnd(cb?: (err?: Error) => void) {
  if (cb !== undefined) validateFunction(cb, "profileEndCallback");
  settle(apiProfileEnd(), "CPU profile could not be stopped", cb);
}

/** A heap snapshot of the calling thread, which the agent sends to the Console. */
function snapshot(cb?: (err?: Error) => void) {
  if (cb !== undefined) validateFunction(cb, "snapshotCallback");
  settle(assetStatus("snapshot", 0), "Heap snapshot could not be generated", cb);
}

// Heap profiles and heap sampling take N|Solid's arguments, and are not
// available: JavaScriptCore has neither.
function heapProfile(timeout?: unknown, trackAllocations?: unknown, cb?: unknown) {
  if (typeof timeout === "function") {
    cb = timeout;
    timeout = undefined;
    trackAllocations = false;
  } else if (typeof trackAllocations === "function") {
    cb = trackAllocations;
    trackAllocations = false;
  }
  timeout ||= DEFAULT_PROFILE_MS;
  trackAllocations ||= false;
  validateNumber(timeout, "timeout");
  validateBoolean(trackAllocations, "trackAllocations");
  if (cb !== undefined) validateFunction(cb, "heapProfileCallback");
  settle(NOT_AVAILABLE, "Heap profile could not be started", cb as (err?: Error) => void);
}

function heapProfileEnd(cb?: (err?: Error) => void) {
  if (cb !== undefined) validateFunction(cb, "heapProfileEndCallback");
  settle(NOT_AVAILABLE, "Heap profile could not be stopped", cb);
}

function heapSampling(duration?: unknown, options?: unknown, cb?: unknown) {
  if (typeof duration === "function") {
    cb = duration;
    duration = undefined;
    options = undefined;
  } else if (typeof options === "function") {
    cb = options;
    options = undefined;
  }
  duration ||= DEFAULT_PROFILE_MS;
  options ||= {};
  validateObject(options, "options");
  validateNumber(duration, "duration");
  if (cb !== undefined) validateFunction(cb, "heapSamplingCallback");
  settle(NOT_AVAILABLE, "Heap sampling could not be started", cb as (err?: Error) => void);
}

function heapSamplingEnd(cb?: (err?: Error) => void) {
  if (cb !== undefined) validateFunction(cb, "heapSamplingEndCallback");
  settle(NOT_AVAILABLE, "Heap sampling could not be stopped", cb);
}

// N|Solid's callback form: the value, on the next tick.
function maybeCallback<T>(value: T, callback: unknown): T | undefined {
  if (typeof callback !== "function") return value;
  process.nextTick(callback as (err: null, value: T) => void, null, value);
}

function info(callback?: (err: null, info: object) => void) {
  return maybeCallback(nsolidInfo.infoObject(currentConfig(), id, processStart()), callback);
}

let probe;
// The calling thread's metrics, then the process's, as one object.
function metrics(callback?: (err: null, metrics: object) => void) {
  probe ??= new nsolidProcess.ProcessProbe(processStart());
  const proc = probe.sample();
  const memory = process.memoryUsage();
  let user = "";
  try {
    user = String(os.userInfo().username);
  } catch {}
  const result = {
    threadName: "",
    threadId,
    timestamp: proc.timestamp,
    heapTotal: memory.heapTotal,
    heapUsed: memory.heapUsed,
    externalMem: memory.external,
    title: process.title,
    user,
    ...proc,
    cpu: proc.cpuPercent,
  };
  return maybeCallback(result, callback);
}

let inventory;
// The packages the main script can require, those this thread loaded marked
// `required`.
function packages(callback?: (err: null, packages: object[]) => void) {
  const config = currentConfig();
  inventory ??= new nsolidPackages.Inventory(
    mainPath,
    !config.disablePackageScan,
    config.trackGlobalPackages ? (require("node:module").globalPaths ?? []) : [],
  );
  return maybeCallback(inventory.packagesAtOnce(loadedModules()).slice(), callback);
}

Object.assign(start, {
  start,
  id,
  info,
  metrics,
  packages,
  profile,
  profileEnd,
  snapshot,
  heapProfile,
  heapProfileEnd,
  heapSampling,
  heapSamplingEnd,
  enableAssets,
  disableAssets,
});
Object.defineProperties(start, {
  config: {
    enumerable: true,
    get() {
      const config = { ...currentConfig() };
      for (const key of Object.keys(config)) if (config[key] === undefined || config[key] === null) delete config[key];
      return config;
    },
  },
  app: { enumerable: true, get: () => currentConfig().app || DEFAULT_APPNAME },
  appName: { enumerable: true, get: () => currentConfig().app || DEFAULT_APPNAME },
  appVersion: { enumerable: true, get: () => currentConfig().appVersion },
  tags: { enumerable: true, get: () => [...(currentConfig().tags ?? [])] },
  metricsPaused: { enumerable: true, get: () => !!currentConfig().pauseMetrics },
  assetsEnabled: { enumerable: true, get: () => currentConfig().assetsEnabled ?? true },
  tracingEnabled: { enumerable: true, get: () => currentConfig().tracingEnabled ?? false },
});

export default start;
