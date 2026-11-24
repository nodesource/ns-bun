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

const agentId = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidAgentId", 0);
const sharedConfig = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidSharedConfig", 0);
const startAgent = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidStart", 1);
const processStart = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidProcessStart", 0);
const loadedModules = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidLoadedModules", 0);

const DEFAULT_APPNAME = "untitled application";

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
  localUpdates.push(update);
  if (localConfig) nsolidConfig.applyUpdate(localConfig, update);
  if (agentRequested()) {
    startAgent(JSON.stringify(update));
  } else if ((update as any).grpc || (update as any).saas) {
    for (const each of localUpdates) startAgent(JSON.stringify(each));
  }
  return start;
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
    user = os.userInfo().username;
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
  return maybeCallback(inventory.packagesOf(loadedModules()).slice(), callback);
}

Object.assign(start, { start, id, info, metrics, packages });
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
