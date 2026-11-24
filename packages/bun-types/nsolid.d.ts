/**
 * The N|Solid API of nsolid-bun, NodeSource's build of Bun. `require("nsolid")`
 * is an alias of `bun:nsolid`, as in N|Solid for Node.js: the module is
 * {@link start}, with the rest of the API on it.
 *
 * The agent that reports to the N|Solid Console starts when `NSOLID_GRPC` or
 * `NSOLID_SAAS` is set, or when {@link start} is given `grpc` or `saas`.
 *
 * @example
 * ```ts
 * import nsolid from "nsolid";
 *
 * console.log(nsolid.id); // "2f7c…", 40 hexadecimal digits
 * console.log(nsolid.info().app);
 * ```
 */
declare module "bun:nsolid" {
  namespace nsolid {
    /** The configuration, as the agent applies it. Keys that are not set are left out. */
    interface Config {
      grpc?: string;
      app: string;
      appVersion?: string;
      hostname: string;
      tags: string[];
      interval: number;
      blockedLoopThreshold: number;
      pauseMetrics: boolean;
      promiseTracking: boolean;
      disableSnapshots: boolean;
      redactSnapshots: boolean;
      statsd?: string;
      statsdBucket: string;
      statsdTags?: string;
      tracingEnabled: boolean;
      tracingModulesBlacklist: number;
      traceSampleRate: number;
      contCpuProfile: boolean;
      contCpuProfileInterval: number;
      assetsEnabled: boolean;
      disablePackageScan: boolean;
      trackGlobalPackages: boolean;
      [key: string]: unknown;
    }

    interface Info {
      id: string;
      app: string;
      appVersion?: string;
      tags: string[];
      pid: number;
      /** When the process started, in milliseconds since the epoch. */
      processStart: number;
      nodeEnv: string;
      execPath: string;
      main: string;
      arch: string;
      platform: string;
      hostname: string;
      totalMem: number;
      /** `process.versions`, without `node` and `v8`: the runtime is Bun. */
      versions: Record<string, string>;
      cpuCores: number;
      cpuModel: string;
      /** On Linux, major * 65536 + minor * 256 + patch; 0 elsewhere. */
      kernelVersion: number;
    }

    /** The calling thread's metrics, then the process's. */
    interface Metrics {
      threadName: string;
      threadId: number;
      timestamp: number;
      heapTotal: number;
      heapUsed: number;
      externalMem: number;
      title: string;
      user: string;
      uptime: number;
      systemUptime: number;
      freeMem: number;
      totalMem: number;
      blockInputOpCount: number;
      blockOutputOpCount: number;
      ctxSwitchInvoluntaryCount: number;
      ctxSwitchVoluntaryCount: number;
      ipcReceivedCount: number;
      ipcSentCount: number;
      pageFaultHardCount: number;
      pageFaultSoftCount: number;
      signalCount: number;
      swapCount: number;
      rss: number;
      load1m: number;
      load5m: number;
      load15m: number;
      cpuUserPercent: number;
      cpuSystemPercent: number;
      cpuPercent: number;
      /** The same as `cpuPercent`. */
      cpu: number;
    }

    interface Package {
      path: string;
      name: string;
      version?: string;
      main?: string;
      /** Each dependency's path relative to `path`, or its name if it did not resolve. */
      dependencies: string[];
      /** Whether the calling thread loaded a module of the package. */
      required: boolean;
    }

    type Callback<T> = (error: null, value: T) => void;
  }

  interface NSolid {
    /**
     * Applies `config`, validated as N|Solid validates it, and starts the
     * agent when `config` names an endpoint (`grpc` or `saas`) and none runs
     * yet. Returns the module; off the main thread it does nothing.
     */
    (config?: Partial<nsolid.Config> & { saas?: string }): NSolid | undefined;
    start(config?: Partial<nsolid.Config> & { saas?: string }): NSolid | undefined;
    /**
     * The id the N|Solid Console knows this process by: 40 hexadecimal
     * digits. One per process, the same on every thread, whether or not the
     * agent is connected.
     */
    readonly id: string;
    readonly config: nsolid.Config;
    readonly app: string;
    readonly appName: string;
    readonly appVersion: string | undefined;
    readonly tags: string[];
    readonly metricsPaused: boolean;
    readonly assetsEnabled: boolean;
    readonly tracingEnabled: boolean;
    info(): nsolid.Info;
    info(callback: nsolid.Callback<nsolid.Info>): void;
    metrics(): nsolid.Metrics;
    metrics(callback: nsolid.Callback<nsolid.Metrics>): void;
    packages(): nsolid.Package[];
    packages(callback: nsolid.Callback<nsolid.Package[]>): void;
  }

  const nsolid: NSolid;
  export default nsolid;
}

declare module "nsolid" {
  export { default } from "bun:nsolid";
}
