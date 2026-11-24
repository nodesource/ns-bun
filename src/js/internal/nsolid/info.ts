// The process's info, as N|Solid reports it (lib/nsolid.js `genInfoObject`):
// what the agent sends for `info`, and what `nsolid.info()` returns.

const os = require("node:os");

// N|Solid's packed kernel version on Linux: major * 65536 + minor * 256 + patch.
function kernelVersion(): number {
  if (process.platform !== "linux") return 0;
  const [major = 0, minor = 0, patch = 0] = os
    .release()
    .split(/[.-]/, 3)
    .map((part: string) => parseInt(part, 10) || 0);
  return major * 65536 + minor * 256 + patch;
}

function infoObject(config, id: string, processStartMs: number) {
  const cpus = os.cpus();
  // The runtime is Bun: the Console reads versions.node as "this is Node.js",
  // and Bun's v8 entry is only what it reports for compatibility.
  const versions: Record<string, string> = {};
  for (const [name, version] of Object.entries(process.versions)) {
    if (name !== "node" && name !== "v8" && typeof version === "string") versions[name] = version;
  }
  return {
    id,
    app: config.app,
    appVersion: config.appVersion,
    tags: config.tags,
    pid: process.pid,
    processStart: Math.round(processStartMs),
    nodeEnv: config.nodeEnv,
    execPath: process.execPath,
    main: config.main,
    arch: process.arch,
    platform: process.platform,
    hostname: config.hostname,
    totalMem: os.totalmem(),
    versions,
    cpuCores: cpus.length,
    cpuModel: cpus[0]?.model ?? "",
    kernelVersion: kernelVersion(),
  };
}

export default { infoObject };
