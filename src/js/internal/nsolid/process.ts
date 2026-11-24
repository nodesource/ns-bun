// The process's metrics, under the names of the Console's catalog (N|Solid's
// src/nsolid.h `NSOLID_PROCESS_METRICS`). Process-wide, so any thread can
// read them.

const os = require("node:os");

export interface ProcessMetrics {
  timestamp: number;
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
}

// CPU percentages are over the time since the previous sample, of one core:
// above 100 when several threads are busy.
class ProcessProbe {
  #lastCpu = process.cpuUsage();
  #lastHrtime = process.hrtime.bigint();

  constructor(public processStartMs: number) {}

  sample(): ProcessMetrics {
    const now = Date.now();
    const cpu = process.cpuUsage();
    const hrtime = process.hrtime.bigint();
    const elapsedUs = Number(hrtime - this.#lastHrtime) / 1000;
    const percent = (us: number) => (elapsedUs > 0 ? (us / elapsedUs) * 100 : 0);
    const cpuUserPercent = percent(cpu.user - this.#lastCpu.user);
    const cpuSystemPercent = percent(cpu.system - this.#lastCpu.system);
    this.#lastCpu = cpu;
    this.#lastHrtime = hrtime;

    const usage = process.resourceUsage();
    const [load1m, load5m, load15m] = os.loadavg();
    return {
      timestamp: now,
      uptime: Math.floor((now - this.processStartMs) / 1000),
      systemUptime: Math.floor(os.uptime()),
      freeMem: os.freemem(),
      totalMem: os.totalmem(),
      blockInputOpCount: usage.fsRead,
      blockOutputOpCount: usage.fsWrite,
      ctxSwitchInvoluntaryCount: usage.involuntaryContextSwitches,
      ctxSwitchVoluntaryCount: usage.voluntaryContextSwitches,
      ipcReceivedCount: usage.ipcReceived,
      ipcSentCount: usage.ipcSent,
      pageFaultHardCount: usage.majorPageFault,
      pageFaultSoftCount: usage.minorPageFault,
      signalCount: usage.signalsCount,
      swapCount: usage.swappedOut,
      rss: process.memoryUsage.rss(),
      load1m,
      load5m,
      load15m,
      cpuUserPercent,
      cpuSystemPercent,
      cpuPercent: cpuUserPercent + cpuSystemPercent,
    };
  }
}

export default { ProcessProbe };
