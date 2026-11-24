// Each JavaScript thread's heap, read on that thread for the agent, and its
// event-loop and GC figures, read from the agent's thread, under the names of
// the Console's catalog (N|Solid's src/nsolid.h).

const requests = require("internal/nsolid/requests");

const nsolidThreads = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidThreads", 0);
const requestThreadHeap = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidRequestThreadHeap", 2);
const nsolidThreadStats = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidThreadStats", 0);

// A thread busy with a long synchronous task answers late: its figures are
// left out of this round rather than holding up the others.
const THREAD_REPLY_TIMEOUT_MS = 1000;

interface ThreadHeap {
  heapTotal: number;
  heapUsed: number;
  external: number;
  arrayBuffers: number;
}

export interface ThreadMetrics extends Partial<ThreadHeap> {
  threadId: number;
  threadName: string;
  timestamp: number;
  [loopOrGcMetric: string]: number | string | undefined;
}

function threadHeap(contextId: number): Promise<ThreadHeap | undefined> {
  return requests.ask(requestThreadHeap, contextId, THREAD_REPLY_TIMEOUT_MS);
}

// A thread's figures at the previous collection, from which that interval's
// loop utilization, idle percent and average tasks come (N|Solid's
// ThreadMetrics). Until a first collection, the thread's registration.
interface Baseline {
  time: number;
  idle: number;
  iterations: number;
  events: number;
}
const baselines = new Map<number, Baseline>();

// N|Solid's EnvInst::get_event_loop_stats_ and get_gc_stats_.
function loopAndGcMetrics(stats: Record<string, number>): Record<string, number> {
  const now = stats.nowNs;
  const previous = baselines.get(stats.threadId) ?? {
    time: stats.registeredNs,
    idle: stats.registeredIdleNs,
    iterations: 0,
    events: 0,
  };
  const loopDiff = now - previous.time;
  const idleDiff = Math.min(stats.loopIdleTime - previous.idle, loopDiff);
  const utilization = loopDiff > 0 ? (loopDiff - idleDiff) / loopDiff : 0;
  const iterationsDiff = stats.loopIterations - previous.iterations;
  // The loop's exit time is 0 while it waits. If it has not polled yet, it has
  // been running since the thread registered.
  const exit = stats.loopEntryNs || stats.loopExitNs ? stats.loopExitNs : stats.registeredNs;
  baselines.set(stats.threadId, {
    time: now,
    idle: stats.loopIdleTime,
    iterations: stats.loopIterations,
    events: stats.eventsProcessed,
  });
  return {
    gcCount: stats.gcCount,
    gcForcedCount: stats.gcForcedCount,
    gcFullCount: stats.gcFullCount,
    gcMajorCount: stats.gcMajorCount,
    gcDurUsMedian: stats.gcDurUsMedian,
    gcDurUs99Ptile: stats.gcDurUs99Ptile,
    loopIdleTime: stats.loopIdleTime,
    loopIterations: stats.loopIterations,
    loopIterWithEvents: stats.loopIterWithEvents,
    eventsProcessed: stats.eventsProcessed,
    eventsWaiting: stats.eventsWaiting,
    providerDelay: stats.providerDelay,
    processingDelay: stats.processingDelay,
    loopTotalCount: stats.loopIterations,
    loopUtilization: utilization,
    res5s: stats.res5s,
    res1m: stats.res1m,
    res5m: stats.res5m,
    res15m: stats.res15m,
    loopAvgTasks: iterationsDiff > 0 ? (stats.eventsProcessed - previous.events) / iterationsDiff : 0,
    // Stuck outside the poll since before the last collection: the time since
    // it left the poll. Otherwise the moving average of iteration times.
    loopEstimatedLag: exit > 0 && previous.time > exit ? (now - exit) / 1e6 : stats.rollingEstimatedLag,
    loopIdlePercent: (1 - utilization) * 100,
  };
}

// Every running JavaScript thread's heap, loop and GC figures. A thread that
// ended is left out; one that did not answer in time has no heap figures.
async function threadMetrics(): Promise<ThreadMetrics[]> {
  const threads = nsolidThreads() as { contextId: number; threadId: number; name: string }[];
  // Read before waiting on the threads' heaps, so that a thread busy with a
  // long task shows as busy.
  const timestamp = Date.now();
  const loopAndGc = new Map<number, Record<string, number>>();
  for (const stats of nsolidThreadStats() as Record<string, number>[]) {
    loopAndGc.set(stats.threadId, loopAndGcMetrics(stats));
  }
  for (const threadId of baselines.keys()) if (!loopAndGc.has(threadId)) baselines.delete(threadId);
  const answers = await Promise.all(threads.map(thread => threadHeap(thread.contextId)));
  const metrics: ThreadMetrics[] = [];
  for (let i = 0; i < threads.length; i++) {
    const { threadId, name } = threads[i];
    const heap = answers[i];
    const loop = loopAndGc.get(threadId);
    if (!heap && !loop) continue;
    metrics.push({ threadId, threadName: name, timestamp, ...heap, ...loop });
  }
  return metrics;
}

export default { threadMetrics };
