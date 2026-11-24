// Each JavaScript thread's heap, read on that thread for the agent, under the
// names of the Console's catalog (N|Solid's src/nsolid.h).

const requests = require("internal/nsolid/requests");

const nsolidThreads = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidThreads", 0);
const requestThreadHeap = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidRequestThreadHeap", 2);

// A thread busy with a long synchronous task answers late: its figures are
// left out of this round rather than holding up the others.
const THREAD_REPLY_TIMEOUT_MS = 1000;

interface ThreadHeap {
  heapTotal: number;
  heapUsed: number;
  external: number;
  arrayBuffers: number;
}

export interface ThreadMetrics extends ThreadHeap {
  threadId: number;
  threadName: string;
  timestamp: number;
}

function threadHeap(contextId: number): Promise<ThreadHeap | undefined> {
  return requests.ask(requestThreadHeap, contextId, THREAD_REPLY_TIMEOUT_MS);
}

// Every running JavaScript thread's heap. A thread that ended, or did not
// answer in time, is left out.
async function threadMetrics(): Promise<ThreadMetrics[]> {
  const threads = nsolidThreads() as { contextId: number; threadId: number; name: string }[];
  const answers = await Promise.all(threads.map(thread => threadHeap(thread.contextId)));
  const metrics: ThreadMetrics[] = [];
  for (let i = 0; i < threads.length; i++) {
    const heap = answers[i];
    if (!heap) continue;
    metrics.push({ threadId: threads[i].threadId, threadName: threads[i].name, timestamp: Date.now(), ...heap });
  }
  return metrics;
}

export default { threadMetrics };
