// A CPU profile from Bun's sampling profiler, given the nodes V8's profiles
// have and the N|Solid Console relies on. JavaScriptCore samples a thread only
// while it runs JavaScript; V8 samples it all the time. So:
// - the time between samples is filled with samples, one per sampling
//   interval, up to when the profile stopped, so that the samples cover the
//   whole profile (the Console spreads its duration evenly over them). They
//   are `(idle)` where the thread's loop waited, which the Console counts as
//   not busy, `(garbage collector)` where it collected, and `(program)`
//   elsewhere;
// - a sample with no JavaScript frame is `(program)`, or `(garbage collector)`
//   when the thread was collecting then.

// Bun's sampling interval (BunCPUProfiler.cpp), in µs.
const SAMPLING_INTERVAL_US = 1000;

interface ProfileNode {
  id: number;
  callFrame: { functionName: string; scriptId: string; url: string; lineNumber: number; columnNumber: number };
  hitCount?: number;
  children?: number[];
}

// Whether times, asked in increasing order, fall in one of `windows`
// ([start, end, start, end, ...], in order).
function windowsAt(windows: number[]) {
  let i = 0;
  return (time: number) => {
    while (i + 1 < windows.length && windows[i + 1] < time) i += 2;
    return i + 1 < windows.length && time >= windows[i];
  };
}

// `collections` and `waits` are [start, end, start, end, ...] in µs since the
// epoch, the clock of the profile's times; `waits` is in order.
function completeProfile(json: string, stoppedAt: number, collections: number[], waits: number[]): string {
  const profile = JSON.parse(json);
  const nodes: ProfileNode[] = profile.nodes;
  const root = nodes.find(node => node.id === 1)!;
  let nextId = nodes.reduce((max, node) => Math.max(max, node.id), 0) + 1;
  const engineNode = (functionName: string): ProfileNode => ({
    id: nextId++,
    callFrame: { functionName, scriptId: "0", url: "", lineNumber: -1, columnNumber: -1 },
    hitCount: 0,
    children: [],
  });
  const program = engineNode("(program)");
  const idle = engineNode("(idle)");
  const collecting = engineNode("(garbage collector)");
  const pairs: [number, number][] = [];
  for (let i = 0; i + 1 < collections.length; i += 2) pairs.push([collections[i], collections[i + 1]]);
  pairs.sort((a, b) => a[0] - b[0]);
  const collectingAt = windowsAt(pairs.flat());
  const waitingAt = windowsAt(waits);

  const samples: number[] = [];
  const timeDeltas: number[] = [];
  let last = profile.startTime;
  const fillUntil = (time: number) => {
    while (time - last >= 2 * SAMPLING_INTERVAL_US) {
      last += SAMPLING_INTERVAL_US;
      const engine = collectingAt(last) ? collecting : waitingAt(last) ? idle : program;
      samples.push(engine.id);
      timeDeltas.push(SAMPLING_INTERVAL_US);
      engine.hitCount!++;
    }
  };
  let time = profile.startTime;
  for (let i = 0; i < profile.samples.length; i++) {
    time += profile.timeDeltas[i];
    fillUntil(time);
    let id = profile.samples[i];
    if (id === root.id) {
      const engine = collectingAt(time) ? collecting : program;
      engine.hitCount!++;
      id = engine.id;
    }
    samples.push(id);
    timeDeltas.push(Math.max(0, Math.round(time - last)));
    last = time;
  }
  fillUntil(stoppedAt);

  for (const engine of [program, idle, collecting]) {
    if (!engine.hitCount) continue;
    nodes.push(engine);
    (root.children ??= []).push(engine.id);
  }
  profile.samples = samples;
  profile.timeDeltas = timeDeltas;
  profile.endTime = Math.max(last, stoppedAt);
  return JSON.stringify(profile);
}

export default { completeProfile };
