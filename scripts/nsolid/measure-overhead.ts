// Measures what the N|Solid agent costs an nsolid-bun process, and compares it
// with the overhead budget: startup, resident memory, idle CPU, and the
// throughput and latency of hello-world servers (Bun.serve and node:http).
// Each is measured without the agent, with it (metrics, event-loop and GC
// counters), and with tracing on at 100% and at 10% sampling. The agent
// reports to stub-gateway.ts, which this script starts.
//
//   bun scripts/nsolid/measure-overhead.ts <nsolid-bun binary> [--json <file>] [--markdown <file>]
//
// With `taskset` and at least 4 CPUs, the measured process runs on CPU 0
// alone, so what its agent thread spends comes out of the core that serves
// requests: a one-core container. The gateway runs on CPU 1, and two load
// clients on CPUs 2 and 3.
import { spawnSync } from "node:child_process";
import { appendFileSync, readFileSync, writeFileSync } from "node:fs";
import { availableParallelism } from "node:os";
import { join } from "node:path";
import { parseArgs } from "node:util";

const SELF = import.meta.path;
const GATEWAY = join(import.meta.dir, "stub-gateway.ts");
// Latency histogram: 10 µs buckets up to 100 ms.
const LATENCY_BUCKETS = 10_000;
const CONNECTIONS = 32;
const LOAD_SECONDS = 10;
const ROUNDS = 5;

// The budget: deltas against the same binary without the agent.
const BUDGET = {
  startupMs: 2,
  rssMb: 25,
  idleCpuPercent: 0.5,
  throughputLossPercent: 3,
};

const SERVERS = {
  "Bun.serve": `const s = Bun.serve({ port: 0, fetch: () => new Response("hello") }); console.log(s.port);`,
  "node:http": `const s = require("node:http").createServer((q, r) => r.end("hello")).listen(0, "127.0.0.1", () => console.log(s.address().port));`,
};

type Setup = { name: string; env: Record<string, string> };

const median = (xs: number[]) => {
  const s = xs.slice().sort((a, b) => a - b);
  return s.length % 2 ? s[s.length >> 1] : (s[s.length / 2 - 1] + s[s.length / 2]) / 2;
};
const round = (x: number, digits = 1) => Math.round(x * 10 ** digits) / 10 ** digits;

const status = (pid: number, field: string) =>
  Number(new RegExp(`${field}:\\s+(\\d+)`).exec(readFileSync(`/proc/${pid}/status`, "utf8"))![1]);
// utime + stime of a process, in clock ticks (1/100 s).
const cpuTicks = (pid: number) => {
  const fields = readFileSync(`/proc/${pid}/stat`, "utf8").split(") ")[1].split(" ");
  return Number(fields[11]) + Number(fields[12]);
};

const pinning = spawnSync("taskset", ["--version"]).status === 0 && availableParallelism() >= 4;
const pinned = (cpus: string, cmd: string[]) => (pinning ? ["taskset", "-c", cpus, ...cmd] : cmd);

// A line from a child's stdout.
async function firstLine(stream: ReadableStream<Uint8Array>) {
  let text = "";
  for await (const chunk of stream) {
    text += new TextDecoder().decode(chunk);
    if (text.includes("\n")) break;
  }
  return text.split("\n")[0].trim();
}

// --- client mode: `connections` keep-alive HTTP/1.1 connections to `port`,
// each sending its next GET once the last response is in, for `seconds`.
// Plain sockets rather than fetch, so that the client is not the bottleneck.
// Prints the count and a histogram of the latencies in 10 µs buckets.
async function client(port: number, connections: number, seconds: number) {
  const request = Buffer.from("GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
  const histogram = new Uint32Array(LATENCY_BUCKETS);
  let count = 0;
  let errors = 0;
  const end = performance.now() + seconds * 1000;
  await Promise.all(
    Array.from(
      { length: connections },
      () =>
        new Promise<void>(done => {
          let buffered = "";
          let sentAt = 0;
          const send = (socket: Bun.Socket) => {
            if (performance.now() >= end) return socket.end();
            sentAt = performance.now();
            socket.write(request);
          };
          Bun.connect({
            hostname: "127.0.0.1",
            port,
            socket: {
              open: send,
              data(socket, chunk) {
                buffered += chunk.toString("latin1");
                for (;;) {
                  const head = buffered.indexOf("\r\n\r\n");
                  if (head === -1) return;
                  const length = /content-length: *(\d+)/i.exec(buffered.slice(0, head));
                  const size = length ? head + 4 + Number(length[1]) : buffered.indexOf("0\r\n\r\n", head) + 5;
                  if (size < head + 4 || buffered.length < size) return;
                  buffered = buffered.slice(size);
                  count++;
                  histogram[Math.min(LATENCY_BUCKETS - 1, Math.floor((performance.now() - sentAt) * 100))]++;
                  send(socket);
                }
              },
              close: () => done(),
              error: () => (errors++, done()),
            },
          }).catch(() => (errors++, done()));
        }),
    ),
  );
  console.log(JSON.stringify({ count, errors, histogram: Array.from(histogram) }));
}

// --- the measurement.
async function measure(bin: string) {
  const runner = process.execPath;
  const gw = Bun.spawn({ cmd: pinned("1", [runner, GATEWAY]), stdin: "pipe", stdout: "pipe", stderr: "inherit" });
  const gwLines = (async function* () {
    let buffered = "";
    for await (const chunk of gw.stdout) {
      buffered += new TextDecoder().decode(chunk);
      for (let i; (i = buffered.indexOf("\n")) !== -1; buffered = buffered.slice(i + 1)) yield buffered.slice(0, i);
    }
  })();
  const gatewayPort = Number((await gwLines.next()).value);
  const gatewayCounts = async () => {
    gw.stdin.write("stats\n");
    gw.stdin.flush();
    return JSON.parse((await gwLines.next()).value as string) as Record<string, { calls: number; bytes: number }>;
  };

  const base: Record<string, string> = { ...(process.env as Record<string, string>), BUN_DEBUG_QUIET_LOGS: "1" };
  for (const key of Object.keys(base)) if (key.startsWith("NSOLID_")) delete base[key];
  const agent = { ...base, NSOLID_GRPC: `127.0.0.1:${gatewayPort}`, NSOLID_GRPC_INSECURE: "1", NSOLID_APP: "overhead" };
  const setups: Setup[] = [
    { name: "no agent", env: base },
    { name: "agent", env: agent },
    { name: "tracing 100%", env: { ...agent, NSOLID_TRACING_ENABLED: "1", NSOLID_TRACE_SAMPLE_RATE: "1" } },
    { name: "tracing 10%", env: { ...agent, NSOLID_TRACING_ENABLED: "1", NSOLID_TRACE_SAMPLE_RATE: "0.1" } },
  ];

  const startServer = async (kind: keyof typeof SERVERS, env: Record<string, string>) => {
    const proc = Bun.spawn({ cmd: pinned("0", [bin, "-e", SERVERS[kind]]), env, stdout: "pipe", stderr: "inherit" });
    const port = Number(await firstLine(proc.stdout));
    return { proc, port, url: `http://127.0.0.1:${port}/` };
  };
  const stop = async (proc: Bun.Subprocess) => {
    proc.kill();
    await proc.exited;
  };
  // Two clients of half the connections each; the server's CPU use meanwhile
  // shows whether it, and not the clients, was the bottleneck.
  const load = async (server: Bun.Subprocess, port: number) => {
    const ticks = cpuTicks(server.pid);
    const outputs = await Promise.all(
      ["2", "3"].map(async cpu => {
        const proc = Bun.spawn({
          cmd: pinned(cpu, [runner, SELF, "--client", String(port), String(CONNECTIONS / 2), String(LOAD_SECONDS)]),
          stdout: "pipe",
          stderr: "inherit",
        });
        const text = await new Response(proc.stdout).text();
        await proc.exited;
        return JSON.parse(text) as { count: number; errors: number; histogram: number[] };
      }),
    );
    const serverCpuPercent = (cpuTicks(server.pid) - ticks) / LOAD_SECONDS;
    const count = outputs.reduce((sum, o) => sum + o.count, 0);
    const histogram = outputs[0].histogram.map((n, i) => n + outputs[1].histogram[i]);
    const quantile = (q: number) => {
      let seen = 0;
      for (let i = 0; i < histogram.length; i++) if ((seen += histogram[i]) >= count * q) return (i + 0.5) / 100;
      return LATENCY_BUCKETS / 100;
    };
    return {
      rps: count / LOAD_SECONDS,
      p50: quantile(0.5),
      p99: quantile(0.99),
      errors: outputs[0].errors + outputs[1].errors,
      serverCpuPercent,
    };
  };

  const results: Record<string, any> = {};
  for (const setup of setups) results[setup.name] = { startupMs: 0, rssMb: 0, threads: 0, idleCpuPercent: 0, http: {} };

  // Startup: the median wall time of `-e 0`, over 30 runs after 3 warm-ups.
  for (const setup of setups) {
    const times: number[] = [];
    for (let i = 0; i < 33; i++) {
      const t0 = performance.now();
      await Bun.spawn({ cmd: pinned("0", [bin, "-e", "0"]), env: setup.env, stdout: "ignore", stderr: "ignore" })
        .exited;
      if (i >= 3) times.push(performance.now() - t0);
    }
    results[setup.name].startupMs = median(times);
  }

  // Memory: the median RSS of 5 idle Bun.serve processes, 5 s after their
  // first request, the agent connected. Idle CPU: the first one's, over 30 s.
  for (const setup of setups) {
    const rss: number[] = [];
    for (let i = 0; i < 5; i++) {
      const { proc, url } = await startServer("Bun.serve", setup.env);
      await (await fetch(url)).text();
      await Bun.sleep(5000);
      rss.push(status(proc.pid, "VmRSS") / 1024);
      results[setup.name].threads = status(proc.pid, "Threads");
      if (i === 0) {
        const before = cpuTicks(proc.pid);
        await Bun.sleep(30_000);
        results[setup.name].idleCpuPercent = (cpuTicks(proc.pid) - before) / 30;
      }
      await stop(proc);
    }
    results[setup.name].rssMb = median(rss);
  }

  // Throughput and latency: ROUNDS rounds of LOAD_SECONDS, the setups taking
  // turns so that drift spreads over all of them. The median round counts,
  // with its peak RSS and what the gateway received from it.
  await gatewayCounts();
  for (const kind of Object.keys(SERVERS) as (keyof typeof SERVERS)[]) {
    const rounds: Record<string, any[]> = {};
    for (let r = 0; r < ROUNDS; r++) {
      for (const setup of setups) {
        const { proc, port } = await startServer(kind, setup.env);
        await Bun.sleep(3000);
        await gatewayCounts();
        const result: any = await load(proc, port);
        result.peakRssMb = status(proc.pid, "VmHWM") / 1024;
        await Bun.sleep(1500); // the agent's last span export
        result.gateway = await gatewayCounts();
        await stop(proc);
        (rounds[setup.name] ??= []).push(result);
      }
    }
    for (const setup of setups) {
      const list = rounds[setup.name].slice().sort((a, b) => a.rps - b.rps);
      const mid = list[ROUNDS >> 1];
      const traces = Object.entries(mid.gateway as Record<string, { calls: number; bytes: number }>).find(([path]) =>
        path.includes("TraceService"),
      )?.[1];
      results[setup.name].http[kind] = {
        rps: mid.rps,
        p50Ms: mid.p50,
        p99Ms: mid.p99,
        errors: mid.errors,
        serverCpuPercent: mid.serverCpuPercent,
        peakRssMb: mid.peakRssMb,
        traceExports: traces?.calls ?? 0,
        traceBytes: traces?.bytes ?? 0,
        rpsRounds: rounds[setup.name].map(x => Math.round(x.rps)),
      };
    }
  }

  gw.kill();
  return { binary: bin, pinned: pinning, cpus: availableParallelism(), results };
}

function report(measured: Awaited<ReturnType<typeof measure>>) {
  const { results } = measured;
  const none = results["no agent"];
  const lines: string[] = [];
  const row = (...cells: (string | number)[]) => lines.push(`| ${cells.join(" | ")} |`);
  const verdict = (ok: boolean) => (ok ? "within" : "**over**");

  lines.push(`Binary: \`${measured.binary}\`, ${measured.cpus} CPUs${measured.pinned ? ", pinned" : ""}.`, "");
  row("Setup", "Startup", "RSS idle", "Threads", "Idle CPU (% of a core)");
  row("---", "---:", "---:", "---:", "---:");
  for (const [name, r] of Object.entries(results) as [string, any][]) {
    const dStart = r.startupMs - none.startupMs;
    const dRss = r.rssMb - none.rssMb;
    row(
      name,
      `${round(r.startupMs, 2)} ms${name === "no agent" ? "" : ` (+${round(dStart, 2)}, ${verdict(dStart <= BUDGET.startupMs)})`}`,
      `${round(r.rssMb)} MB${name === "no agent" ? "" : ` (+${round(dRss)}, ${verdict(dRss <= BUDGET.rssMb)})`}`,
      r.threads,
      `${round(r.idleCpuPercent, 2)}${name === "no agent" ? "" : ` (${verdict(r.idleCpuPercent <= BUDGET.idleCpuPercent)})`}`,
    );
  }
  for (const kind of Object.keys(SERVERS)) {
    lines.push(
      "",
      `${kind}, hello world, ${CONNECTIONS} connections, the median of ${ROUNDS} rounds of ${LOAD_SECONDS} s:`,
      "",
    );
    row("Setup", "Requests/s", "Rounds", "Change", "p50", "p99", "Server CPU", "Peak RSS", "Trace exports (bytes)");
    row("---", "---:", "---:", "---:", "---:", "---:", "---:", "---:", "---:");
    for (const [name, r] of Object.entries(results) as [string, any][]) {
      const h = r.http[kind];
      const change = (h.rps / none.http[kind].rps - 1) * 100;
      row(
        name,
        Math.round(h.rps),
        `${Math.min(...h.rpsRounds)}–${Math.max(...h.rpsRounds)}`,
        name === "no agent"
          ? ""
          : `${change >= 0 ? "+" : ""}${round(change)}%${kind === "Bun.serve" ? ` (${verdict(-change <= BUDGET.throughputLossPercent)})` : ""}`,
        `${round(h.p50Ms, 2)} ms`,
        `${round(h.p99Ms, 2)} ms`,
        `${Math.round(h.serverCpuPercent)}%`,
        `${round(h.peakRssMb)} MB`,
        h.traceExports ? `${h.traceExports} (${round(h.traceBytes / 1e6)} MB)` : "",
      );
    }
  }
  lines.push(
    "",
    `Budget: startup +${BUDGET.startupMs} ms, RSS +${BUDGET.rssMb} MB, idle CPU ${BUDGET.idleCpuPercent}% of a core, Bun.serve throughput ${BUDGET.throughputLossPercent}% lower at most.`,
  );
  return lines.join("\n");
}

const { values, positionals } = parseArgs({
  args: process.argv.slice(2),
  allowPositionals: true,
  options: {
    client: { type: "boolean" },
    json: { type: "string" },
    markdown: { type: "string" },
  },
});

if (values.client) await client(Number(positionals[0]), Number(positionals[1]), Number(positionals[2]));
else {
  const bin = positionals[0];
  if (!bin) throw new Error("usage: bun scripts/nsolid/measure-overhead.ts <nsolid-bun binary> [--json <file>]");
  const measured = await measure(bin);
  const text = report(measured);
  console.log(text);
  if (values.json) writeFileSync(values.json, JSON.stringify(measured, null, 2));
  if (values.markdown) appendFileSync(values.markdown, `${text}\n`);
}
