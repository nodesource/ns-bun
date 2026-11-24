import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { dlopen } from "bun:ffi";
import { describe, expect, test } from "bun:test";
import { bunEnv, bunExe, isGlibc, tempDir, tls } from "harness";
import { existsSync, readdirSync, readFileSync, realpathSync, symlinkSync, writeFileSync } from "node:fs";
import http2 from "node:http2";
import type { AddressInfo } from "node:net";
import { join } from "node:path";

const COMMAND_PATH = "/grpcagent.NSolidService/Command";
// Keeps the child alive for the test, and lets it exit if the test runner dies first.
const UNTIL_STDIN_CLOSES = "for await (const _ of console) {}";

// Sends a signal to one thread of a process.
const tgkill = isGlibc
  ? dlopen("libc.so.6", { tgkill: { args: ["i32", "i32", "i32"], returns: "i32" } }).symbols.tgkill
  : undefined;

// Stands in for the Console's gateway: answers the first Command stream it is
// offered and hands it to the test, and accepts every other call at once.
async function fakeGateway() {
  const server = http2.createServer();
  const { promise: firstStream, resolve } = Promise.withResolvers<{
    headers: http2.IncomingHttpHeaders;
    stream: http2.ServerHttp2Stream;
  }>();
  // Each test ends by killing the agent's process, which resets its connection.
  server.on("session", session => session.on("error", () => {}));
  server.on("stream", (stream, headers) => {
    stream.on("error", () => {});
    if (headers[":path"] !== COMMAND_PATH) {
      stream.respond({ ":status": 200, "content-type": "application/grpc", "grpc-status": "0" }, { endStream: true });
      return;
    }
    stream.respond({ ":status": 200, "content-type": "application/grpc" });
    resolve({ headers, stream });
  });
  await new Promise<void>(done => server.listen(0, "127.0.0.1", done));
  return {
    port: (server.address() as AddressInfo).port,
    firstStream,
    [Symbol.asyncDispose]: async () => {
      server.close();
    },
  };
}

// A real gRPC server built from the agent's protos, standing in for the
// Console's gateway: it sends `commands` on the Command stream and records
// what the agent sends back on each RPC.
const protoDir = join(import.meta.dir, "..", "..", "..", "..", "packages", "nsolid-protocol", "proto");
const { grpcagent, opentelemetry } = grpc.loadPackageDefinition(
  protoLoader.loadSync(
    [
      "nsolid_service.proto",
      "opentelemetry/proto/collector/metrics/v1/metrics_service.proto",
      "opentelemetry/proto/collector/trace/v1/trace_service.proto",
      "opentelemetry/proto/collector/logs/v1/logs_service.proto",
    ],
    {
      includeDirs: [protoDir],
      keepCase: true,
      longs: String,
      enums: Number,
    },
  ),
) as any;

async function grpcGateway(commands: object[]) {
  const received: Record<string, any[]> = {};
  const waiting: Record<string, ((value: any) => void)[]> = {};
  const record = (rpc: string, value: any) => {
    const resolve = waiting[rpc]?.shift();
    if (resolve) resolve(value);
    else (received[rpc] ??= []).push(value);
  };
  let acks = 0;
  const { promise: commandMetadata, resolve: gotCommandStream } = Promise.withResolvers<grpc.Metadata>();
  let commandCall: any;
  const unary = (rpc: string) => (call: any, callback: any) => {
    record(rpc, call.request);
    callback(null, {});
  };
  const clientStream = (rpc: string) => (call: any, callback: any) => {
    const messages: any[] = [];
    call.on("data", (m: any) => messages.push(m));
    call.on("end", () => {
      record(rpc, messages);
      callback(null, {});
    });
  };

  const server = new grpc.Server();
  server.addService(grpcagent.NSolidService.service, {
    Command(call: any) {
      gotCommandStream(call.metadata);
      commandCall = call;
      call.on("data", () => acks++);
      call.on("error", () => {});
      for (const command of commands) call.write(command);
    },
    ExportAsset: clientStream("ExportAsset"),
    ExportBlockedLoop: unary("ExportBlockedLoop"),
    ExportContinuousProfile: clientStream("ExportContinuousProfile"),
    ExportExit: unary("ExportExit"),
    ExportInfo: unary("ExportInfo"),
    ExportMetrics: unary("ExportMetrics"),
    ExportPackages: unary("ExportPackages"),
    ExportReconfigure: unary("ExportReconfigure"),
    ExportSourceCode: unary("ExportSourceCode"),
    ExportStartupTimes: unary("ExportStartupTimes"),
    ExportUnblockedLoop: unary("ExportUnblockedLoop"),
  });
  server.addService(opentelemetry.proto.collector.metrics.v1.MetricsService.service, {
    Export: unary("MetricsExport"),
  });
  server.addService(opentelemetry.proto.collector.trace.v1.TraceService.service, { Export: unary("TraceExport") });
  server.addService(opentelemetry.proto.collector.logs.v1.LogsService.service, { Export: unary("LogsExport") });
  const port = await new Promise<number>((resolve, reject) =>
    server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (err, port) =>
      err ? reject(err) : resolve(port),
    ),
  );
  return {
    port,
    commandMetadata,
    acks: () => acks,
    // Sends one more command on the Command stream.
    async command(command: object) {
      await commandMetadata;
      commandCall.write(command);
    },
    next(rpc: string): Promise<any> {
      if (received[rpc]?.length) return Promise.resolve(received[rpc].shift());
      return new Promise(resolve => (waiting[rpc] ??= []).push(resolve));
    },
    [Symbol.asyncDispose]: async () => {
      server.forceShutdown();
    },
  };
}

// The gateway serves harness's self-signed localhost certificate, which the
// agent trusts as its CA. Client certificates are Node's fixtures: agent1 is
// signed by ca1 and agent3 by ca2.
const nodeKeys = join(import.meta.dir, "..", "..", "node", "test", "fixtures", "keys");
const nodeKey = (name: string) => readFileSync(join(nodeKeys, name), "utf8");

interface CommandStream {
  headers: http2.IncomingHttpHeaders;
  stream: http2.ServerHttp2Stream;
  at: number;
  // The CN of the client certificate the agent presented.
  peer: string | undefined;
}

// A gateway, over TLS when given `secure` options, that records every
// Command stream and lets `answer` respond to it, and accepts every other
// call at once.
async function recordingGateway(
  answer: (command: CommandStream, index: number) => void,
  secure?: http2.SecureServerOptions,
) {
  const server = secure ? http2.createSecureServer(secure) : http2.createServer();
  const seen: CommandStream[] = [];
  let changed = () => {};
  server.on("session", session => session.on("error", () => {}));
  server.on("stream", (stream, headers) => {
    stream.on("error", () => {});
    if (headers[":path"] !== COMMAND_PATH) {
      stream.respond({ ":status": 200, "content-type": "application/grpc", "grpc-status": "0" }, { endStream: true });
      return;
    }
    const peer = secure ? (stream.session?.socket as any)?.getPeerCertificate?.()?.subject?.CN : undefined;
    const command = { headers, stream, at: performance.now(), peer };
    seen.push(command);
    answer(command, seen.length - 1);
    changed();
  });
  await new Promise<void>(done => server.listen(0, "127.0.0.1", done));
  return {
    port: (server.address() as AddressInfo).port,
    seen,
    async until(condition: (seen: CommandStream[]) => boolean) {
      while (!condition(seen)) await new Promise<void>(resolve => (changed = resolve));
    },
    [Symbol.asyncDispose]: async () => {
      server.close();
    },
  };
}

const keepOpen = ({ stream }: CommandStream) => stream.respond({ ":status": 200, "content-type": "application/grpc" });

// Ends a Command stream at once with a gRPC status (a trailers-only response).
const endWith =
  (grpcStatus: number, extra: http2.OutgoingHttpHeaders = {}) =>
  ({ stream }: CommandStream) =>
    stream.respond(
      { ":status": 200, "content-type": "application/grpc", "grpc-status": String(grpcStatus), ...extra },
      { endStream: true },
    );

async function readUntil(stream: ReadableStream<Uint8Array>, done: (text: string) => boolean) {
  const decoder = new TextDecoder();
  let text = "";
  for await (const chunk of stream) {
    text += decoder.decode(chunk, { stream: true });
    if (done(text)) break;
  }
  return text;
}

// The lines a child writes, one at a time.
function lineReader(stream: ReadableStream<Uint8Array>) {
  const lines = (async function* () {
    const decoder = new TextDecoder();
    let buffered = "";
    for await (const chunk of stream) {
      buffered += decoder.decode(chunk, { stream: true });
      for (let end; (end = buffered.indexOf("\n")) !== -1; buffered = buffered.slice(end + 1)) {
        yield buffered.slice(0, end);
      }
    }
  })();
  return async () => (await lines.next()).value as string;
}

function frame(payload: number[]) {
  const out = Buffer.alloc(5 + payload.length);
  out.writeUInt32BE(payload.length, 1);
  Buffer.from(payload).copy(out, 5);
  return out;
}

describe("N|Solid agent", () => {
  test.concurrent("opens the Command stream with the agent's metadata when NSOLID_GRPC is set", async () => {
    await using gateway = await fakeGateway();
    await using proc = Bun.spawn({
      cmd: [bunExe(), "-e", UNTIL_STDIN_CLOSES],
      stdin: "pipe",
      env: { ...bunEnv, NSOLID_GRPC: String(gateway.port), NSOLID_GRPC_INSECURE: "1" },
      stdout: "ignore",
      stderr: "inherit",
    });

    const { headers } = await gateway.firstStream;
    expect({
      method: headers[":method"],
      path: headers[":path"],
      contentType: headers["content-type"],
      te: headers["te"],
      saas: headers["nsolid-saas"],
    }).toEqual({
      method: "POST",
      path: COMMAND_PATH,
      contentType: "application/grpc",
      te: "trailers",
      saas: undefined,
    });
    expect(headers["nsolid-agent-id"]).toMatch(/^[0-9a-f]{40}$/);
    expect(headers["nsolid-version"]).toBe(`nsolid-bun-v${process.versions.nsolid_bun}+bun-v${Bun.version}`);
  });

  test.concurrent("acknowledges every command with an empty message", async () => {
    await using gateway = await fakeGateway();
    await using proc = Bun.spawn({
      cmd: [bunExe(), "-e", UNTIL_STDIN_CLOSES],
      stdin: "pipe",
      env: { ...bunEnv, NSOLID_GRPC: `127.0.0.1:${gateway.port}`, NSOLID_GRPC_INSECURE: "1" },
      stdout: "ignore",
      stderr: "inherit",
    });

    const { stream } = await gateway.firstStream;
    // Two CommandRequests, the second split across writes.
    const second = frame([0x0a, 0x04, 0x69, 0x6e, 0x66, 0x6f]);
    stream.write(frame([0x0a, 0x01, 0x41]));
    stream.write(second.subarray(0, 3));
    stream.write(second.subarray(3));

    let received = Buffer.alloc(0);
    for await (const chunk of stream) {
      received = Buffer.concat([received, chunk]);
      if (received.length >= 10) break;
    }
    expect([...received]).toEqual([0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
  });

  test.concurrent("dials the endpoint in a testing SaaS token and sends the token", async () => {
    await using gateway = await fakeGateway();
    const token =
      Buffer.alloc(40, "k").toString() + Buffer.alloc(36, "c").toString() + `.testing.localhost:${gateway.port}`;
    await using proc = Bun.spawn({
      cmd: [bunExe(), "-e", UNTIL_STDIN_CLOSES],
      stdin: "pipe",
      env: { ...bunEnv, NSOLID_SAAS: token, NSOLID_GRPC_INSECURE: "1" },
      stdout: "ignore",
      stderr: "inherit",
    });

    const { headers } = await gateway.firstStream;
    expect(headers[":path"]).toBe(COMMAND_PATH);
    expect(headers["nsolid-saas"]).toBe(token);
  });

  test.concurrent("does not keep the process alive", async () => {
    await using gateway = await fakeGateway();
    await using proc = Bun.spawn({
      cmd: [bunExe(), "-e", `for await (const line of console) { console.log("done"); break; }`],
      env: { ...bunEnv, NSOLID_GRPC: `localhost:${gateway.port}`, NSOLID_GRPC_INSECURE: "1" },
      stdin: "pipe",
      stdout: "pipe",
      stderr: "inherit",
    });

    // The agent's stream is open while the script finishes.
    await gateway.firstStream;
    proc.stdin.write("go\n");
    await proc.stdin.end();

    const [stdout, exitCode] = await Promise.all([proc.stdout.text(), proc.exited]);
    expect(stdout).toBe("done\n");
    expect(exitCode).toBe(0);
  });

  test.concurrent("answers info, startup_times and reconfigure, and 404 for the rest", async () => {
    await using gateway = await grpcGateway([
      { requestId: "r-info", id: "x", command: "info" },
      { requestId: "r-startup", id: "x", command: "startup_times" },
      {
        requestId: "r-reconfigure",
        id: "x",
        command: "reconfigure",
        args: {
          reconfigure: {
            interval: 3000,
            tags: ["xy"],
            redactSnapshots: true,
            ebpfProfiling: 1,
            sourceCodeEnabled: true,
          },
        },
      },
      {
        requestId: "r-profile",
        id: "x",
        command: "heap_profile",
        args: { profile: { thread_id: 0, duration: 1000, heap_profile: {} } },
      },
      { requestId: "r-packages", id: "x", command: "packages" },
      { requestId: "r-metrics", id: "x", command: "metrics" },
      {
        requestId: "r-source",
        id: "x",
        command: "source_code",
        args: { source_code: { thread_id: 0, script_id: 1, path: "/nowhere.js" } },
      },
    ]);
    using dir = tempDir("nsolid-agent", {
      "package.json": JSON.stringify({ name: "from-package-json", version: "1.2.3" }),
      "app.js": `console.log(require("nsolid").id); for await (const _ of console) {}`,
    });
    await using proc = Bun.spawn({
      cmd: [bunExe(), join(String(dir), "app.js")],
      cwd: String(dir),
      stdin: "pipe",
      stdout: "pipe",
      stderr: "inherit",
      env: {
        ...bunEnv,
        NSOLID_GRPC: `127.0.0.1:${gateway.port}`,
        NSOLID_GRPC_INSECURE: "1",
        NSOLID_APP: "shop",
        NSOLID_TAGS: "bb, aa",
        NSOLID_HOSTNAME: "host-1",
        NODE_ENV: "staging",
      },
    });
    const reader = proc.stdout.getReader();
    const idFromModule = new TextDecoder().decode((await reader.read()).value).trim();

    const info = await gateway.next("ExportInfo");
    expect({ ...info.common, recorded: undefined }).toEqual({
      requestId: "r-info",
      command: "info",
      recorded: undefined,
    });
    expect(Number(info.common.recorded.seconds)).toBeGreaterThan(0);
    const {
      versions,
      cpuCores,
      cpuModel,
      totalMem,
      processStart,
      execPath,
      kernelVersion,
      runtime,
      capabilities,
      ...body
    } = info.body;
    expect({ runtime, capabilities }).toEqual({
      runtime: { name: "bun", version: Bun.version, engine: "jsc", agent_version: versions.nsolid_bun },
      capabilities: {
        commands: ["info", "metrics", "packages", "profile", "reconfigure", "snapshot", "source_code", "startup_times"],
        settings: [
          "assetsEnabled",
          "blockedLoopThreshold",
          "contCpuProfile",
          "interval",
          "pauseMetrics",
          "redactSnapshots",
          "sourceCodeEnabled",
          "tags",
          "traceSampleRate",
          "tracingEnabled",
          "tracingModulesBlacklist",
        ],
      },
    });
    expect(body).toEqual({
      app: "shop",
      appVersion: "1.2.3",
      arch: process.arch,
      hostname: "host-1",
      id: idFromModule,
      main: join(String(dir), "app.js"),
      nodeEnv: "staging",
      pid: proc.pid,
      platform: process.platform,
      tags: ["aa", "bb"],
    });
    expect(idFromModule).toMatch(/^[0-9a-f]{40}$/);
    expect((await gateway.commandMetadata).get("nsolid-agent-id")).toEqual([idFromModule]);
    expect(versions.bun).toBe(Bun.version);
    expect(versions.node).toBeUndefined();
    expect(cpuCores).toBeGreaterThan(0);
    expect(Number(totalMem)).toBeGreaterThan(0);
    expect(Math.abs(Number(processStart) - Date.now())).toBeLessThan(60_000);

    const startup = await gateway.next("ExportStartupTimes");
    expect(startup.common.requestId).toBe("r-startup");
    const marks = Object.fromEntries(Object.entries(startup.body).map(([k, v]) => [k, Number(v)]));
    expect(Object.keys(marks).sort()).toEqual([
      "main_module_start",
      "nsolid_agent_ready",
      "nsolid_agent_thread_start",
      "timeOrigin",
    ]);
    expect(marks.timeOrigin).toBe(0);
    expect(marks.main_module_start).toBeGreaterThan(0);
    expect(marks.nsolid_agent_thread_start).toBeGreaterThanOrEqual(marks.main_module_start);
    expect(marks.nsolid_agent_ready).toBeGreaterThanOrEqual(marks.nsolid_agent_thread_start);

    const reconfigure = await gateway.next("ExportReconfigure");
    expect(reconfigure.common.requestId).toBe("r-reconfigure");
    expect(reconfigure.body).toEqual({
      assetsEnabled: true,
      blockedLoopThreshold: "200",
      contCpuProfile: false,
      ebpfProfiling: 2,
      interval: "3000",
      pauseMetrics: false,
      promiseTracking: false,
      redactSnapshots: true,
      sourceCodeEnabled: true,
      statsdBucket: "nsolid.${env}.${app}.${hostname}.${shortId}",
      tags: ["xy"],
      traceSampleRate: 1,
      tracingEnabled: false,
      tracingModulesBlacklist: 0,
    });

    const notAvailable = { code: 404, message: "Resource not available(1006)" };
    const [asset, packages, metrics, source] = await Promise.all([
      gateway.next("ExportAsset"),
      gateway.next("ExportPackages"),
      gateway.next("ExportMetrics"),
      gateway.next("ExportSourceCode"),
    ]);
    expect(asset.map((m: any) => [m.common.requestId, m.common.error])).toEqual([["r-profile", notAvailable]]);
    // Source reads were turned on by the reconfigure, and no script has that id and path.
    expect([source].map(m => [m.common.requestId, m.common.error])).toEqual([
      ["r-source", { code: 500, message: "Internal Runtime Error(1007)" }],
    ]);
    expect([packages.common.requestId, packages.common.error]).toEqual(["r-packages", undefined]);
    expect({
      requestId: metrics.common.requestId,
      error: metrics.common.error,
      resources: metrics.body.resource_metrics.length,
    }).toEqual({ requestId: "r-metrics", error: undefined, resources: 1 });
    expect(gateway.acks()).toBe(7);
  });

  test.concurrent("exports the process's metrics and each thread's heap over OTLP every interval", async () => {
    await using gateway = await grpcGateway([]);
    await using proc = Bun.spawn({
      cmd: [
        bunExe(),
        "-e",
        `const { Worker } = require("node:worker_threads");
         const worker = new Worker("setInterval(() => {}, 1000)", { eval: true, name: "w1" });
         worker.on("online", () => console.log("online " + worker.threadId));
         worker.on("exit", () => console.log("exited"));
         for await (const line of console) if (line === "stop") worker.terminate();
         worker.terminate();`,
      ],
      stdin: "pipe",
      stdout: "pipe",
      stderr: "inherit",
      env: {
        ...bunEnv,
        NSOLID_GRPC: `127.0.0.1:${gateway.port}`,
        NSOLID_GRPC_INSECURE: "1",
        NSOLID_APP: "metrics-app",
        NSOLID_INTERVAL: "100",
      },
    });
    const lines = (async function* () {
      const decoder = new TextDecoder();
      let buffered = "";
      for await (const chunk of proc.stdout) {
        buffered += decoder.decode(chunk, { stream: true });
        for (let end; (end = buffered.indexOf("\n")) !== -1; buffered = buffered.slice(end + 1)) {
          yield buffered.slice(0, end);
        }
      }
    })();
    const metricOf = (request: any, name: string) =>
      request.resource_metrics[0].scope_metrics[0].metrics.find((m: any) => m.name === name);
    const heapThreads = (request: any) =>
      (metricOf(request, "heapUsed")?.gauge.data_points ?? []).map((p: any) =>
        Object.fromEntries(p.attributes.map((a: any) => [a.key, a.value.int_value ?? a.value.string_value])),
      );
    const exportWhere = async (condition: (request: any) => boolean) => {
      for (;;) {
        const request = await gateway.next("MetricsExport");
        if (condition(request)) return request;
      }
    };

    // The agent's own VM does not take a thread id from the application's workers.
    expect((await lines.next()).value).toBe("online 1");
    const request = await exportWhere(r => heapThreads(r).some((t: any) => t["thread.id"] === "1"));
    const [resourceMetrics] = request.resource_metrics;
    const resource = Object.fromEntries(
      resourceMetrics.resource.attributes.map((a: any) => [a.key, a.value.int_value ?? a.value.string_value]),
    );
    expect({
      app: resource["service.name"],
      agentId: resource["service.instance.id"],
      pid: resource["process.pid"],
      runtime: resource["process.runtime.name"],
      scope: resourceMetrics.scope_metrics[0].scope.name,
    }).toEqual({
      app: "metrics-app",
      agentId: (await gateway.commandMetadata).get("nsolid-agent-id")[0],
      pid: String(proc.pid),
      runtime: "bun",
      scope: "nsolid",
    });

    const metrics = resourceMetrics.scope_metrics[0].metrics;
    expect(metrics.map((m: any) => m.name).sort()).toEqual(
      [
        "activeHandles",
        "activeRequests",
        "tcpServerCreatedCount",
        "tcpServerDestroyedCount",
        "tcpSocketCreatedCount",
        "tcpSocketDestroyedCount",
        "udpSocketCreatedCount",
        "udpSocketDestroyedCount",
        "pipeServerCreatedCount",
        "pipeServerDestroyedCount",
        "pipeSocketCreatedCount",
        "pipeSocketDestroyedCount",
        "fsHandlesOpenedCount",
        "fsHandlesClosedCount",
        "blockInputOpCount",
        "blockOutputOpCount",
        "cpuPercent",
        "cpuSystemPercent",
        "cpuUserPercent",
        "ctxSwitchInvoluntaryCount",
        "ctxSwitchVoluntaryCount",
        "dns",
        "dnsCount",
        "eventsProcessed",
        "eventsWaiting",
        "externalMem",
        "freeMem",
        "gcCount",
        "gcDurUs",
        "gcForcedCount",
        "gcFullCount",
        "gcMajorCount",
        "heapTotal",
        "heapUsed",
        "httpClient",
        "httpClientAbortCount",
        "httpClientCount",
        "httpServer",
        "httpServerAbortCount",
        "httpServerCount",
        "ipcReceivedCount",
        "ipcSentCount",
        "load15m",
        "load1m",
        "load5m",
        "loopAvgTasks",
        "loopEstimatedLag",
        "loopIdlePercent",
        "loopIdleTime",
        "loopIterWithEvents",
        "loopIterations",
        "loopTotalCount",
        "loopUtilization",
        "pageFaultHardCount",
        "pageFaultSoftCount",
        "processingDelay",
        "providerDelay",
        "res15m",
        "res1m",
        "res5m",
        "res5s",
        "rss",
        "signalCount",
        "swapCount",
        "systemUptime",
        "totalMem",
        "uptime",
      ].sort(),
    );
    const uptime = metricOf(request, "uptime");
    expect({ temporality: uptime.sum.aggregation_temporality, monotonic: uptime.sum.is_monotonic }).toEqual({
      temporality: 2,
      monotonic: true,
    });
    expect(Number(metricOf(request, "rss").gauge.data_points[0].as_int)).toBeGreaterThan(0);
    expect(metricOf(request, "cpuPercent").gauge.data_points[0].as_double).toBeGreaterThanOrEqual(0);
    expect(heapThreads(request).sort((a: any, b: any) => Number(a["thread.id"]) - Number(b["thread.id"]))).toEqual([
      { "thread.id": "0", "thread.name": "" },
      { "thread.id": "1", "thread.name": "" },
    ]);
    for (const p of metricOf(request, "heapUsed").gauge.data_points) expect(Number(p.as_int)).toBeGreaterThan(0);

    proc.stdin.write("stop\n");
    expect((await lines.next()).value).toBe("exited");
    await exportWhere(r => heapThreads(r).every((t: any) => t["thread.id"] !== "1"));
  });

  test.concurrent("exports each thread's event-loop and GC metrics, as N|Solid defines them", async () => {
    await using gateway = await grpcGateway([]);
    await using proc = Bun.spawn({
      cmd: [
        bunExe(),
        "-e",
        `for await (const line of console) {
           if (line === "gc") Bun.gc(true);
           if (line === "block") { const end = Date.now() + 500; while (Date.now() < end) {} }
           console.log(line);
         }`,
      ],
      stdin: "pipe",
      stdout: "pipe",
      stderr: "inherit",
      env: { ...bunEnv, NSOLID_GRPC: `127.0.0.1:${gateway.port}`, NSOLID_GRPC_INSECURE: "1", NSOLID_INTERVAL: "100" },
    });
    const nextLine = lineReader(proc.stdout);
    const mainThread = (request: any) => {
      const values: Record<string, any> = {};
      for (const metric of request.resource_metrics[0].scope_metrics[0].metrics) {
        const point = (metric.gauge ?? metric.sum ?? metric.summary)?.data_points.find((p: any) =>
          p.attributes?.some((a: any) => a.key === "thread.id" && a.value.int_value === "0"),
        );
        if (!point) continue;
        values[metric.name] = metric.summary
          ? Object.fromEntries(point.quantile_values.map((q: any) => [q.quantile, q.value]))
          : (point.as_double ?? Number(point.as_int));
        if (metric.sum) values[metric.name + "Kind"] = metric.sum.is_monotonic ? "counter" : "sum";
      }
      return values;
    };
    const exportWhere = async (condition: (thread: Record<string, any>) => boolean) => {
      for (;;) {
        const thread = mainThread(await gateway.next("MetricsExport"));
        if (condition(thread)) return thread;
      }
    };

    const first = await exportWhere(thread => thread.loopIterations > 0);
    expect({
      iterationsKind: first.loopIterationsKind,
      gcCountKind: first.gcCountKind,
      utilization: first.loopUtilization >= 0 && first.loopUtilization <= 1,
      idlePercent: Math.abs(first.loopIdlePercent - (1 - first.loopUtilization) * 100) < 1e-6,
      totalCount: first.loopTotalCount === first.loopIterations,
      idleTime: first.loopIdleTime > 0,
      quantiles: Object.keys(first.gcDurUs).sort(),
    }).toEqual({
      iterationsKind: "counter",
      gcCountKind: "counter",
      utilization: true,
      idlePercent: true,
      totalCount: true,
      idleTime: true,
      quantiles: ["0.5", "0.99"],
    });

    proc.stdin.write("gc\n");
    expect(await nextLine()).toBe("gc");
    const collected = await exportWhere(thread => thread.gcForcedCount >= 1);
    expect({
      forced: collected.gcForcedCount,
      full: collected.gcFullCount,
      counted: collected.gcCount >= 1 && collected.gcMajorCount >= 1,
      timed: collected.gcDurUs["0.5"] > 0 && collected.gcDurUs["0.99"] >= collected.gcDurUs["0.5"],
    }).toEqual({ forced: 1, full: 1, counted: true, timed: true });

    // A loop stuck in a task shows as busy, and lagging by the time since it
    // last polled.
    proc.stdin.write("block\n");
    const blocked = await exportWhere(thread => thread.loopEstimatedLag >= 100);
    expect(blocked.loopUtilization).toBeGreaterThan(0.9);
    expect(await nextLine()).toBe("block");
  });

  // An asset's messages: the request, thread and metadata of the first, the
  // JSON from the data, and the duration of the last, which completes it.
  const assetOf = (messages: any[]) => {
    const data = Buffer.concat(messages.filter(m => m.data?.length).map(m => m.data));
    const last = messages.at(-1);
    return {
      requestId: messages[0].common.requestId,
      command: messages[0].common.command,
      threadId: Number(messages[0].thread_id),
      error: messages[0].common.error,
      complete: !!last.complete,
      duration: Number(last.duration),
      chunkSizes: messages.filter(m => m.data?.length).map(m => m.data.length),
      text: data.toString(),
      profile: data.length ? JSON.parse(data.toString()) : undefined,
    };
  };
  const spawnAgent = (
    gateway: { port: number },
    script: string,
    extraEnv: Record<string, string> = {},
    args: string[] = [],
  ) =>
    Bun.spawn({
      cmd: [bunExe(), ...args, "-e", script],
      stdin: "pipe",
      stdout: "pipe",
      stderr: "inherit",
      env: { ...bunEnv, NSOLID_GRPC: `127.0.0.1:${gateway.port}`, NSOLID_GRPC_INSECURE: "1", ...extraEnv },
    });

  describe("takes CPU profiles", () => {
    const profileCommand = (requestId: string, threadId: number, duration: number) => ({
      requestId,
      id: "x",
      command: "profile",
      args: { profile: { thread_id: threadId, duration, cpu_profile: {} } },
    });
    const functionsOf = (profile: any) => new Set(profile.nodes.map((n: any) => n.callFrame.functionName));
    const hitsOf = (profile: any, name: string) =>
      profile.samples.filter(
        (id: number) => profile.nodes.find((n: any) => n.id === id).callFrame.functionName === name,
      ).length;

    test.concurrent("of the main thread and of a worker, with the nodes V8's profiles have", async () => {
      await using gateway = await grpcGateway([]);
      await using proc = spawnAgent(
        gateway,
        `const { Worker } = require("node:worker_threads");
         const worker = new Worker(
           "function workerSpin() { const end = Date.now() + 50; while (Date.now() < end) {} }" +
           "setInterval(workerSpin, 60);",
           { eval: true },
         );
         worker.on("online", () => console.log("online"));
         for await (const line of console) {}
         worker.terminate();`,
      );
      const nextLine = lineReader(proc.stdout);
      expect(await nextLine()).toBe("online");
      await gateway.command(profileCommand("r-main", 0, 600));
      const main = assetOf(await gateway.next("ExportAsset"));
      await gateway.command(profileCommand("r-worker", 1, 600));
      const worker = assetOf(await gateway.next("ExportAsset"));

      expect({ ...main, profile: undefined, duration: undefined, chunkSizes: undefined, text: undefined }).toEqual({
        requestId: "r-main",
        command: "profile",
        threadId: 0,
        error: undefined,
        complete: true,
        duration: undefined,
        chunkSizes: undefined,
        text: undefined,
        profile: undefined,
      });
      expect(main.duration).toBeGreaterThanOrEqual(600);
      // The main thread waits on stdin: nearly all of it is idle, sampled once
      // a millisecond over the whole profile.
      const { profile } = main;
      const spanUs = profile.endTime - profile.startTime;
      const sumUs = profile.timeDeltas.reduce((a: number, b: number) => a + b, 0);
      expect(spanUs).toBeGreaterThanOrEqual(550_000);
      expect(Math.abs(sumUs - spanUs)).toBeLessThan(5_000);
      expect(profile.samples.length).toBeGreaterThan(spanUs / 1000 / 2);
      expect(hitsOf(profile, "(idle)") / profile.samples.length).toBeGreaterThan(0.8);
      expect(profile.nodes[0].callFrame.functionName).toBe("(root)");

      expect({ requestId: worker.requestId, threadId: worker.threadId, complete: worker.complete }).toEqual({
        requestId: "r-worker",
        threadId: 1,
        complete: true,
      });
      expect(functionsOf(worker.profile).has("workerSpin")).toBe(true);
      expect(hitsOf(worker.profile, "workerSpin")).toBeGreaterThan(50);
    });

    test.concurrent("of a thread that is busy in JS", async () => {
      await using gateway = await grpcGateway([]);
      await using proc = spawnAgent(
        gateway,
        `function spin(ms) { const end = Date.now() + ms; while (Date.now() < end) {} }
         for await (const line of console) { console.log("spinning"); spin(4000); console.log("done"); }`,
      );
      const nextLine = lineReader(proc.stdout);
      await gateway.commandMetadata;
      proc.stdin.write("go\n");
      expect(await nextLine()).toBe("spinning");
      await gateway.command(profileCommand("r-busy", 0, 500));
      // The profile starts and stops while the thread is still spinning.
      const busy = assetOf(await gateway.next("ExportAsset"));
      const spinHits = hitsOf(busy.profile, "spin");
      expect({ requestId: busy.requestId, complete: busy.complete, spun: spinHits > 100 }).toEqual({
        requestId: "r-busy",
        complete: true,
        spun: true,
      });
      expect(busy.duration).toBeLessThan(3000);
      expect(await nextLine()).toBe("done");
    });

    test.concurrent("with N|Solid's errors: in progress, thread gone, assets disabled", async () => {
      await using gateway = await grpcGateway([]);
      await using proc = spawnAgent(gateway, UNTIL_STDIN_CLOSES);
      await gateway.command(profileCommand("r-first", 0, 1500));
      await gateway.command(profileCommand("r-again", 0, 100));
      await gateway.command(profileCommand("r-nobody", 7, 100));
      const errors = [assetOf(await gateway.next("ExportAsset")), assetOf(await gateway.next("ExportAsset"))];
      errors.sort((a, b) => a.requestId.localeCompare(b.requestId));
      expect(errors.map(e => [e.requestId, e.threadId, e.error])).toEqual([
        ["r-again", 0, { code: 409, message: "Operation already in progress(1001)" }],
        ["r-nobody", 7, { code: 410, message: "Thread already gone(1002)" }],
      ]);
      expect(assetOf(await gateway.next("ExportAsset")).requestId).toBe("r-first");

      await gateway.command({
        requestId: "r-off",
        id: "x",
        command: "reconfigure",
        args: { reconfigure: { assetsEnabled: false } },
      });
      await gateway.next("ExportReconfigure");
      await gateway.command(profileCommand("r-disabled", 0, 100));
      const disabled = assetOf(await gateway.next("ExportAsset"));
      expect([disabled.requestId, disabled.error]).toEqual([
        "r-disabled",
        { code: 500, message: "Assets collection disabled(1008)" },
      ]);
    });

    test.concurrent("refused while another profiler runs on the thread", async () => {
      await using gateway = await grpcGateway([]);
      using dir = tempDir("nsolid-cpu-prof", {});
      await using proc = spawnAgent(gateway, UNTIL_STDIN_CLOSES, {}, ["--cpu-prof", `--cpu-prof-dir=${String(dir)}`]);
      await gateway.command(profileCommand("r-taken", 0, 100));
      const taken = assetOf(await gateway.next("ExportAsset"));
      expect([taken.requestId, taken.error]).toEqual([
        "r-taken",
        { code: 409, message: "Operation already in progress(1001)" },
      ]);
    });

    test.concurrent("cut short by the process's exit, which names it", async () => {
      await using gateway = await grpcGateway([]);
      await using proc = spawnAgent(gateway, `for await (const _ of console) {} process.exitCode = 4;`);
      await gateway.command(profileCommand("r-exit", 0, 60_000));
      // The idle main thread runs the profile's start long before the agent
      // has answered a later command.
      await gateway.command({ requestId: "r-info", id: "x", command: "info" });
      await gateway.next("ExportInfo");
      proc.stdin.end();
      const asset = assetOf(await gateway.next("ExportAsset"));
      const exit = await gateway.next("ExportExit");
      expect({
        requestId: asset.requestId,
        complete: asset.complete,
        profile: exit.body.profile,
        code: exit.body.code,
      }).toEqual({ requestId: "r-exit", complete: true, profile: "r-exit", code: 4 });
      expect(await proc.exited).toBe(4);
    });
  });

  describe("takes heap snapshots", () => {
    const snapshotCommand = (requestId: string, threadId: number, redacted: boolean) => ({
      requestId,
      id: "x",
      command: "snapshot",
      args: { profile: { thread_id: threadId, duration: 10_000, heap_snapshot: { redacted } } },
    });
    // A V8 snapshot's nodes as { type, name, edges }, each edge as
    // { type, name, to } with `to` a node index.
    const graphOf = (snapshot: any) => {
      const { node_fields, node_types, edge_fields, edge_types } = snapshot.snapshot.meta;
      const node = (field: string) => node_fields.indexOf(field);
      const edge = (field: string) => edge_fields.indexOf(field);
      const nodes: { type: string; name: string; edges: { type: string; name: string | number; to: number }[] }[] = [];
      let e = 0;
      for (let i = 0; i < snapshot.nodes.length; i += node_fields.length) {
        const edges = [];
        for (let k = 0; k < snapshot.nodes[i + node("edge_count")]; k++, e += edge_fields.length) {
          const type = edge_types[0][snapshot.edges[e + edge("type")]];
          const nameOrIndex = snapshot.edges[e + edge("name_or_index")];
          edges.push({
            type,
            name: type === "element" || type === "hidden" ? nameOrIndex : snapshot.strings[nameOrIndex],
            to: snapshot.edges[e + edge("to_node")] / node_fields.length,
          });
        }
        nodes.push({
          type: node_types[0][snapshot.nodes[i + node("type")]],
          name: snapshot.strings[snapshot.nodes[i + node("name")]],
          edges,
        });
      }
      return { nodes, edgesRead: e };
    };
    const plantedOf = (graph: ReturnType<typeof graphOf>, className: string) => {
      const holder = graph.nodes.find(n => n.type === "object" && n.name === className);
      const value = holder?.edges.find(edge => edge.type === "property" && edge.name === "value");
      return value && { type: graph.nodes[value.to].type, name: graph.nodes[value.to].name };
    };

    test.concurrent("of a thread, in V8's format, and with no string's text when redacted", async () => {
      await using gateway = await grpcGateway([]);
      await using proc = spawnAgent(
        gateway,
        `class PlantedHolder { constructor(value) { this.value = value; } }
         globalThis.holder = new PlantedHolder(crypto.randomUUID());
         globalThis.many = Array.from({ length: 50_000 }, (_, i) => ({ i }));
         console.log(globalThis.holder.value);
         ${UNTIL_STDIN_CLOSES}`,
      );
      const planted = await lineReader(proc.stdout)();
      await gateway.command(snapshotCommand("r-plain", 0, false));
      const plain = assetOf(await gateway.next("ExportAsset"));
      await gateway.command(snapshotCommand("r-redacted", 0, true));
      const redacted = assetOf(await gateway.next("ExportAsset"));

      expect([plain, redacted].map(a => [a.requestId, a.command, a.threadId, a.error, a.complete])).toEqual([
        ["r-plain", "snapshot", 0, undefined, true],
        ["r-redacted", "snapshot", 0, undefined, true],
      ]);
      expect(plantedOf(graphOf(plain.profile), "PlantedHolder")).toEqual({ type: "string", name: planted });
      expect(plain.text).toContain(planted);

      const snapshot = redacted.profile;
      expect(redacted.text).not.toContain(planted);
      expect(Object.keys(snapshot)).toEqual([
        "snapshot",
        "nodes",
        "edges",
        "trace_function_infos",
        "trace_tree",
        "samples",
        "locations",
        "strings",
      ]);
      const { node_count, edge_count, meta } = snapshot.snapshot;
      const graph = graphOf(snapshot);
      expect({
        nodes: snapshot.nodes.length,
        edges: snapshot.edges.length,
        edgesRead: graph.edgesRead,
        targets: graph.nodes.every(n => n.edges.every(edge => Number.isInteger(edge.to) && edge.to < node_count)),
      }).toEqual({
        nodes: node_count * meta.node_fields.length,
        edges: edge_count * meta.edge_fields.length,
        edgesRead: edge_count * meta.edge_fields.length,
        targets: true,
      });
      const types = new Set(graph.nodes.map(n => n.type));
      for (const type of ["synthetic", "object", "closure", "string", "code", "hidden", "object shape"]) {
        expect(types).toContain(type);
      }
      expect(graph.nodes[0]).toMatchObject({ type: "synthetic", name: "" });
      expect(new Set(graph.nodes.filter(n => n.type === "string").map(n => n.name))).toEqual(new Set(["(redacted)"]));
      expect(plantedOf(graph, "PlantedHolder")).toEqual({ type: "string", name: "(redacted)" });
      // In chunks of at most 1 MiB.
      expect(redacted.chunkSizes.length).toBeGreaterThan(1);
      expect(Math.max(...redacted.chunkSizes)).toBeLessThanOrEqual(1 << 20);
    });

    test.concurrent("of a worker's heap, not the main thread's", async () => {
      await using gateway = await grpcGateway([]);
      await using proc = spawnAgent(
        gateway,
        `const { Worker } = require("node:worker_threads");
         const worker = new Worker(
           "class WorkerHolder { constructor(value) { this.value = value; } }" +
           "globalThis.holder = new WorkerHolder('w'); setInterval(() => {}, 1000);",
           { eval: true },
         );
         worker.on("online", () => console.log("online"));
         for await (const line of console) {}
         worker.terminate();`,
      );
      expect(await lineReader(proc.stdout)()).toBe("online");
      await gateway.command(snapshotCommand("r-worker", 1, true));
      const asset = assetOf(await gateway.next("ExportAsset"));
      const names = new Set(graphOf(asset.profile).nodes.map(n => n.name));
      expect([asset.requestId, asset.threadId, asset.error, names.has("WorkerHolder")]).toEqual([
        "r-worker",
        1,
        undefined,
        true,
      ]);
    });

    test.concurrent("with N|Solid's errors: in progress, thread gone, disabled", async () => {
      await using gateway = await grpcGateway([]);
      await using proc = spawnAgent(gateway, UNTIL_STDIN_CLOSES);
      await gateway.command(snapshotCommand("r-first", 0, true));
      await gateway.command(snapshotCommand("r-again", 0, true));
      await gateway.command(snapshotCommand("r-nobody", 7, true));
      const answers = [];
      for (let i = 0; i < 3; i++) answers.push(assetOf(await gateway.next("ExportAsset")));
      answers.sort((a, b) => a.requestId.localeCompare(b.requestId));
      expect(answers.map(a => [a.requestId, a.threadId, a.error, a.complete])).toEqual([
        ["r-again", 0, { code: 409, message: "Operation already in progress(1001)" }, false],
        ["r-first", 0, undefined, true],
        ["r-nobody", 7, { code: 410, message: "Thread already gone(1002)" }, false],
      ]);

      await using otherGateway = await grpcGateway([]);
      await using disabled = spawnAgent(otherGateway, UNTIL_STDIN_CLOSES, { NSOLID_DISABLE_SNAPSHOTS: "1" });
      await otherGateway.command(snapshotCommand("r-disabled", 0, true));
      const refused = assetOf(await otherGateway.next("ExportAsset"));
      expect([refused.requestId, refused.error]).toEqual([
        "r-disabled",
        { code: 500, message: "Heap Snapshots disabled(1004)" },
      ]);
    });
  });

  test.concurrent("reports blocked loops past the threshold, with the stack that blocked", async () => {
    await using gateway = await grpcGateway([]);
    const script = [
      `const { Worker } = require("node:worker_threads");`,
      `function spin(ms) { const end = Date.now() + ms; while (Date.now() < end) {} }`,
      `function beforeReconfigure() { spin(600); }`,
      `function briefly() { spin(100); }`,
      `function blockFor() { spin(1500); }`,
      `const worker = new Worker(`,
      `  "function workerBlock() { const end = Date.now() + 1500; while (Date.now() < end) {} }\\n" +`,
      `  "require('node:worker_threads').parentPort.on('message', () => workerBlock());",`,
      `  { eval: true },`,
      `);`,
      `worker.on("online", () => console.log("online"));`,
      `const actions = { beforeReconfigure, briefly, blockFor, worker: () => worker.postMessage("block") };`,
      `for await (const line of console) { actions[line](); console.log("done " + line); }`,
      `worker.terminate();`,
    ].join("\n");
    await using proc = spawnAgent(gateway, script, { NSOLID_BLOCKED_LOOP_THRESHOLD: "5000" });
    const nextLine = lineReader(proc.stdout);
    const run = async (action: string) => {
      proc.stdin.write(action + "\n");
      expect(await nextLine()).toBe("done " + action);
    };
    const summary = (event: any) => ({
      command: event.common.command,
      threadId: Number(event.body.thread_id),
      frames: event.body.stack?.slice(0, 2).map((frame: any) => [frame.function_name, frame.line_number]),
    });
    expect(await nextLine()).toBe("online");

    // Under the threshold: nothing, so the first event is the third block's.
    await run("beforeReconfigure");
    await gateway.command({
      requestId: "r-threshold",
      id: "x",
      command: "reconfigure",
      args: { reconfigure: { blockedLoopThreshold: 300 } },
    });
    await gateway.next("ExportReconfigure");
    await run("briefly");
    await run("blockFor");
    const blocked = await gateway.next("ExportBlockedLoop");
    const unblocked = await gateway.next("ExportUnblockedLoop");
    expect([summary(blocked), summary(unblocked)]).toEqual([
      {
        command: "loop_blocked",
        threadId: 0,
        frames: [
          ["spin", 2],
          ["blockFor", 5],
        ],
      },
      { command: "loop_unblocked", threadId: 0, frames: undefined },
    ]);
    const top = blocked.body.stack[0];
    expect({
      scriptName: top.script_name.includes("eval"),
      isEval: top.is_eval,
      column: top.column > 0,
      scriptId: Number(top.script_id) > 0,
      blockedFor: blocked.body.blocked_for >= 300 && blocked.body.blocked_for < 1500,
      ranFor: unblocked.body.blocked_for >= 1500,
      sameLoop: unblocked.body.loop_id === blocked.body.loop_id,
    }).toEqual({
      scriptName: true,
      isEval: false,
      column: true,
      scriptId: true,
      blockedFor: true,
      ranFor: true,
      sameLoop: true,
    });

    await run("worker");
    const workerBlocked = await gateway.next("ExportBlockedLoop");
    const workerUnblocked = await gateway.next("ExportUnblockedLoop");
    expect([summary(workerBlocked), summary(workerUnblocked)]).toEqual([
      {
        command: "loop_blocked",
        threadId: 1,
        frames: [
          ["workerBlock", 1],
          ["", 2],
        ],
      },
      { command: "loop_unblocked", threadId: 1, frames: undefined },
    ]);
  });

  test.concurrent("reports a loop blocked outside JavaScript, with an empty stack", async () => {
    await using gateway = await grpcGateway([]);
    await using proc = spawnAgent(
      gateway,
      `for await (const line of console) { Bun.sleepSync(2500); console.log("done"); }`,
      { NSOLID_BLOCKED_LOOP_THRESHOLD: "300" },
    );
    await gateway.commandMetadata;
    proc.stdin.write("go\n");
    const blocked = await gateway.next("ExportBlockedLoop");
    const unblocked = await gateway.next("ExportUnblockedLoop");
    expect({
      threadId: Number(blocked.body.thread_id),
      stack: blocked.body.stack ?? [],
      ranFor: unblocked.body.blocked_for >= 2500,
    }).toEqual({ threadId: 0, stack: [], ranFor: true });
  });

  test.concurrent("lists the packages the main script can require, and marks the ones it loaded", async () => {
    await using gateway = await grpcGateway([]);
    using dir = tempDir("nsolid-packages", {
      "package.json": JSON.stringify({ name: "app" }),
      "app.js": `require("a"); require("d"); console.log("loaded"); ${UNTIL_STDIN_CLOSES}`,
      "node_modules/a/package.json": JSON.stringify({
        name: "a",
        version: "1.0.0",
        main: "index.js",
        dependencies: { c: "^3" },
      }),
      "node_modules/a/index.js": `module.exports = require("c");`,
      "node_modules/a/node_modules/c/package.json": JSON.stringify({ name: "c", version: "3.0.0" }),
      "node_modules/a/node_modules/c/index.js": `module.exports = "c";`,
      "node_modules/@scope/b/package.json": JSON.stringify({ name: "@scope/b", version: "2.0.0" }),
      "node_modules/.hidden/package.json": JSON.stringify({ name: "hidden" }),
      // bun install's isolated linker: packages live under node_modules/.bun,
      // linked by name where they are required from.
      "node_modules/.bun/d@4.0.0/node_modules/d/package.json": JSON.stringify({
        name: "d",
        version: "4.0.0",
        dependencies: { e: "^5" },
      }),
      "node_modules/.bun/d@4.0.0/node_modules/d/index.js": `module.exports = require("e");`,
      "node_modules/.bun/e@5.0.0/node_modules/e/package.json": JSON.stringify({ name: "e", version: "5.0.0" }),
      "node_modules/.bun/e@5.0.0/node_modules/e/index.js": `module.exports = "e";`,
    });
    const root = realpathSync(String(dir));
    symlinkSync(join(root, "node_modules/.bun/d@4.0.0/node_modules/d"), join(root, "node_modules/d"), "dir");
    symlinkSync(
      join(root, "node_modules/.bun/e@5.0.0/node_modules/e"),
      join(root, "node_modules/.bun/d@4.0.0/node_modules/e"),
      "dir",
    );
    await using proc = Bun.spawn({
      cmd: [bunExe(), join(root, "app.js")],
      cwd: root,
      stdin: "pipe",
      stdout: "pipe",
      stderr: "inherit",
      env: { ...bunEnv, NSOLID_GRPC: `127.0.0.1:${gateway.port}`, NSOLID_GRPC_INSECURE: "1" },
    });
    const reader = proc.stdout.getReader();
    expect(new TextDecoder().decode((await reader.read()).value).trim()).toBe("loaded");

    await gateway.command({ requestId: "r-packages", id: "x", command: "packages" });
    const { body } = await gateway.next("ExportPackages");
    const packages = body.packages
      .filter((p: any) => p.path.startsWith(root))
      .map((p: any) => ({
        path: p.path.slice(root.length + 1),
        name: p.name,
        version: p.version,
        dependencies: p.dependencies ?? [],
        required: p.required ?? false,
      }))
      .sort((a: any, b: any) => a.path.localeCompare(b.path));
    expect(packages).toEqual([
      {
        path: "node_modules/.bun/d@4.0.0/node_modules/e",
        name: "e",
        version: "5.0.0",
        dependencies: [],
        required: true,
      },
      { path: "node_modules/@scope/b", name: "@scope/b", version: "2.0.0", dependencies: [], required: false },
      { path: "node_modules/a", name: "a", version: "1.0.0", dependencies: ["node_modules/c"], required: true },
      { path: "node_modules/a/node_modules/c", name: "c", version: "3.0.0", dependencies: [], required: true },
      {
        path: "node_modules/d",
        name: "d",
        version: "4.0.0",
        dependencies: ["../.bun/d@4.0.0/node_modules/e"],
        required: true,
      },
    ]);
  });

  describe("reports the process's exit", () => {
    const exitOf = async (
      script: string,
      end: (proc: Bun.Subprocess) => void,
      extraEnv: Record<string, string> = {},
      args: string[] = [],
    ) => {
      await using gateway = await grpcGateway([{ requestId: "r-info", id: "x", command: "info" }]);
      await using proc = Bun.spawn({
        cmd: [bunExe(), "-e", script, ...args],
        stdin: "pipe",
        stdout: "ignore",
        stderr: "ignore",
        env: { ...bunEnv, NSOLID_GRPC: `127.0.0.1:${gateway.port}`, NSOLID_GRPC_INSECURE: "1", ...extraEnv },
      });
      // Answering `info` means the agent is connected.
      await gateway.next("ExportInfo");
      end(proc);
      const exit = await gateway.next("ExportExit");
      await proc.exited;
      return {
        command: exit.common.command,
        requestId: exit.common.requestId,
        code: exit.body.code ?? 0,
        signal: exit.body.signal ?? "",
        error: exit.body.error,
        exitCode: proc.exitCode,
        signalCode: proc.signalCode,
      };
    };

    test.concurrent("with its exit code", async () => {
      const exit = await exitOf(`${UNTIL_STDIN_CLOSES}; process.exitCode = 3;`, proc => proc.stdin!.end());
      expect(exit.requestId).toMatch(/^[0-9a-f]{40}$/);
      expect({ ...exit, requestId: undefined }).toEqual({
        command: "exit",
        requestId: undefined,
        code: 3,
        signal: "",
        error: undefined,
        exitCode: 3,
        signalCode: null,
      });
    });

    test.concurrent("with the uncaught exception it ends with", async () => {
      const exit = await exitOf(`${UNTIL_STDIN_CLOSES}; setTimeout(() => { throw new Error("boom"); });`, proc =>
        proc.stdin!.end(),
      );
      expect({ code: exit.code, message: exit.error?.message, exitCode: exit.exitCode }).toEqual({
        code: 1,
        message: "Uncaught Error: boom",
        exitCode: 1,
      });
      expect(exit.error.stack).toStartWith("Error: boom\n");
    });

    test.concurrent("with the signal that ends it", async () => {
      const exit = await exitOf(UNTIL_STDIN_CLOSES, proc => proc.kill("SIGTERM"));
      expect({ code: exit.code, signal: exit.signal, signalCode: exit.signalCode }).toEqual({
        code: 15,
        signal: "SIGTERM",
        signalCode: "SIGTERM",
      });
    });

    // The signal handler waits on the agent's thread and on the thread that
    // reports the signal. A signal the kernel gave either of them went
    // unreported, and held the exit for 5 seconds.
    for (const thread of ["NSolidAgent", "NSolidSignal"]) {
      test.concurrent.skipIf(!isGlibc)(`with a signal the kernel gives the ${thread} thread`, async () => {
        const exit = await exitOf(UNTIL_STDIN_CLOSES, proc => {
          // Threads the agent's thread starts take its name too; it is the first.
          const tid = readdirSync(`/proc/${proc.pid}/task`)
            .map(Number)
            .sort((a, b) => a - b)
            .find(tid => readFileSync(`/proc/${proc.pid}/task/${tid}/comm`, "utf8").trim() === thread)!;
          expect(tgkill!(proc.pid, tid, 15)).toBe(0);
        });
        expect({ code: exit.code, signal: exit.signal, signalCode: exit.signalCode }).toEqual({
          code: 15,
          signal: "SIGTERM",
          signalCode: "SIGTERM",
        });
      });
    }

    test.concurrent.each([
      ["segfault", 11, "SIGSEGV", "Segmentation fault at address 0xDEADBEEF"],
      ["panic", 6, "SIGABRT", "invoked crashByPanic() handler"],
    ])("with the crash that ends it: %s", async (crash, code, signal, message) => {
      const exit = await exitOf(
        `const { crash_handler } = require("bun:internal-for-testing");
         for await (const line of console) crash_handler[line]();`,
        proc => proc.stdin!.write(crash + "\n"),
        { BUN_FEATURE_FLAG_INTERNAL_FOR_TESTING: "1", BUN_ENABLE_CRASH_REPORTING: "0" },
        // A debug build symbolizes the trace for seconds instead.
        ["--debug-crash-handler-use-trace-string"],
      );
      expect({
        code: exit.code,
        signal: exit.signal,
        message: exit.error?.message,
        trace: typeof exit.error?.stack,
        signalCode: exit.signalCode,
      }).toEqual({ code, signal, message, trace: "string", signalCode: signal });
    });
  });

  test.concurrent("reads its configuration with N|Solid's precedence and validation", async () => {
    await using gateway = await grpcGateway([
      { requestId: "r-info", id: "x", command: "info" },
      { requestId: "r-initial", id: "x", command: "reconfigure" },
      {
        requestId: "r-invalid",
        id: "x",
        command: "reconfigure",
        args: { reconfigure: { interval: 0, traceSampleRate: 5, tags: ["  zz", "yy "] } },
      },
    ]);
    // A tag is cut at 140 bytes without splitting a character: 1 + 69 * 2.
    const longTag = "a" + Buffer.alloc(140, "é").toString();
    const manyTags = Array.from({ length: 22 }, (_, i) => `t${String(i).padStart(2, "0")}`);
    using dir = tempDir("nsolid-config", {
      "package.json": JSON.stringify({
        name: "from-package-json",
        nsolid: {
          interval: 2500,
          traceSampleRate: 0.25,
          redactSnapshots: true,
          hostname: "pkg-host",
          assetsEnabled: 0,
        },
      }),
      "app.js": UNTIL_STDIN_CLOSES,
    });
    await using proc = Bun.spawn({
      cmd: [bunExe(), join(String(dir), "app.js")],
      cwd: String(dir),
      stdin: "pipe",
      stdout: "ignore",
      stderr: "pipe",
      env: {
        ...bunEnv,
        NSOLID_GRPC: `127.0.0.1:${gateway.port}`,
        NSOLID_GRPC_INSECURE: "1",
        NSOLID_APPNAME: "first",
        NSOLID_APP: "second",
        NSOLID_INTERVAL: "-5",
        NSOLID_TRACE_SAMPLE_RATE: "2",
        NSOLID_REDACT_SNAPSHOTS: "false",
        NSOLID_TRACING_MODULES_BLACKLIST: "dns, http, nope",
        NSOLID_STATSD: "8125",
        NSOLID_TAGS: [...manyTags, "x", longTag].join(","),
      },
    });

    const info = await gateway.next("ExportInfo");
    expect({ app: info.body.app, hostname: info.body.hostname, tags: info.body.tags }).toEqual({
      app: "first",
      hostname: "pkg-host",
      tags: [longTag.slice(0, 70), ...manyTags.slice(0, 19)],
    });
    expect(Buffer.byteLength(info.body.tags[0])).toBe(139);

    const initial = await gateway.next("ExportReconfigure");
    expect(initial.body).toEqual({
      assetsEnabled: false,
      blockedLoopThreshold: "200",
      contCpuProfile: false,
      ebpfProfiling: 2,
      interval: "2500",
      pauseMetrics: false,
      promiseTracking: false,
      redactSnapshots: true,
      sourceCodeEnabled: false,
      statsd: "localhost:8125",
      statsdBucket: "nsolid.${env}.${app}.${hostname}.${shortId}",
      tags: info.body.tags,
      traceSampleRate: 0.25,
      tracingEnabled: false,
      tracingModulesBlacklist: 13,
    });

    const invalid = await gateway.next("ExportReconfigure");
    expect({
      interval: invalid.body.interval,
      traceSampleRate: invalid.body.traceSampleRate,
      tags: invalid.body.tags,
    }).toEqual({ interval: "2500", traceSampleRate: 0.25, tags: ["yy", "zz"] });

    proc.stdin.end();
    const stderr = await proc.stderr.text();
    expect(stderr).toContain(`Tags must be between 2 to 140 bytes. The following are violations:\n  ${longTag}\n  x\n`);
    expect(stderr).toContain("A maximum of 20 tags are allowed but 23 were provided. Only using the first 20 tags\n");
  });

  test.concurrent(
    "connects over TLS to a gateway the CA in NSOLID_GRPC_CERTS signed, and writes NSOLID_GRPC_KEYLOG",
    async () => {
      await using gateway = await recordingGateway(keepOpen, tls);
      using dir = tempDir("nsolid-keylog", { "ca.pem": tls.cert });
      await using proc = Bun.spawn({
        cmd: [bunExe(), "-e", UNTIL_STDIN_CLOSES],
        cwd: String(dir),
        stdin: "pipe",
        stdout: "ignore",
        stderr: "inherit",
        env: {
          ...bunEnv,
          NSOLID_GRPC: `localhost:${gateway.port}`,
          NSOLID_GRPC_CERTS: "ca.pem",
          NSOLID_GRPC_KEYLOG: "1",
        },
      });

      await gateway.until(seen => seen.length > 0);
      expect(gateway.seen[0].headers[":path"]).toBe(COMMAND_PATH);
      const keylog = join(String(dir), `nsolid-tls-keylog-${proc.pid}.log`);
      while (!existsSync(keylog) || !readFileSync(keylog, "utf8").includes("CLIENT_TRAFFIC_SECRET_0")) {
        await Bun.sleep(10);
      }
      expect(readFileSync(keylog, "utf8")).toMatch(/^CLIENT_HANDSHAKE_TRAFFIC_SECRET [0-9a-f]{64} [0-9a-f]+$/m);
    },
  );

  test.concurrent("presents NSOLID_GRPC_CLIENT_CERT, and a renewed one from the next connection on", async () => {
    using dir = tempDir("nsolid-mtls", {
      "ca.pem": tls.cert,
      "cert.pem": nodeKey("agent1-cert.pem"),
      "key.pem": nodeKey("agent1-key.pem"),
    });
    let renewed = false;
    await using gateway = await recordingGateway(
      // Once the files are renewed, connections with the old certificate are ended.
      command => (renewed && command.peer !== "agent3" ? endWith(14)(command) : keepOpen(command)),
      { ...tls, ca: [nodeKey("ca1-cert.pem"), nodeKey("ca2-cert.pem")], requestCert: true, rejectUnauthorized: true },
    );
    await using proc = Bun.spawn({
      cmd: [bunExe(), "-e", UNTIL_STDIN_CLOSES],
      cwd: String(dir),
      stdin: "pipe",
      stdout: "ignore",
      stderr: "inherit",
      env: {
        ...bunEnv,
        NSOLID_GRPC: `localhost:${gateway.port}`,
        NSOLID_GRPC_CERTS: "ca.pem",
        NSOLID_GRPC_CLIENT_CERT: "cert.pem",
        NSOLID_GRPC_CLIENT_KEY: "key.pem",
        NSOLID_GRPC_CLIENT_CERT_REFRESH_INTERVAL: "1",
        BUN_NSOLID_RETRY_MS_FOR_TESTING: "20",
      },
    });

    await gateway.until(seen => seen.length > 0);
    expect(gateway.seen[0].peer).toBe("agent1");

    writeFileSync(join(String(dir), "cert.pem"), nodeKey("agent3-cert.pem"));
    writeFileSync(join(String(dir), "key.pem"), nodeKey("agent3-key.pem"));
    renewed = true;
    gateway.seen[0].stream.session?.destroy();
    await gateway.until(seen => seen.some(command => command.peer === "agent3"));
  });

  for (const [name, files, env, message] of [
    [
      "a client certificate without a key",
      { "cert.pem": "" },
      { NSOLID_GRPC_CLIENT_CERT: "cert.pem" },
      "N|Solid warning: NSOLID_GRPC_CLIENT_CERT is set without NSOLID_GRPC_CLIENT_KEY, so the agent connects without a client certificate\n",
    ],
    [
      "a client key without a certificate",
      { "key.pem": "" },
      { NSOLID_GRPC_CLIENT_KEY: "key.pem" },
      "N|Solid warning: NSOLID_GRPC_CLIENT_KEY is set without NSOLID_GRPC_CLIENT_CERT, so the agent connects without a client certificate\n",
    ],
    [
      "a client certificate that doesn't parse",
      { "cert.pem": "not a certificate", "key.pem": "not a key" },
      { NSOLID_GRPC_CLIENT_CERT: "cert.pem", NSOLID_GRPC_CLIENT_KEY: "key.pem" },
      "N|Solid warning: the client certificate (cert.pem) or its key (key.pem) cannot be used yet: ",
    ],
  ] as const) {
    test.concurrent(`warns about ${name}`, async () => {
      await using gateway = await recordingGateway(keepOpen, tls);
      using dir = tempDir("nsolid-client-cert", { "ca.pem": tls.cert, ...files });
      await using proc = Bun.spawn({
        cmd: [bunExe(), "-e", UNTIL_STDIN_CLOSES],
        cwd: String(dir),
        stdin: "pipe",
        stdout: "ignore",
        stderr: "pipe",
        env: {
          ...bunEnv,
          NSOLID_GRPC: `localhost:${gateway.port}`,
          NSOLID_GRPC_CERTS: "ca.pem",
          ...env,
        },
      });

      const stderr = await readUntil(proc.stderr, text => text.includes(message));
      expect(stderr).toContain(message);
    });
  }

  test.concurrent("stops after 20 UNAUTHENTICATED answers, with N|Solid's warning", async () => {
    await using gateway = await recordingGateway(endWith(16));
    await using proc = Bun.spawn({
      cmd: [bunExe(), "-e", UNTIL_STDIN_CLOSES],
      stdin: "pipe",
      stdout: "ignore",
      stderr: "pipe",
      env: {
        ...bunEnv,
        NSOLID_GRPC: `127.0.0.1:${gateway.port}`,
        NSOLID_GRPC_INSECURE: "1",
        BUN_NSOLID_RETRY_MS_FOR_TESTING: "5",
      },
    });

    const stderr = await readUntil(proc.stderr, text => text.includes("Unable to authenticate"));
    const agentId = gateway.seen[0].headers["nsolid-agent-id"];
    expect(stderr).toContain(
      `N|Solid warning: ${agentId} Unable to authenticate, please verify your token and network connection!\n`,
    );
    proc.stdin.end();
    expect(await proc.exited).toBe(0);
    expect(gateway.seen.length).toBe(20);
  });

  test.concurrent("waits as long as the gateway's retry-after asks before connecting again", async () => {
    await using gateway = await recordingGateway((command, index) =>
      index === 0 ? endWith(8, { "retry-after": "1" })(command) : keepOpen(command),
    );
    await using proc = Bun.spawn({
      cmd: [bunExe(), "-e", UNTIL_STDIN_CLOSES],
      stdin: "pipe",
      stdout: "ignore",
      stderr: "inherit",
      env: {
        ...bunEnv,
        NSOLID_GRPC: `127.0.0.1:${gateway.port}`,
        NSOLID_GRPC_INSECURE: "1",
        BUN_NSOLID_RETRY_MS_FOR_TESTING: "5",
      },
    });

    await gateway.until(seen => seen.length >= 2);
    expect(gateway.seen[1].at - gateway.seen[0].at).toBeGreaterThanOrEqual(950);
  });

  test.concurrent("nsolid is start(), with N|Solid's id, config, info, metrics and packages", async () => {
    using dir = tempDir("nsolid-api", {
      "package.json": JSON.stringify({ name: "api-app", version: "2.0.0" }),
      "node_modules/a/package.json": JSON.stringify({ name: "a", version: "1.0.0" }),
      "node_modules/a/index.js": "module.exports = 1;",
      "node_modules/b/package.json": JSON.stringify({ name: "b", version: "1.0.0" }),
      "app.js": `
        require("a");
        const nsolid = require("nsolid");
        const returned = nsolid.start({ app: "via-start", tags: "aa, bb", interval: 0, hostname: "h1" });
        const info = nsolid.info();
        const metrics = nsolid.metrics();
        const root = require("path").dirname(Bun.main);
        nsolid.info((err, viaCallback) => {
          console.log(JSON.stringify({
            isStart: typeof nsolid === "function" && returned === nsolid && nsolid.start === nsolid,
            app: nsolid.app,
            appName: nsolid.appName,
            appVersion: nsolid.appVersion,
            tags: nsolid.tags,
            interval: nsolid.config.interval,
            hostname: nsolid.config.hostname,
            flags: [nsolid.metricsPaused, nsolid.assetsEnabled, nsolid.tracingEnabled],
            info: {
              sameId: info.id === nsolid.id,
              app: info.app,
              appVersion: info.appVersion,
              pid: info.pid === process.pid,
              main: info.main === Bun.main,
              node: info.versions.node ?? null,
              bun: info.versions.bun === Bun.version,
            },
            metrics: {
              threadId: metrics.threadId,
              heap: metrics.heapUsed > 0 && metrics.heapTotal > 0,
              rss: metrics.rss > 0,
              cpu: metrics.cpu === metrics.cpuPercent,
            },
            packages: nsolid
              .packages()
              .filter(p => p.path.startsWith(root))
              .map(p => [p.name, p.required])
              .sort(),
            callback: err === null && viaCallback.id === nsolid.id,
          }));
        });`,
    });
    await using proc = Bun.spawn({
      cmd: [bunExe(), join(String(dir), "app.js")],
      cwd: String(dir),
      stdout: "pipe",
      stderr: "inherit",
      env: bunEnv,
    });
    const [stdout, exitCode] = await Promise.all([proc.stdout.text(), proc.exited]);
    expect(JSON.parse(stdout)).toEqual({
      isStart: true,
      app: "via-start",
      appName: "via-start",
      appVersion: "2.0.0",
      tags: ["aa", "bb"],
      interval: 5000,
      hostname: "h1",
      flags: [false, true, false],
      info: { sameId: true, app: "via-start", appVersion: "2.0.0", pid: true, main: true, node: null, bun: true },
      metrics: { threadId: 0, heap: true, rss: true, cpu: true },
      packages: [
        ["a", true],
        ["b", false],
      ],
      callback: true,
    });
    expect(exitCode).toBe(0);
  });

  test.concurrent(
    "start({ grpc }) starts the agent, which then takes start(config) and publishes the Console's reconfigure",
    async () => {
      await using gateway = await grpcGateway([]);
      await using proc = Bun.spawn({
        cmd: [
          bunExe(),
          "-e",
          `const nsolid = require("nsolid");
         nsolid.start({ grpc: "127.0.0.1:${gateway.port}", app: "started-by-script" });
         for await (const line of console) {
           if (line === "rename") nsolid.start({ app: "renamed" });
           if (line === "config") console.log(JSON.stringify({ interval: nsolid.config.interval, app: nsolid.app }));
         }`,
        ],
        stdin: "pipe",
        stdout: "pipe",
        stderr: "inherit",
        env: { ...bunEnv, NSOLID_GRPC_INSECURE: "1" },
      });
      const nextLine = lineReader(proc.stdout);
      const appOfInfo = async () => {
        await gateway.command({ requestId: "r-info", id: "x", command: "info" });
        return (await gateway.next("ExportInfo")).body.app;
      };

      expect(await appOfInfo()).toBe("started-by-script");
      proc.stdin.write("rename\n");
      while ((await appOfInfo()) !== "renamed");

      await gateway.command({
        requestId: "r-reconfigure",
        id: "x",
        command: "reconfigure",
        args: { reconfigure: { interval: 7000 } },
      });
      await gateway.next("ExportReconfigure");
      proc.stdin.write("config\n");
      expect(JSON.parse(await nextLine())).toEqual({ interval: 7000, app: "renamed" });
    },
  );

  test.concurrent("nsolid.profile(), profileEnd() and snapshot() send assets the way N|Solid marks them", async () => {
    await using gateway = await grpcGateway([]);
    await using proc = spawnAgent(
      gateway,
      `const nsolid = require("nsolid");
       const answer = (name) => (err) => console.log(name + " " + (err ? err.code + " " + err.message : "ok"));
       for await (const line of console) {
         if (line === "profile") nsolid.profile(60_000, answer("profile"));
         if (line === "end") nsolid.profileEnd(answer("end"));
         if (line === "snapshot") nsolid.snapshot(answer("snapshot"));
         if (line === "heap") nsolid.heapProfile(answer("heap"));
         if (line === "off") { nsolid.disableAssets(); console.log("off " + nsolid.assetsEnabled); }
         if (line === "refused") { try { nsolid.snapshot(); } catch (err) { console.log("refused " + err.code); } }
       }`,
    );
    const nextLine = lineReader(proc.stdout);
    const ask = async (line: string) => {
      proc.stdin.write(line + "\n");
      return nextLine();
    };
    const marked = (messages: any[]) => JSON.stringify(messages[0].metadata).includes("Agent API");
    await gateway.commandMetadata;

    expect(await ask("profile")).toBe("profile ok");
    // The profile starts once the agent has asked the thread to start it.
    let ended: string;
    while ((ended = await ask("end")) !== "end ok") expect(ended).toBe("end 404 CPU profile could not be stopped");
    const profile = await gateway.next("ExportAsset");
    expect(await ask("snapshot")).toBe("snapshot ok");
    const snapshot = await gateway.next("ExportAsset");
    expect(
      [profile, snapshot].map(m => [assetOf(m).command, assetOf(m).threadId, assetOf(m).complete, marked(m)]),
    ).toEqual([
      ["profile", 0, true, true],
      ["snapshot", 0, true, true],
    ]);
    expect(assetOf(profile).profile.nodes[0].callFrame.functionName).toBe("(root)");
    expect(assetOf(snapshot).profile.snapshot.node_count).toBeGreaterThan(0);

    expect(await ask("heap")).toBe("heap 404 Heap profile could not be started");
    expect(await ask("off")).toBe("off false");
    expect(await ask("refused")).toBe("refused 500");
  });

  test.concurrent("nsolid's profiles and snapshots are not available without an agent", async () => {
    await using proc = Bun.spawn({
      cmd: [
        bunExe(),
        "-e",
        `const nsolid = require("nsolid");
         const calls = [() => nsolid.profile(), () => nsolid.profileEnd(), () => nsolid.snapshot(), () => nsolid.heapSampling()];
         console.log(JSON.stringify(calls.map(call => { try { call(); return "ok"; } catch (err) { return [err.code, err.message]; } })));`,
      ],
      env: bunEnv,
      stdout: "pipe",
      stderr: "inherit",
    });
    const [stdout, exitCode] = await Promise.all([proc.stdout.text(), proc.exited]);
    expect(JSON.parse(stdout)).toEqual([
      [404, "CPU profile could not be started"],
      [404, "CPU profile could not be stopped"],
      [404, "Heap snapshot could not be generated"],
      [404, "Heap sampling could not be started"],
    ]);
    expect(exitCode).toBe(0);
  });

  describe("traces", () => {
    const valueOf = (value: any): unknown =>
      value.string_value ??
      (value.int_value !== undefined ? Number(value.int_value) : undefined) ??
      value.bool_value ??
      value.double_value ??
      value.array_value?.values.map(valueOf);
    const hex = (bytes: Uint8Array | undefined) => Buffer.from(bytes ?? []).toString("hex");
    const spansOf = (request: any) =>
      request.resource_spans.flatMap((resource: any) =>
        resource.scope_spans.flatMap((scope: any) =>
          scope.spans.map((span: any) => ({
            traceId: hex(span.trace_id),
            spanId: hex(span.span_id),
            parentSpanId: hex(span.parent_span_id),
            name: span.name,
            kind: span.kind,
            attributes: Object.fromEntries(span.attributes.map((a: any) => [a.key, valueOf(a.value)])),
            durationNs: BigInt(span.end_time_unix_nano) - BigInt(span.start_time_unix_nano),
            status: span.status ?? undefined,
          })),
        ),
      );
    // Every span exported until `found` finds what it looks for.
    async function spansUntil<T>(gateway: { next(rpc: string): Promise<any> }, found: (spans: any[]) => T | undefined) {
      const spans: any[] = [];
      for (;;) {
        const result = found(spans);
        if (result !== undefined) return result;
        spans.push(...spansOf(await gateway.next("TraceExport")));
      }
    }
    const metricPoint = (request: any, name: string) =>
      request.resource_metrics[0].scope_metrics[0].metrics.find((m: any) => m.name === name);

    test.concurrent("of node:http, with the client's span the parent of the server's", async () => {
      await using gateway = await grpcGateway([]);
      const script = `
        const http = require("node:http");
        const server = http.createServer((req, res) => res.end("ok"));
        server.listen(0, "127.0.0.1", () => {
          console.log(server.address().port);
          setInterval(() => {
            http.get({ host: "127.0.0.1", port: server.address().port, path: "/work?x=1" }, res => res.resume());
          }, 50);
        });`;
      await using proc = spawnAgent(gateway, script, { NSOLID_TRACING_ENABLED: "1", NSOLID_INTERVAL: "100" });
      const port = Number(await lineReader(proc.stdout)());
      const [client, server] = await spansUntil(gateway, spans => {
        for (const server of spans.filter(span => span.attributes["nsolid.span_type"] === 8)) {
          const client = spans.find(span => span.spanId === server.parentSpanId);
          if (client) return [client, server];
        }
      });
      const url = `http://127.0.0.1:${port}/work?x=1`;
      expect({ ...client, spanId: undefined, durationNs: undefined }).toEqual({
        traceId: server.traceId,
        spanId: undefined,
        parentSpanId: "",
        name: "HTTP GET",
        kind: 3,
        attributes: {
          "http.method": "GET",
          "http.url": url,
          "network.protocol.version": "1.1",
          "http.status_code": 200,
          "http.status_text": "OK",
          "thread.id": 0,
          "nsolid.span_type": 4,
        },
        durationNs: undefined,
        status: undefined,
      });
      expect({ ...server, spanId: undefined, durationNs: undefined }).toEqual({
        traceId: client.traceId,
        spanId: undefined,
        parentSpanId: client.spanId,
        name: "HTTP GET",
        kind: 2,
        attributes: {
          "http.method": "GET",
          "http.url": url,
          "network.protocol.version": "1.1",
          "http.status_code": 200,
          "http.status_text": "OK",
          "thread.id": 0,
          "nsolid.span_type": 8,
        },
        durationNs: undefined,
        status: undefined,
      });
      expect(client.durationNs).toBeGreaterThan(0n);
      expect(server.durationNs).toBeGreaterThan(0n);

      // The same requests, in the thread's metrics.
      for (;;) {
        const request = await gateway.next("MetricsExport");
        const count = (name: string) => Number(metricPoint(request, name).sum.data_points[0].as_int);
        if (count("httpServerCount") === 0 || count("httpClientCount") === 0) continue;
        const summary = metricPoint(request, "httpServer");
        expect({
          unit: summary.unit,
          quantiles: summary.summary.data_points[0].quantile_values.map((q: any) => q.quantile),
        }).toEqual({
          unit: "ms",
          quantiles: [0.5, 0.99],
        });
        break;
      }
    });

    test.concurrent("of Bun.serve and fetch, continuing the trace a request came with", async () => {
      await using gateway = await grpcGateway([]);
      const script = `
        const nsolid = require("nsolid");
        const server = Bun.serve({
          port: 0,
          async fetch(req) {
            if (new URL(req.url).pathname === "/inner") return new Response("inner", { status: 201 });
            nsolid.logger.info("outer");
            const res = await fetch("http://127.0.0.1:" + server.port + "/inner?q=1");
            return new Response(await res.text());
          },
        });
        console.log(server.port);
        for await (const _ of console) {}`;
      await using proc = spawnAgent(gateway, script, { NSOLID_TRACING_ENABLED: "1" });
      const port = Number(await lineReader(proc.stdout)());
      const traceId = "4bf92f3577b34da6a3ce929d0e0e4736";
      const callerSpanId = "00f067aa0ba902b7";
      let poking = true;
      (async () => {
        while (poking) {
          await fetch(`http://127.0.0.1:${port}/outer`, {
            headers: { traceparent: `00-${traceId}-${callerSpanId}-01` },
          }).then(res => res.text());
        }
      })();
      try {
        const [outer, client, inner] = await spansUntil(gateway, spans => {
          for (const outer of spans.filter(span => span.parentSpanId === callerSpanId)) {
            const client = spans.find(span => span.parentSpanId === outer.spanId);
            const inner = client && spans.find(span => span.parentSpanId === client.spanId);
            if (inner) return [outer, client, inner];
          }
        });
        const summary = ({ traceId, name, kind, attributes }: any) => ({ traceId, name, kind, attributes });
        expect([outer, client, inner].map(summary)).toEqual([
          {
            traceId,
            name: "HTTP GET",
            kind: 2,
            attributes: {
              "http.method": "GET",
              "http.url": `http://127.0.0.1:${port}/outer`,
              "network.protocol.version": "1.1",
              "http.status_code": 200,
              "thread.id": 0,
              "nsolid.span_type": 8,
            },
          },
          {
            traceId,
            name: "HTTP GET",
            kind: 3,
            attributes: {
              "http.method": "GET",
              "http.url": `http://127.0.0.1:${port}/inner?q=1`,
              "http.status_code": 201,
              "thread.id": 0,
              "nsolid.span_type": 4,
            },
          },
          {
            traceId,
            name: "HTTP GET",
            kind: 2,
            attributes: {
              "http.method": "GET",
              "http.url": `http://127.0.0.1:${port}/inner?q=1`,
              "network.protocol.version": "1.1",
              "http.status_code": 201,
              "thread.id": 0,
              "nsolid.span_type": 8,
            },
          },
        ]);

        // A line logged in the handler carries the handler's span.
        for (;;) {
          const request = await gateway.next("LogsExport");
          const records = request.resource_logs.flatMap((r: any) => r.scope_logs.flatMap((s: any) => s.log_records));
          const record = records.find((r: any) => hex(r.span_id) === outer.spanId);
          if (!record) continue;
          expect({
            body: record.body.string_value,
            severity: record.severity_number,
            text: record.severity_text,
            traceId: hex(record.trace_id),
            flags: record.flags,
            sameTimes: record.time_unix_nano === record.observed_time_unix_nano,
          }).toEqual({ body: "outer", severity: 9, text: "INFO", traceId, flags: 1, sameTimes: true });
          expect(Math.abs(Number(BigInt(record.time_unix_nano) / 1_000_000n) - Date.now())).toBeLessThan(60_000);
          break;
        }
      } finally {
        poking = false;
      }
    });

    test.concurrent("of node:dns, with its counts and latencies in the thread's metrics", async () => {
      await using gateway = await grpcGateway([]);
      const script = `setInterval(() => require("node:dns").promises.lookup("localhost").catch(() => {}), 50);`;
      await using proc = spawnAgent(gateway, script, { NSOLID_TRACING_ENABLED: "1", NSOLID_INTERVAL: "100" });
      const span = await spansUntil(gateway, spans => spans.find(span => span.name === "DNS lookup"));
      const { "dns.address": address, ...attributes } = span.attributes;
      expect({ kind: span.kind, parentSpanId: span.parentSpanId, attributes }).toEqual({
        kind: 3,
        parentSpanId: "",
        attributes: { "dns.op_type": 0, "dns.hostname": "localhost", "thread.id": 0, "nsolid.span_type": 1 },
      });
      expect(JSON.parse(address)).toBeTypeOf("string");
      for (;;) {
        const request = await gateway.next("MetricsExport");
        if (Number(metricPoint(request, "dnsCount").sum.data_points[0].as_int) > 0) break;
      }
    });

    test.concurrent("sampled at the root with traceSampleRate, which reconfigure changes", async () => {
      await using gateway = await grpcGateway([]);
      const script = `
        const http = require("node:http");
        const server = http.createServer((req, res) => { console.log(req.headers.traceparent); res.end(); });
        server.listen(0, "127.0.0.1", () => {
          setInterval(() => http.get({ host: "127.0.0.1", port: server.address().port }, res => res.resume()), 50);
        });`;
      await using proc = spawnAgent(gateway, script, { NSOLID_TRACING_ENABLED: "1", NSOLID_TRACE_SAMPLE_RATE: "0" });
      const nextLine = lineReader(proc.stdout);
      // Not sampled, but still propagated, so that the services downstream don't sample it either.
      while (!/^00-[0-9a-f]{32}-[0-9a-f]{16}-00$/.test(await nextLine()));
      await gateway.command({
        requestId: "r-rate",
        id: "x",
        command: "reconfigure",
        args: { reconfigure: { traceSampleRate: 1 } },
      });
      expect((await gateway.next("ExportReconfigure")).body.traceSampleRate).toBe(1);
      await spansUntil(gateway, spans => spans.find(span => span.attributes["nsolid.span_type"] === 8));
    });

    test.concurrent("of the OpenTelemetry API an application registers, in its own traces", async () => {
      await using gateway = await grpcGateway([]);
      // What the bridge uses of @opentelemetry/api: it delegates to the registered globals, as the API does.
      const script = `
        const nsolid = require("nsolid");
        const KEY = Symbol.for("OpenTelemetry Context Key SPAN");
        class Context {
          constructor(values = new Map()) { this.values = values; }
          getValue(key) { return this.values.get(key); }
          setValue(key, value) { return new Context(new Map(this.values).set(key, value)); }
          deleteValue(key) { const values = new Map(this.values); values.delete(key); return new Context(values); }
        }
        const ROOT_CONTEXT = new Context();
        let manager, provider;
        const api = {
          ROOT_CONTEXT,
          context: {
            setGlobalContextManager(m) { if (manager) return false; manager = m; return true; },
            active() { return manager ? manager.active() : ROOT_CONTEXT; },
            with(context, fn, thisArg, ...args) { return manager.with(context, fn, thisArg, ...args); },
            disable() { manager?.disable(); manager = undefined; },
          },
          trace: {
            setGlobalTracerProvider(p) { if (provider) return false; provider = p; return true; },
            getTracer(name, version) { return provider.getTracer(name, version); },
            getSpanContext(context) { return context.getValue(KEY)?.spanContext(); },
            setSpan(context, span) { return context.setValue(KEY, span); },
            wrapSpanContext(spanContext) { return { spanContext: () => spanContext, end() {} }; },
            disable() { provider = undefined; },
          },
        };
        console.log(nsolid.otel.register(api));
        try { nsolid.otel.register(api); } catch (error) { console.log(error.code); }
        nsolid.otel.registerInstrumentations({
          enable() {
            const tracer = api.trace.getTracer("app", "1.0.0");
            setInterval(() => {
              const result = tracer.startActiveSpan("parent", { attributes: { a: 1, list: ["x", "y"], nested: { no: 1 } } }, parent => {
                const child = tracer.startSpan("child", { kind: 2 });
                child.setStatus({ code: 2, message: "bad" });
                child.end();
                nsolid.logger.warn({ in: "parent" });
                parent.end();
                return "result";
              });
              if (result !== "result") console.log("lost the result");
            }, 50);
          },
          disable() {},
        });
        for await (const _ of console) {}`;
      await using proc = spawnAgent(gateway, script, { NSOLID_TRACING_ENABLED: "1" });
      const nextLine = lineReader(proc.stdout);
      expect([await nextLine(), await nextLine()]).toEqual(["true", "ERR_NSOLID_OTEL_API_ALREADY_REGISTERED"]);
      const [parent, child] = await spansUntil(gateway, spans => {
        for (const child of spans.filter(span => span.name === "child")) {
          const parent = spans.find(span => span.spanId === child.parentSpanId);
          if (parent) return [parent, child];
        }
      });
      const summary = ({ name, kind, parentSpanId, attributes, status }: any) => ({
        name,
        kind,
        root: parentSpanId === "",
        attributes,
        status,
      });
      expect([parent, child].map(summary)).toEqual([
        {
          name: "parent",
          kind: 1,
          root: true,
          attributes: { a: 1, list: ["x", "y"], "thread.id": 0, "nsolid.span_type": 16 },
          status: undefined,
        },
        {
          name: "child",
          kind: 3,
          root: false,
          attributes: { "thread.id": 0, "nsolid.span_type": 16 },
          status: { code: 2, message: "bad" },
        },
      ]);
      expect(child.traceId).toBe(parent.traceId);
      for (;;) {
        const request = await gateway.next("LogsExport");
        const records = request.resource_logs.flatMap((r: any) => r.scope_logs.flatMap((s: any) => s.log_records));
        const record = records.find((r: any) => hex(r.trace_id) === parent.traceId);
        if (!record) continue;
        expect([record.body.string_value, record.severity_text, hex(record.span_id)]).toEqual([
          '{"in":"parent"}',
          "WARN",
          parent.spanId,
        ]);
        break;
      }
    });
  });

  test.concurrent("counts the sockets, listeners and files a thread opens and closes", async () => {
    await using gateway = await grpcGateway([]);
    using dir = tempDir("nsolid-handles", {});
    const script = `
      const net = require("node:net");
      const dgram = require("node:dgram");
      const fs = require("node:fs");
      const listening = server => new Promise(resolve => server.once("listening", resolve));
      const connected = socket => new Promise(resolve => socket.once("connect", resolve));
      const accepted = [];
      const tcp = net.createServer(socket => accepted.push(socket)).listen(0, "127.0.0.1");
      const pipe = net.createServer(socket => accepted.push(socket)).listen(${JSON.stringify(join(String(dir), "s.sock"))});
      await Promise.all([listening(tcp), listening(pipe)]);
      const clients = [net.connect(tcp.address().port, "127.0.0.1"), net.connect(pipe.address())];
      await Promise.all(clients.map(connected));
      while (accepted.length < 2) await new Promise(resolve => setImmediate(resolve));
      const udp = dgram.createSocket("udp4");
      await new Promise(resolve => udp.bind(0, "127.0.0.1", resolve));
      const fd = fs.openSync(${JSON.stringify(join(String(dir), "file.txt"))}, "w");
      console.log("open");
      for await (const line of console) {
        fs.closeSync(fd);
        udp.close();
        for (const socket of [...clients, ...accepted]) socket.destroy();
        tcp.close();
        pipe.close();
        console.log("closed");
      }`;
    await using proc = spawnAgent(gateway, script, { NSOLID_INTERVAL: "100" });
    const nextLine = lineReader(proc.stdout);
    const counters = [
      "tcpServerCreatedCount",
      "tcpServerDestroyedCount",
      "tcpSocketCreatedCount",
      "tcpSocketDestroyedCount",
      "udpSocketCreatedCount",
      "udpSocketDestroyedCount",
      "pipeServerCreatedCount",
      "pipeServerDestroyedCount",
      "pipeSocketCreatedCount",
      "pipeSocketDestroyedCount",
      "fsHandlesOpenedCount",
      "fsHandlesClosedCount",
    ];
    const mainThread = (request: any, name: string) => {
      const metric = request.resource_metrics[0].scope_metrics[0].metrics.find((m: any) => m.name === name);
      const point = (metric.sum ?? metric.gauge).data_points.find((p: any) =>
        p.attributes.some((a: any) => a.key === "thread.id" && a.value.int_value === "0"),
      );
      return Number(point.as_int);
    };
    const countsWhen = async (done: (counts: Record<string, number>) => boolean) => {
      for (;;) {
        const request = await gateway.next("MetricsExport");
        const counts = Object.fromEntries([...counters, "activeHandles"].map(name => [name, mainThread(request, name)]));
        if (done(counts)) return counts;
      }
    };
    // A client, and the connection the server accepted, of each kind.
    const opened = {
      tcpServerCreatedCount: 1,
      tcpSocketCreatedCount: 2,
      udpSocketCreatedCount: 1,
      pipeServerCreatedCount: 1,
      pipeSocketCreatedCount: 2,
      fsHandlesOpenedCount: 1,
    };
    expect(await nextLine()).toBe("open");
    const open = await countsWhen(counts => counts.fsHandlesOpenedCount > 0 && counts.udpSocketCreatedCount > 0);
    const { activeHandles: activeWhileOpen, ...openCounts } = open;
    expect(openCounts).toEqual({
      ...Object.fromEntries(counters.map(name => [name, 0])),
      ...opened,
    });
    // The listeners, the sockets, the UDP socket and stdin hold the loop open.
    expect(activeWhileOpen).toBeGreaterThanOrEqual(7);

    proc.stdin.write("close\n");
    expect(await nextLine()).toBe("closed");
    const closed = await countsWhen(counts => counts.tcpSocketDestroyedCount === 2 && counts.fsHandlesClosedCount === 1);
    const { activeHandles: activeAfter, ...closedCounts } = closed;
    expect(closedCounts).toEqual({
      tcpServerCreatedCount: 1,
      tcpServerDestroyedCount: 1,
      tcpSocketCreatedCount: 2,
      tcpSocketDestroyedCount: 2,
      udpSocketCreatedCount: 1,
      udpSocketDestroyedCount: 1,
      pipeServerCreatedCount: 1,
      pipeServerDestroyedCount: 1,
      pipeSocketCreatedCount: 2,
      pipeSocketDestroyedCount: 2,
      fsHandlesOpenedCount: 1,
      fsHandlesClosedCount: 1,
    });
    expect(activeAfter).toBeLessThan(activeWhileOpen);
  });

  test.concurrent("answers source_code once the Console turns source reads on", async () => {
    await using gateway = await grpcGateway([]);
    const source = `function spin(ms) { const end = Date.now() + ms; while (Date.now() < end) {} }
for await (const line of console) { spin(800); console.log("done"); }
`;
    using dir = tempDir("nsolid-source", { "app.mjs": source });
    const file = join(realpathSync(String(dir)), "app.mjs");
    await using proc = Bun.spawn({
      cmd: [bunExe(), file],
      stdin: "pipe",
      stdout: "pipe",
      stderr: "inherit",
      env: {
        ...bunEnv,
        NSOLID_GRPC: `127.0.0.1:${gateway.port}`,
        NSOLID_GRPC_INSECURE: "1",
        NSOLID_BLOCKED_LOOP_THRESHOLD: "300",
      },
    });
    await gateway.commandMetadata;
    proc.stdin.write("block\n");
    const blocked = await gateway.next("ExportBlockedLoop");
    const frame = blocked.body.stack.find((f: any) => f.function_name === "spin");
    expect(frame.script_name).toBe(file);
    let requests = 0;
    const sourceCode = async (args: object) => {
      const requestId = `r-source-${++requests}`;
      await gateway.command({ requestId, id: "x", command: "source_code", args: { source_code: args } });
      const answer = await gateway.next("ExportSourceCode");
      expect(answer.common.requestId).toBe(requestId);
      return {
        error: answer.common.error,
        path: answer.path,
        code: answer.code?.length ? Buffer.from(answer.code).toString() : undefined,
        isEsm: answer.is_esm,
      };
    };
    const script = { thread_id: 0, script_id: frame.script_id };
    expect(await sourceCode({ ...script, path: file })).toEqual({
      error: { code: 500, message: "Source code reads disabled(1009)" },
      path: file,
      code: undefined,
      isEsm: undefined,
    });
    await gateway.command({
      requestId: "r-on",
      id: "x",
      command: "reconfigure",
      args: { reconfigure: { sourceCodeEnabled: true } },
    });
    expect((await gateway.next("ExportReconfigure")).body.sourceCodeEnabled).toBe(true);
    // By a stack's path, and by a profile's file URL, which is echoed as it came.
    const fileUrl = Bun.pathToFileURL(file).href;
    expect([await sourceCode({ ...script, path: file }), await sourceCode({ ...script, path: fileUrl })]).toEqual([
      { error: undefined, path: file, code: source, isEsm: true },
      { error: undefined, path: fileUrl, code: source, isEsm: true },
    ]);
    const runtimeError = { code: 500, message: "Internal Runtime Error(1007)" };
    expect([
      (await sourceCode({ ...script, path: join(String(dir), "other.mjs") })).error,
      (await sourceCode({ thread_id: 23, script_id: frame.script_id, path: file })).error,
    ]).toEqual([runtimeError, { code: 410, message: "Thread already gone(1002)" }]);
  });

  test.concurrent("profiles every thread continuously while contCpuProfile is on", async () => {
    await using gateway = await grpcGateway([
      {
        requestId: "r-profile",
        id: "x",
        command: "profile",
        args: { profile: { thread_id: 0, duration: 100, cpu_profile: {} } },
      },
    ]);
    const script = `function work() { let x = 0; for (let i = 0; i < 1e5; i++) x += i; return x; }
      setInterval(work, 5);`;
    await using proc = spawnAgent(gateway, script, {
      NSOLID_CONT_CPU_PROFILE: "1",
      NSOLID_CONT_CPU_PROFILE_INTERVAL: "200",
    });
    // Profiling continuously is a profile in progress.
    const refused = await gateway.next("ExportAsset");
    expect(refused.map((m: any) => m.common.error)).toEqual([
      { code: 409, message: "Operation already in progress(1001)" },
    ]);
    const windows = [await gateway.next("ExportContinuousProfile"), await gateway.next("ExportContinuousProfile")];
    for (const messages of windows) {
      const last = messages.at(-1);
      const requestIds = new Set(messages.map((m: any) => m.common.requestId));
      const profile = JSON.parse(Buffer.concat(messages.slice(0, -1).map((m: any) => m.data)).toString());
      expect({
        command: last.common.command,
        sameRequest: requestIds.size,
        thread: Number(last.thread_id),
        complete: last.complete,
        metadata: last.metadata,
        hasNodes: profile.nodes.length > 0,
      }).toEqual({
        command: "profile",
        sameRequest: 1,
        thread: 0,
        complete: true,
        metadata: {},
        hasNodes: true,
      });
      expect([...requestIds][0]).toMatch(/^[0-9a-f]{40}$/);
      expect(Number(last.duration)).toBeGreaterThan(0);
      expect(last.end_ts).toBeGreaterThan(last.start_ts);
      expect(Math.abs(last.start_ts - Date.now())).toBeLessThan(60_000);
    }
    expect(windows[1].at(-1).start_ts).toBeGreaterThanOrEqual(windows[0].at(-1).end_ts - 1);
  });

  test.concurrent("nsolid and bun:nsolid are the same module, with the process's id", async () => {
    await using proc = Bun.spawn({
      cmd: [
        bunExe(),
        "-e",
        `const a = require("nsolid"); const b = require("bun:nsolid");
         const m = await import("nsolid");
         console.log(JSON.stringify({ same: a === b && m.default === a, id: a.id }));`,
      ],
      env: bunEnv,
      stdout: "pipe",
      stderr: "inherit",
    });
    const [stdout, exitCode] = await Promise.all([proc.stdout.text(), proc.exited]);
    const result = JSON.parse(stdout);
    expect(result.same).toBe(true);
    expect(result.id).toMatch(/^[0-9a-f]{40}$/);
    expect(exitCode).toBe(0);
  });
});

describe("nsolid-bun", () => {
  test.concurrent("reports its own version beside Bun's", async () => {
    await using proc = Bun.spawn({ cmd: [bunExe(), "--version"], env: bunEnv, stdout: "pipe", stderr: "inherit" });
    const [stdout, exitCode] = await Promise.all([proc.stdout.text(), proc.exited]);
    expect(process.versions.nsolid_bun).toMatch(/^\d+\.\d+\.\d+$/);
    expect(stdout).toBe(`${Bun.version} (nsolid-bun ${process.versions.nsolid_bun})\n`);
    expect(exitCode).toBe(0);
  });

  test("names itself in the User-Agent", async () => {
    using server = Bun.serve({ port: 0, fetch: request => new Response(request.headers.get("user-agent")) });
    const userAgent = `Bun/${Bun.version} nsolid-bun/${process.versions.nsolid_bun}`;
    expect(await (await fetch(server.url)).text()).toBe(userAgent);
    expect(navigator.userAgent).toBe(userAgent);
  });

  test.concurrent("does not replace itself with Bun on upgrade", async () => {
    await using proc = Bun.spawn({ cmd: [bunExe(), "upgrade"], env: bunEnv, stdout: "pipe", stderr: "pipe" });
    const [stderr, exitCode] = await Promise.all([proc.stderr.text(), proc.exited]);
    expect(stderr).toContain("nsolid-bun does not upgrade itself");
    expect(stderr).toContain("https://github.com/nodesource/ns-bun/releases");
    expect(exitCode).toBe(1);
  });

  test.concurrent("prints a crash's trace with no bun.report link", async () => {
    const env: Record<string, string | undefined> = {
      ...bunEnv,
      BUN_FEATURE_FLAG_INTERNAL_FOR_TESTING: "1",
      BUN_ENABLE_CRASH_REPORTING: "0",
    };
    delete env.BUN_CRASH_REPORT_URL;
    await using proc = Bun.spawn({
      cmd: [
        bunExe(),
        join(import.meta.dir, "..", "..", "..", "cli", "run", "fixture-crash.js"),
        "panic",
        "--debug-crash-handler-use-trace-string",
      ],
      env,
      stdout: "ignore",
      stderr: "pipe",
    });
    const [stderr, exitCode] = await Promise.all([proc.stderr.text(), proc.exited]);
    expect(stderr).toContain("Please include this crash trace when you report it to NodeSource:");
    expect(stderr).not.toContain("bun.report");
    expect(exitCode).not.toBe(0);
  });
});
