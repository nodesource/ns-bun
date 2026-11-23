import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { describe, expect, test } from "bun:test";
import { bunEnv, bunExe, tempDir } from "harness";
import http2 from "node:http2";
import type { AddressInfo } from "node:net";
import { join } from "node:path";

const COMMAND_PATH = "/grpcagent.NSolidService/Command";
// Keeps the child alive for the test, and lets it exit if the test runner dies first.
const UNTIL_STDIN_CLOSES = "for await (const _ of console) {}";

// Stands in for the Console's gateway: answers the first Command stream it is
// offered and hands it to the test.
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
const { grpcagent } = grpc.loadPackageDefinition(
  protoLoader.loadSync("nsolid_service.proto", {
    includeDirs: [protoDir],
    keepCase: true,
    longs: String,
    enums: Number,
  }),
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
  const unary = (rpc: string) => (call: any, callback: any) => {
    record(rpc, call.request);
    callback(null, {});
  };

  const server = new grpc.Server();
  server.addService(grpcagent.NSolidService.service, {
    Command(call: any) {
      gotCommandStream(call.metadata);
      call.on("data", () => acks++);
      call.on("error", () => {});
      for (const command of commands) call.write(command);
    },
    ExportAsset(call: any, callback: any) {
      const messages: any[] = [];
      call.on("data", (m: any) => messages.push(m));
      call.on("end", () => {
        record("ExportAsset", messages);
        callback(null, {});
      });
    },
    ExportInfo: unary("ExportInfo"),
    ExportMetrics: unary("ExportMetrics"),
    ExportPackages: unary("ExportPackages"),
    ExportReconfigure: unary("ExportReconfigure"),
    ExportSourceCode: unary("ExportSourceCode"),
    ExportStartupTimes: unary("ExportStartupTimes"),
  });
  const port = await new Promise<number>((resolve, reject) =>
    server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (err, port) =>
      err ? reject(err) : resolve(port),
    ),
  );
  return {
    port,
    commandMetadata,
    acks: () => acks,
    next(rpc: string): Promise<any> {
      if (received[rpc]?.length) return Promise.resolve(received[rpc].shift());
      return new Promise(resolve => (waiting[rpc] ??= []).push(resolve));
    },
    [Symbol.asyncDispose]: async () => {
      server.forceShutdown();
    },
  };
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
    expect(headers["nsolid-version"]).toStartWith("bun-v");
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
            tags: ["x"],
            redactSnapshots: true,
            ebpfProfiling: 1,
            sourceCodeEnabled: true,
          },
        },
      },
      {
        requestId: "r-profile",
        id: "x",
        command: "profile",
        args: { profile: { thread_id: 0, duration: 1000, cpu_profile: {} } },
      },
      { requestId: "r-packages", id: "x", command: "packages" },
      { requestId: "r-metrics", id: "x", command: "metrics" },
      { requestId: "r-source", id: "x", command: "source_code", args: { source_code: { thread_id: 0, script_id: 1 } } },
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
        NSOLID_TAGS: "b, a",
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
    const { versions, cpuCores, cpuModel, totalMem, processStart, execPath, kernelVersion, ...body } = info.body;
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
      tags: ["a", "b"],
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
      tags: ["x"],
      traceSampleRate: 1,
      tracingEnabled: false,
      tracingModulesBlacklist: 0,
    });

    const notAvailable = { code: 404, message: "Resource not available" };
    const [asset, packages, metrics, source] = await Promise.all([
      gateway.next("ExportAsset"),
      gateway.next("ExportPackages"),
      gateway.next("ExportMetrics"),
      gateway.next("ExportSourceCode"),
    ]);
    expect(asset.map((m: any) => [m.common.requestId, m.common.error])).toEqual([["r-profile", notAvailable]]);
    expect([packages, metrics, source].map(m => [m.common.requestId, m.common.error])).toEqual([
      ["r-packages", notAvailable],
      ["r-metrics", notAvailable],
      ["r-source", notAvailable],
    ]);
    expect(gateway.acks()).toBe(7);
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
