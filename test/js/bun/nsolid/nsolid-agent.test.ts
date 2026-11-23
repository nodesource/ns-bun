import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { describe, expect, test } from "bun:test";
import { bunEnv, bunExe, tempDir, tls } from "harness";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
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
// Command stream and lets `answer` respond to it.
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
