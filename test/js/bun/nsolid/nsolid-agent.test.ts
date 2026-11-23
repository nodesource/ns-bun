import { describe, expect, test } from "bun:test";
import { bunEnv, bunExe } from "harness";
import http2 from "node:http2";
import type { AddressInfo } from "node:net";

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
});
