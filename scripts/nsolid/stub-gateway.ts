// A stand-in for the N|Solid Console's gateway, for measurements and test runs
// with the agent on: it holds each agent's Command stream open, accepts every
// other call, and counts the calls and their bytes by method.
//
//   bun scripts/nsolid/stub-gateway.ts [port]
//
// It prints the port it listens on (on 127.0.0.1, h2c: give agents
// NSOLID_GRPC_INSECURE=1), then answers each line on stdin with the counts
// since the last one, as JSON. It runs until it is killed.
import http2 from "node:http2";
import type { AddressInfo } from "node:net";

const calls: Record<string, { calls: number; bytes: number }> = {};
const server = http2.createServer();
server.on("session", session => session.on("error", () => {}));
server.on("stream", (stream, headers) => {
  stream.on("error", () => {});
  const method = String(headers[":path"]).slice(1);
  const count = (calls[method] ??= { calls: 0, bytes: 0 });
  count.calls++;
  stream.on("data", (chunk: Buffer) => (count.bytes += chunk.length));
  if (method.endsWith("/Command")) {
    stream.respond({ ":status": 200, "content-type": "application/grpc" });
    return;
  }
  stream.on("end", () => {
    stream.respond({ ":status": 200, "content-type": "application/grpc" }, { waitForTrailers: true });
    stream.on("wantTrailers", () => stream.sendTrailers({ "grpc-status": "0" }));
    // An empty message: every reply in the protocol is one.
    stream.end(Buffer.alloc(5));
  });
});

await new Promise<void>(done => server.listen(Number(process.argv[2] ?? 0), "127.0.0.1", done));
console.log((server.address() as AddressInfo).port);

for await (const _ of console) {
  console.log(JSON.stringify(calls));
  for (const method in calls) delete calls[method];
}
