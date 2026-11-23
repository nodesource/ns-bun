// The N|Solid agent. It runs on a thread of its own, in a VM of its own,
// started by src/jsc/nsolid_agent.rs when NSOLID_GRPC or NSOLID_SAAS is set,
// and talks to the N|Solid Console over gRPC (`grpcagent.NSolidService`).
//
// The Console drives the agent through the bidirectional `Command` stream: it
// sends a `CommandRequest` and expects an empty `CommandResponse` back at once,
// which it also uses to tell a live agent from a dead one. Replies to the
// commands themselves are separate unary RPCs.

const http2 = require("node:http2");

const COMMAND_PATH = "/grpcagent.NSolidService/Command";
const SAAS_KEY_LENGTH = 40;
const SAAS_CONSOLE_ID_LENGTH = 36;
const RETRY_MIN_MS = 1_000;
const RETRY_MAX_MS = 30_000;
const GRPC_HEADER_LENGTH = 5;
// An empty protobuf message, framed: not compressed, length 0.
const EMPTY_MESSAGE = new Uint8Array(GRPC_HEADER_LENGTH);

interface Endpoint {
  authority: string;
  secure: boolean;
  saasToken: string | undefined;
}

function isTrue(value: string | undefined): boolean {
  const v = (value ?? "").toLowerCase();
  return v === "1" || v === "true";
}

// The same rules as N|Solid's `GrpcAgent::parse_saas_token`: a 40-character
// key, then `<console id>.<host>:<port>`. Production and staging tokens dial
// NodeSource's endpoints; a "testing" token dials what follows its last dot.
function parseSaasToken(token: string): { authority: string; testing: boolean } | undefined {
  if (token.length <= SAAS_KEY_LENGTH) return;
  const [host = "", port = ""] = token.slice(SAAS_KEY_LENGTH).split(":");
  if (!host || !port) return;
  const dot = host.indexOf(".");
  const consoleId = dot === -1 ? host : host.slice(0, dot);
  if (consoleId.length !== SAAS_CONSOLE_ID_LENGTH) return;

  if (token.includes("staging")) return { authority: `${consoleId}.grpc.staging.nodesource.io:443`, testing: false };
  if (token.includes("testing")) {
    const lastDot = token.lastIndexOf(".");
    const authority = lastDot !== -1 && lastDot + 1 < token.length ? token.slice(lastDot + 1) : "localhost:50051";
    return { authority, testing: true };
  }
  return { authority: `${consoleId}.grpc.nodesource.io:443`, testing: false };
}

function resolveEndpoint(env: Record<string, string | undefined>): Endpoint | undefined {
  const insecure = isTrue(env.NSOLID_GRPC_INSECURE);
  const saasToken = env.NSOLID_SAAS;
  if (saasToken) {
    const saas = parseSaasToken(saasToken);
    // Plaintext is only for a testing token, never for NodeSource's endpoints.
    if (saas) return { authority: saas.authority, secure: !(saas.testing && insecure), saasToken };
  }
  const grpc = env.NSOLID_GRPC;
  if (grpc) {
    const authority = /^\d+$/.test(grpc) ? `localhost:${grpc}` : grpc;
    return { authority, secure: !insecure, saasToken: undefined };
  }
}

// Answers each complete gRPC message on the Command stream with an empty one.
// Decoding the `CommandRequest`s comes with the protobuf codec.
function ackCommands(stream) {
  let pending = Buffer.alloc(0);
  stream.on("data", (chunk: Buffer) => {
    pending = pending.length ? Buffer.concat([pending, chunk]) : chunk;
    while (pending.length >= GRPC_HEADER_LENGTH) {
      const length = pending.readUInt32BE(1);
      if (pending.length < GRPC_HEADER_LENGTH + length) break;
      pending = pending.subarray(GRPC_HEADER_LENGTH + length);
      stream.write(EMPTY_MESSAGE);
    }
  });
}

function connect(endpoint: Endpoint, agentId: string) {
  const headers: Record<string, string> = {
    ":method": "POST",
    ":path": COMMAND_PATH,
    "content-type": "application/grpc",
    "te": "trailers",
    "grpc-accept-encoding": "identity",
    "nsolid-agent-id": agentId,
    "nsolid-version": `bun-v${Bun.version}`,
  };
  if (endpoint.saasToken) headers["nsolid-saas"] = endpoint.saasToken;
  const url = `${endpoint.secure ? "https" : "http"}://${endpoint.authority}`;

  let attempt = 0;
  const open = () => {
    let retried = false;
    const retry = () => {
      if (retried) return;
      retried = true;
      const ceiling = Math.min(RETRY_MAX_MS, RETRY_MIN_MS * 2 ** attempt++);
      setTimeout(open, ceiling / 2 + Math.random() * (ceiling / 2));
    };

    const session = http2.connect(url);
    session.on("error", (err: Error) => $debug("session error:", err?.message));
    session.on("close", retry);

    const stream = session.request(headers, { endStream: false });
    stream.on("response", () => {
      attempt = 0;
    });
    stream.on("error", (err: Error) => $debug("command stream error:", err?.message));
    stream.on("close", () => session.close());
    ackCommands(stream);
  };
  open();
}

export default function start(agentId: string) {
  const endpoint = resolveEndpoint(process.env);
  if (!endpoint) {
    $debug("no usable NSOLID_GRPC or NSOLID_SAAS");
    return;
  }
  connect(endpoint, agentId);
}
