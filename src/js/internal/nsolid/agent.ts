// The N|Solid agent. It runs on a thread of its own, in a VM of its own,
// started by src/jsc/nsolid_agent.rs when NSOLID_GRPC or NSOLID_SAAS is set,
// and talks to the N|Solid Console over gRPC (`grpcagent.NSolidService`).
//
// The Console drives the agent through the bidirectional `Command` stream: it
// sends a `CommandRequest` and expects an empty `CommandResponse` back at once,
// which it also uses to tell a live agent from a dead one. Each command is
// then answered with the unary or client-streaming RPC that carries its
// result, echoing the request's id.

const http2 = require("node:http2");
const os = require("node:os");
const protobuf = require("internal/nsolid/protobuf");
const nsolidConfig = require("internal/nsolid/config");

const SERVICE = "grpcagent.NSolidService";
const SAAS_KEY_LENGTH = 40;
const SAAS_CONSOLE_ID_LENGTH = 36;
const RETRY_MIN_MS = 1_000;
const RETRY_MAX_MS = 30_000;
const EMPTY_FRAME = protobuf.frame(new Uint8Array(0));
// N|Solid's ENotAvailable: what the agent answers for what it can't provide.
const NOT_AVAILABLE = { code: 404, message: "Resource not available" };

// The RPC that carries each command's answer.
const replyRpc: Record<string, string> = {
  info: "ExportInfo",
  metrics: "ExportMetrics",
  packages: "ExportPackages",
  reconfigure: "ExportReconfigure",
  startup_times: "ExportStartupTimes",
  source_code: "ExportSourceCode",
  profile: "ExportAsset",
  heap_profile: "ExportAsset",
  heap_sampling: "ExportAsset",
  snapshot: "ExportAsset",
};

interface Endpoint {
  authority: string;
  secure: boolean;
  saasToken: string | undefined;
}

interface StartupMarks {
  mainEntryNs: number;
  agentThreadNs: number;
  agentVmNs: number;
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

function recorded() {
  const ms = Date.now();
  return { seconds: Math.floor(ms / 1000), nanoseconds: (ms % 1000) * 1_000_000 };
}

class Agent {
  config;
  metadata: Record<string, string>;
  url: string;
  session: any = null;
  attempt = 0;

  constructor(
    endpoint: Endpoint,
    public agentId: string,
    mainPath: string,
    public marks: StartupMarks,
  ) {
    this.config = nsolidConfig.createConfig(process.env, mainPath);
    this.url = `${endpoint.secure ? "https" : "http"}://${endpoint.authority}`;
    this.metadata = {
      "content-type": "application/grpc",
      "te": "trailers",
      "grpc-accept-encoding": "identity",
      "nsolid-agent-id": agentId,
      "nsolid-version": `bun-v${Bun.version}`,
    };
    const { saasToken } = endpoint;
    if (saasToken) this.metadata["nsolid-saas"] = saasToken;
  }

  connect() {
    let retried = false;
    const retry = () => {
      if (retried) return;
      retried = true;
      this.session = null;
      const ceiling = Math.min(RETRY_MAX_MS, RETRY_MIN_MS * 2 ** this.attempt++);
      setTimeout(() => this.connect(), ceiling / 2 + Math.random() * (ceiling / 2));
    };

    const session = http2.connect(this.url);
    this.session = session;
    session.on("error", (err: Error) => $debug("session error:", err?.message));
    session.on("close", retry);

    const stream = session.request(
      { ":method": "POST", ":path": `/${SERVICE}/Command`, ...this.metadata },
      { endStream: false },
    );
    stream.on("response", () => {
      this.attempt = 0;
    });
    stream.on("error", (err: Error) => $debug("command stream error:", err?.message));
    stream.on("close", () => session.close());

    const frames = new protobuf.FrameReader();
    stream.on("data", (chunk: Uint8Array) => {
      for (const message of frames.push(chunk)) {
        stream.write(EMPTY_FRAME);
        let request;
        try {
          request = protobuf.decode("grpcagent.CommandRequest", message);
        } catch (err) {
          $debug("undecodable command:", (err as Error)?.message);
          continue;
        }
        this.handle(request).catch((err: Error) => $debug(`command ${request.command} failed:`, err?.message));
      }
    });
  }

  // Sends one message on a unary or client-streaming RPC of the service, and
  // waits for the call to end.
  send(rpc: string, message: object): Promise<void> {
    const session = this.session;
    if (!session || session.closed || session.destroyed) return Promise.resolve();
    const [type] = protobuf.services[SERVICE][rpc];
    const { promise, resolve } = Promise.withResolvers<void>();
    let status: string | undefined;
    const stream = session.request({ ":method": "POST", ":path": `/${SERVICE}/${rpc}`, ...this.metadata });
    stream.on("response", (headers: Record<string, string>) => {
      status = headers["grpc-status"] ?? status;
    });
    stream.on("trailers", (trailers: Record<string, string>) => {
      status = trailers["grpc-status"] ?? status;
    });
    stream.on("data", () => {});
    stream.on("error", (err: Error) => $debug(`${rpc} error:`, err?.message));
    stream.on("close", () => {
      if (status !== undefined && status !== "0") $debug(`${rpc} answered grpc-status ${status}`);
      resolve();
    });
    stream.end(protobuf.frame(protobuf.encode(type, message)));
    return promise;
  }

  common(request, error?: { code: number; message: string }) {
    return { requestId: request.requestId, command: request.command, recorded: recorded(), error };
  }

  async handle(request) {
    const command = request.command;
    const rpc = replyRpc[command];
    if (!rpc) {
      $debug("unknown command:", command);
      return;
    }
    switch (command) {
      case "info":
        return this.send(rpc, { common: this.common(request), body: this.info() });
      case "startup_times":
        return this.send(rpc, { common: this.common(request), body: this.startupTimes() });
      case "reconfigure":
        nsolidConfig.applyReconfigure(this.config, request.args?.reconfigure);
        return this.send(rpc, { common: this.common(request), body: nsolidConfig.reconfigureBody(this.config) });
      default: {
        const message: Record<string, any> = { common: this.common(request, NOT_AVAILABLE) };
        if (rpc === "ExportAsset") message.thread_id = request.args?.profile?.thread_id ?? 0;
        return this.send(rpc, message);
      }
    }
  }

  info() {
    const cpus = os.cpus();
    // The runtime is Bun: the Console reads versions.node as "this is Node.js",
    // and Bun's v8 entry is only what it reports for compatibility.
    const versions: Record<string, string> = {};
    for (const [name, version] of Object.entries(process.versions)) {
      if (name !== "node" && name !== "v8" && typeof version === "string") versions[name] = version;
    }
    return {
      app: this.config.app,
      appVersion: this.config.appVersion,
      arch: process.arch,
      cpuCores: cpus.length,
      cpuModel: cpus[0]?.model ?? "",
      execPath: process.execPath,
      hostname: this.config.hostname,
      id: this.agentId,
      main: this.config.main,
      nodeEnv: this.config.nodeEnv,
      pid: process.pid,
      platform: process.platform,
      processStart: Math.round(Date.now() - process.uptime() * 1000),
      tags: this.config.tags,
      totalMem: os.totalmem(),
      versions,
    };
  }

  // Nanoseconds since the process started; the Console measures each mark
  // from `timeOrigin`.
  startupTimes() {
    return {
      timeOrigin: 0,
      main_module_start: Math.round(this.marks.mainEntryNs),
      nsolid_agent_thread_start: Math.round(this.marks.agentThreadNs),
      nsolid_agent_ready: Math.round(this.marks.agentVmNs),
    };
  }
}

export default function start(
  agentId: string,
  mainPath: string,
  mainEntryNs: number,
  agentThreadNs: number,
  agentVmNs: number,
) {
  const endpoint = resolveEndpoint(process.env);
  if (!endpoint) {
    $debug("no usable NSOLID_GRPC or NSOLID_SAAS");
    return;
  }
  new Agent(endpoint, agentId, mainPath, { mainEntryNs, agentThreadNs, agentVmNs }).connect();
}
