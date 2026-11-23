// The N|Solid agent. It runs on a thread of its own, in a VM of its own,
// started by src/jsc/nsolid_agent.rs when NSOLID_GRPC or NSOLID_SAAS is set,
// and talks to the N|Solid Console over gRPC (`grpcagent.NSolidService`).
//
// The Console drives the agent through the bidirectional `Command` stream: it
// sends a `CommandRequest` and expects an empty `CommandResponse` back at once,
// which it also uses to tell a live agent from a dead one. Each command is
// then answered with the unary or client-streaming RPC that carries its
// result, echoing the request's id.

const fs = require("node:fs");
const http2 = require("node:http2");
const os = require("node:os");
const path = require("node:path");
const tls = require("node:tls");
const protobuf = require("internal/nsolid/protobuf");
const nsolidConfig = require("internal/nsolid/config");

const SERVICE = "grpcagent.NSolidService";
const SAAS_KEY_LENGTH = 40;
const SAAS_CONSOLE_ID_LENGTH = 36;
// N|Solid's reconnect rules (agents/grpc/src/grpc_agent.cc): the delay starts
// at half a second and doubles up to a minute; the 20th UNAUTHENTICATED close
// stops the agent. A Command stream that stayed up for a minute resets both.
// The Console's `retry-after` (seconds) lengthens a delay.
const RETRY_MIN_MS = 500;
const RETRY_MAX_MS = 60_000;
const MAX_AUTH_RETRIES = 20;
const MAX_RETRY_AFTER_S = 3600;
const DEFAULT_CLIENT_CERT_REFRESH_S = 60;
const GRPC_UNKNOWN = 2;
const GRPC_UNAVAILABLE = 14;
const GRPC_UNAUTHENTICATED = 16;
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

function warn(message: string) {
  process.stderr.write(message + "\n");
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

// gRPC's mapping of an HTTP status that isn't 200 to a status code.
function grpcStatusOfHttp(status: number): number {
  switch (status) {
    case 400:
      return 13;
    case 401:
      return GRPC_UNAUTHENTICATED;
    case 403:
      return 7;
    case 404:
      return 12;
    case 429:
    case 502:
    case 503:
    case 504:
      return GRPC_UNAVAILABLE;
    default:
      return GRPC_UNKNOWN;
  }
}

// NSOLID_GRPC_CERTS: a CA bundle that replaces the default roots. One that
// can't be read, or is empty, leaves the default roots.
function readCa(env: Record<string, string | undefined>): Buffer | undefined {
  const file = env.NSOLID_GRPC_CERTS;
  if (file === undefined) return;
  try {
    const ca = fs.readFileSync(file);
    if (ca.length) return ca;
  } catch (err) {
    $debug(`cannot read NSOLID_GRPC_CERTS (${file}):`, (err as Error)?.message);
  }
}

// NSOLID_GRPC_KEYLOG: the TLS secrets go to ./nsolid-tls-keylog-<pid>.log,
// which starts out empty.
function keylogFile(env: Record<string, string | undefined>): string | undefined {
  const value = env.NSOLID_GRPC_KEYLOG;
  if (!value || value === "0") return;
  const file = path.resolve(`nsolid-tls-keylog-${process.pid}.log`);
  try {
    fs.unlinkSync(file);
  } catch {}
  return file;
}

// NSOLID_GRPC_CLIENT_CERT and NSOLID_GRPC_CLIENT_KEY: the certificate the
// agent presents. Both files are read again every
// NSOLID_GRPC_CLIENT_CERT_REFRESH_INTERVAL seconds (60 by default), so a
// renewed certificate is used from the next connection on.
class ClientIdentity {
  cert: Buffer | undefined;
  key: Buffer | undefined;

  static fromEnv(env: Record<string, string | undefined>): ClientIdentity | undefined {
    const certFile = env.NSOLID_GRPC_CLIENT_CERT;
    const keyFile = env.NSOLID_GRPC_CLIENT_KEY;
    if (!certFile && !keyFile) return;
    if (!certFile || !keyFile) {
      const [set, unset] = certFile
        ? ["NSOLID_GRPC_CLIENT_CERT", "NSOLID_GRPC_CLIENT_KEY"]
        : ["NSOLID_GRPC_CLIENT_KEY", "NSOLID_GRPC_CLIENT_CERT"];
      warn(`N|Solid warning: ${set} is set without ${unset}, so the agent connects without a client certificate`);
      return;
    }
    const interval = env.NSOLID_GRPC_CLIENT_CERT_REFRESH_INTERVAL ?? "";
    const refreshS =
      /^\d+$/.test(interval) && +interval > 0 && +interval <= 0xffffffff ? +interval : DEFAULT_CLIENT_CERT_REFRESH_S;
    return new ClientIdentity(certFile, keyFile, refreshS);
  }

  constructor(
    public certFile: string,
    public keyFile: string,
    refreshS: number,
  ) {
    // Files that can't be read yet are not an error: they may appear later.
    const error = this.load();
    if (error) {
      warn(
        `N|Solid warning: the client certificate (${certFile}) or its key (${keyFile}) cannot be used yet: ${error}`,
      );
    }
    setInterval(() => this.load(), Math.min(refreshS * 1000, 2 ** 31 - 1)).unref();
  }

  // Reads both files; a pair that makes a TLS context replaces the last one.
  // Returns why the files' contents can't be used.
  load(): string | undefined {
    let cert: Buffer, key: Buffer;
    try {
      cert = fs.readFileSync(this.certFile);
      key = fs.readFileSync(this.keyFile);
    } catch {
      return;
    }
    try {
      tls.createSecureContext({ cert, key });
    } catch (err) {
      return (err as Error)?.message ?? String(err);
    }
    this.cert = cert;
    this.key = key;
  }
}

function appendKeylog(file: string, line: Buffer) {
  try {
    fs.appendFileSync(file, line, { mode: 0o600 });
  } catch (err) {
    $debug("cannot write the TLS keylog:", (err as Error)?.message);
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
  host: string;
  port: number;
  secure: boolean;
  ca: Buffer | undefined;
  identity: ClientIdentity | undefined;
  keylog: string | undefined;
  session: any = null;
  stopped = false;
  retryMinMs = RETRY_MIN_MS;
  retryMaxMs = RETRY_MAX_MS;
  retryInterval = RETRY_MIN_MS;
  authRetries = 0;

  constructor(
    endpoint: Endpoint,
    public agentId: string,
    mainPath: string,
    public marks: StartupMarks,
  ) {
    const env = process.env;
    this.config = nsolidConfig.createConfig(env, mainPath);
    this.secure = endpoint.secure;
    this.url = `${endpoint.secure ? "https" : "http"}://${endpoint.authority}`;
    const url = new URL(this.url);
    this.host = url.hostname.replace(/^\[(.*)\]$/, "$1");
    this.port = Number(url.port) || (endpoint.secure ? 443 : 80);
    if (endpoint.secure) {
      this.ca = readCa(env);
      this.identity = ClientIdentity.fromEnv(env);
      this.keylog = keylogFile(env);
    }
    // Tests reconnect at a fixed, short interval.
    const testingRetryMs = Number(env.BUN_NSOLID_RETRY_MS_FOR_TESTING);
    if (Number.isSafeInteger(testingRetryMs) && testingRetryMs > 0) {
      this.retryMinMs = this.retryMaxMs = this.retryInterval = testingRetryMs;
    }
    this.metadata = {
      "content-type": "application/grpc",
      "te": "trailers",
      "grpc-accept-encoding": "identity",
      "nsolid-agent-id": agentId,
      "nsolid-version": `bun-v${Bun.version}`,
    };
    if (endpoint.saasToken) this.metadata["nsolid-saas"] = endpoint.saasToken;
  }

  connectOptions() {
    if (!this.secure) return undefined;
    const tlsOptions: Record<string, any> = {};
    if (this.ca) tlsOptions.ca = this.ca;
    const { cert, key } = this.identity ?? {};
    if (cert && key) {
      tlsOptions.cert = cert;
      tlsOptions.key = key;
    }
    const keylog = this.keylog;
    if (!keylog) return tlsOptions;
    // The keylog is an event of the TLS socket, so the agent makes its own.
    return {
      createConnection: () => {
        const socket = tls.connect({ ...tlsOptions, host: this.host, port: this.port, ALPNProtocols: ["h2"] });
        socket.on("keylog", (line: Buffer) => appendKeylog(keylog, line));
        return socket;
      },
    };
  }

  connect() {
    if (this.stopped) return;
    let session;
    try {
      session = http2.connect(this.url, this.connectOptions());
    } catch (err) {
      $debug("cannot connect:", (err as Error)?.message);
      return this.commandStreamClosed(GRPC_UNAVAILABLE, 0, 0);
    }
    this.session = session;
    // The Command stream's grpc-status, the Console's retry-after, and when
    // the stream was accepted.
    let status: number | undefined;
    let retryAfterMs = 0;
    let establishedAt = 0;
    let closed = false;
    let stream;
    // The call is over once the gateway ends its side, with or without a
    // status: the agent's side is still open, and is cancelled.
    const onClosed = () => {
      if (closed) return;
      closed = true;
      if (this.session === session) this.session = null;
      if (stream && !stream.closed) stream.close(http2.constants.NGHTTP2_CANCEL);
      session.close();
      this.commandStreamClosed(status ?? GRPC_UNAVAILABLE, establishedAt, retryAfterMs);
    };
    const readStatus = (headers: Record<string, string>) => {
      if (headers["grpc-status"] !== undefined) status = Number(headers["grpc-status"]);
      const retryAfter = Number(headers["retry-after"]);
      if (retryAfter > 0) retryAfterMs = Math.min(retryAfter, MAX_RETRY_AFTER_S) * 1000;
    };

    session.on("error", (err: Error) => $debug("session error:", err?.message));
    session.on("close", onClosed);

    stream = session.request(
      { ":method": "POST", ":path": `/${SERVICE}/Command`, ...this.metadata },
      { endStream: false },
    );
    stream.on("response", (headers: Record<string, string>) => {
      const httpStatus = Number(headers[":status"]);
      if (httpStatus !== 200) status = grpcStatusOfHttp(httpStatus);
      readStatus(headers);
      if (status === undefined) establishedAt = performance.now();
    });
    stream.on("trailers", readStatus);
    stream.on("error", (err: Error) => $debug("command stream error:", err?.message));
    stream.on("end", onClosed);
    stream.on("close", onClosed);

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

  commandStreamClosed(code: number, establishedAt: number, retryAfterMs: number) {
    if (establishedAt && performance.now() - establishedAt >= RETRY_MAX_MS) {
      this.retryInterval = this.retryMinMs;
      this.authRetries = 0;
    }
    if (code === GRPC_UNAUTHENTICATED && ++this.authRetries === MAX_AUTH_RETRIES) {
      warn(`N|Solid warning: ${this.agentId} Unable to authenticate, please verify your token and network connection!`);
      this.stopped = true;
      return;
    }
    const interval = this.retryInterval;
    const delay = Math.max(interval - Math.floor(Math.random() * (Math.floor(interval / 2) + 1)), retryAfterMs);
    this.retryInterval = Math.min(interval * 2, this.retryMaxMs);
    $debug(`Command stream closed (${code}). Retrying in ${delay} ms`);
    setTimeout(() => this.connect(), delay);
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
