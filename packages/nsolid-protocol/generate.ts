// Turns the .proto files in ./proto into the schema tables that the N|Solid
// agent's protobuf codec reads (src/js/internal/nsolid/messages.ts).
//
//   bun packages/nsolid-protocol/generate.ts           writes the file
//   bun packages/nsolid-protocol/generate.ts --check   exits 1 if it is stale
//
// It reads the subset of proto3 these files use: packages, imports, messages
// (nested too), enums, oneofs, maps, `optional`, `repeated`, `reserved` and
// services. Options are ignored.

import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import * as prettier from "prettier";

const here = import.meta.dirname;
const protoDir = join(here, "proto");
const outFile = join(here, "..", "..", "src", "js", "internal", "nsolid", "messages.ts");
// The agent's service, and OTLP's metrics service on the same channel.
const roots = ["nsolid_service.proto", "opentelemetry/proto/collector/metrics/v1/metrics_service.proto"];

const scalars = new Set([
  "double",
  "float",
  "int32",
  "int64",
  "uint32",
  "uint64",
  "sint32",
  "sint64",
  "fixed32",
  "fixed64",
  "sfixed32",
  "sfixed64",
  "bool",
  "string",
  "bytes",
]);

interface Field {
  number: number;
  name: string;
  type: string;
  repeated: boolean;
  optional: boolean;
  keyType?: string;
}
interface Message {
  fullName: string;
  scope: string;
  fields: Field[];
}
interface Rpc {
  name: string;
  request: string;
  response: string;
  clientStreaming: boolean;
  serverStreaming: boolean;
}

const messages = new Map<string, Message>();
const enums = new Map<string, Record<string, number>>();
const services = new Map<string, { scope: string; rpcs: Rpc[] }>();

function tokenize(text: string): string[] {
  const noComments = text.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/[^\n]*/g, "");
  return noComments.match(/"[^"]*"|[A-Za-z_][\w.]*|\d+|[{}<>=;,()[\]-]/g) ?? [];
}

function parseFile(file: string, seen: Set<string>) {
  if (seen.has(file)) return;
  seen.add(file);
  const tokens = tokenize(readFileSync(join(protoDir, file), "utf8"));
  let i = 0;
  let pkg = "";
  const next = () => tokens[i++];
  const expect = (t: string) => {
    const got = next();
    if (got !== t) throw new Error(`${file}: expected ${t}, got ${got}`);
  };
  const skipStatement = () => {
    while (tokens[i] !== ";") i++;
    i++;
  };

  function parseEnum(scope: string) {
    const name = next();
    expect("{");
    const values: Record<string, number> = {};
    while (tokens[i] !== "}") {
      if (tokens[i] === "option" || tokens[i] === "reserved") {
        skipStatement();
        continue;
      }
      const key = next();
      expect("=");
      const negative = tokens[i] === "-" ? (i++, -1) : 1;
      values[key] = negative * Number(next());
      skipStatement();
    }
    expect("}");
    enums.set(`${scope}.${name}`, values);
  }

  function parseField(message: Message, label: string | undefined) {
    let type = next();
    let keyType: string | undefined;
    if (type === "map") {
      expect("<");
      keyType = next();
      expect(",");
      type = next();
      expect(">");
    }
    const name = next();
    expect("=");
    const number = Number(next());
    skipStatement();
    message.fields.push({
      number,
      name,
      type,
      repeated: label === "repeated",
      optional: label === "optional",
      keyType,
    });
  }

  function parseMessage(scope: string) {
    const name = next();
    const message: Message = { fullName: `${scope}.${name}`, scope: `${scope}.${name}`, fields: [] };
    messages.set(message.fullName, message);
    expect("{");
    while (tokens[i] !== "}") {
      const t = tokens[i];
      if (t === "message") {
        i++;
        parseMessage(message.fullName);
      } else if (t === "enum") {
        i++;
        parseEnum(message.fullName);
      } else if (t === "reserved" || t === "option") {
        skipStatement();
      } else if (t === "oneof") {
        i++;
        next();
        expect("{");
        while (tokens[i] !== "}") parseField(message, "optional");
        expect("}");
      } else if (t === "repeated" || t === "optional") {
        i++;
        parseField(message, t);
      } else {
        parseField(message, undefined);
      }
    }
    expect("}");
  }

  function parseService(scope: string) {
    const name = next();
    const rpcs: Rpc[] = [];
    expect("{");
    while (tokens[i] !== "}") {
      if (tokens[i] === "option") {
        skipStatement();
        continue;
      }
      expect("rpc");
      const rpcName = next();
      expect("(");
      const clientStreaming = tokens[i] === "stream" ? (i++, true) : false;
      const request = next();
      expect(")");
      expect("returns");
      expect("(");
      const serverStreaming = tokens[i] === "stream" ? (i++, true) : false;
      const response = next();
      expect(")");
      if (tokens[i] === "{") {
        i++;
        expect("}");
      } else expect(";");
      rpcs.push({ name: rpcName, request, response, clientStreaming, serverStreaming });
    }
    expect("}");
    services.set(`${scope}.${name}`, { scope, rpcs });
  }

  const imports: string[] = [];
  while (i < tokens.length) {
    const t = next();
    if (t === "syntax" || t === "option") skipStatement();
    else if (t === "package") {
      pkg = next();
      expect(";");
    } else if (t === "import") {
      if (tokens[i] === "public" || tokens[i] === "weak") i++;
      imports.push(next().slice(1, -1));
      expect(";");
    } else if (t === "message") parseMessage(pkg);
    else if (t === "enum") parseEnum(pkg);
    else if (t === "service") parseService(pkg);
    else throw new Error(`${file}: unexpected ${t}`);
  }
  for (const imported of imports) parseFile(imported, seen);
}

// Resolves a type name the way protoc does: from the innermost scope outwards.
function resolve(type: string, scope: string): string {
  if (scalars.has(type)) return type;
  if (type.startsWith(".")) return type.slice(1);
  const parts = scope.split(".");
  for (let n = parts.length; n >= 0; n--) {
    const candidate = [...parts.slice(0, n), type].filter(Boolean).join(".");
    if (messages.has(candidate) || enums.has(candidate)) return candidate;
  }
  throw new Error(`unknown type ${type} in ${scope}`);
}

function generate(): string {
  const seen = new Set<string>();
  for (const root of roots) parseFile(root, seen);

  const lines: string[] = [];
  lines.push(`// Generated by packages/nsolid-protocol/generate.ts from packages/nsolid-protocol/proto. Do not edit.`);
  lines.push(`// Fields: [number, name, type, flags, mapKeyType?]; flags: 1 repeated, 2 optional, 4 map. Types are`);
  lines.push(`// scalar names, "enum", or the full name of a message.`);
  lines.push(``);
  lines.push(`export default {`);
  lines.push(`  messages: {`);
  for (const name of [...messages.keys()].sort()) {
    const message = messages.get(name)!;
    const fields = message.fields
      .slice()
      .sort((a, b) => a.number - b.number)
      .map(f => {
        const resolved = resolve(f.type, message.scope);
        const type = enums.has(resolved) ? "enum" : resolved;
        const flags = (f.repeated ? 1 : 0) | (f.optional ? 2 : 0) | (f.keyType ? 4 : 0);
        const parts = [f.number, JSON.stringify(f.name), JSON.stringify(type), flags];
        if (f.keyType) parts.push(JSON.stringify(f.keyType));
        return `[${parts.join(", ")}]`;
      });
    lines.push(`    ${JSON.stringify(name)}: [${fields.join(", ")}],`);
  }
  lines.push(`  },`);
  lines.push(`  enums: {`);
  for (const name of [...enums.keys()].sort())
    lines.push(`    ${JSON.stringify(name)}: ${JSON.stringify(enums.get(name))},`);
  lines.push(`  },`);
  lines.push(`  services: {`);
  for (const name of [...services.keys()].sort()) {
    const { scope, rpcs } = services.get(name)!;
    lines.push(`    ${JSON.stringify(name)}: {`);
    for (const rpc of rpcs) {
      const value = [
        JSON.stringify(resolve(rpc.request, scope)),
        JSON.stringify(resolve(rpc.response, scope)),
        rpc.clientStreaming,
        rpc.serverStreaming,
      ];
      lines.push(`      ${rpc.name}: [${value.join(", ")}],`);
    }
    lines.push(`    },`);
  }
  lines.push(`  },`);
  lines.push(`};`);
  return lines.join("\n") + "\n";
}

// Formatted as the repository formats every file, so a format check passes on it.
const text = await prettier.format(generate(), { ...(await prettier.resolveConfig(outFile)), filepath: outFile });
if (process.argv.includes("--check")) {
  let current = "";
  try {
    current = readFileSync(outFile, "utf8");
  } catch {}
  if (current !== text) {
    console.error(`${relative(process.cwd(), outFile)} is stale: run bun ${relative(process.cwd(), import.meta.path)}`);
    process.exit(1);
  }
} else {
  writeFileSync(outFile, text);
  console.log(`wrote ${relative(dirname(here), outFile)}`);
}
