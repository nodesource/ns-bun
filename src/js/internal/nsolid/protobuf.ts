// Protocol Buffers encoding for the N|Solid agent, driven by the schema tables
// in internal/nsolid/messages (generated from packages/nsolid-protocol/proto).
//
// Messages are plain objects keyed by the proto field names. 64-bit integers
// decode to numbers when they fit in a double exactly, otherwise to BigInts;
// either encodes. Repeated fields decode to arrays and maps to objects.

const schema = require("internal/nsolid/messages");

type Field = [number, string, string, number, string?];

const REPEATED = 1;
const MAP = 4;
const GRPC_HEADER_LENGTH = 5;

const VARINT = 0;
const FIXED64 = 1;
const LENGTH_DELIMITED = 2;
const FIXED32 = 5;

const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder();

function wireType(type: string): number {
  switch (type) {
    case "double":
    case "fixed64":
    case "sfixed64":
      return FIXED64;
    case "float":
    case "fixed32":
    case "sfixed32":
      return FIXED32;
    case "string":
    case "bytes":
      return LENGTH_DELIMITED;
    case "int32":
    case "int64":
    case "uint32":
    case "uint64":
    case "sint32":
    case "sint64":
    case "bool":
    case "enum":
      return VARINT;
    default:
      return LENGTH_DELIMITED;
  }
}

function fieldsOf(type: string): Field[] {
  const fields = schema.messages[type];
  if (!fields) throw new Error(`nsolid: unknown message type ${type}`);
  return fields;
}

class Writer {
  buf = new Uint8Array(256);
  view = new DataView(this.buf.buffer);
  len = 0;

  reserve(n: number) {
    if (this.len + n <= this.buf.length) return;
    let size = this.buf.length * 2;
    while (size < this.len + n) size *= 2;
    const next = new Uint8Array(size);
    next.set(this.buf.subarray(0, this.len));
    this.buf = next;
    this.view = new DataView(next.buffer);
  }

  varint(value: number | bigint) {
    this.reserve(10);
    if (typeof value === "number" && value >= 0 && value <= 0xffffffff) {
      while (value > 0x7f) {
        this.buf[this.len++] = (value & 0x7f) | 0x80;
        value = value >>> 7;
      }
      this.buf[this.len++] = value;
      return;
    }
    let big = BigInt.asUintN(64, BigInt(value));
    while (big > 0x7fn) {
      this.buf[this.len++] = Number(big & 0x7fn) | 0x80;
      big >>= 7n;
    }
    this.buf[this.len++] = Number(big);
  }

  tag(number: number, wire: number) {
    this.varint(number * 8 + wire);
  }

  bytes(value: Uint8Array) {
    this.varint(value.length);
    this.reserve(value.length);
    this.buf.set(value, this.len);
    this.len += value.length;
  }

  fixed64(value: number | bigint, type: string) {
    this.reserve(8);
    if (type === "double") this.view.setFloat64(this.len, Number(value), true);
    else this.view.setBigUint64(this.len, BigInt.asUintN(64, BigInt(value)), true);
    this.len += 8;
  }

  fixed32(value: number, type: string) {
    this.reserve(4);
    if (type === "float") this.view.setFloat32(this.len, value, true);
    else if (type === "sfixed32") this.view.setInt32(this.len, value, true);
    else this.view.setUint32(this.len, value, true);
    this.len += 4;
  }

  finish(): Uint8Array {
    return this.buf.slice(0, this.len);
  }
}

function writeScalar(w: Writer, type: string, value: any) {
  switch (type) {
    case "string":
      w.bytes(utf8Encoder.encode(String(value)));
      return;
    case "bytes":
      w.bytes(typeof value === "string" ? utf8Encoder.encode(value) : value);
      return;
    case "bool":
      w.varint(value ? 1 : 0);
      return;
    case "sint32":
    case "sint64": {
      const big = BigInt(value);
      w.varint(big >= 0n ? big * 2n : -big * 2n - 1n);
      return;
    }
    case "double":
    case "fixed64":
    case "sfixed64":
      w.fixed64(value, type);
      return;
    case "float":
    case "fixed32":
    case "sfixed32":
      w.fixed32(Number(value), type);
      return;
    default:
      w.varint(value);
  }
}

function writeMessage(w: Writer, type: string, value: any) {
  for (const [number, name, fieldType, flags, keyType] of fieldsOf(type)) {
    const v = value[name];
    if (v === undefined || v === null) continue;
    if (flags & MAP) {
      for (const key of Object.keys(v)) {
        const entry = new Writer();
        entry.tag(1, wireType(keyType!));
        writeScalar(entry, keyType!, key);
        writeField(entry, 2, fieldType, v[key]);
        w.tag(number, LENGTH_DELIMITED);
        w.bytes(entry.finish());
      }
    } else if (flags & REPEATED) {
      const wire = wireType(fieldType);
      if (wire === LENGTH_DELIMITED) {
        for (const item of v) writeField(w, number, fieldType, item);
      } else if (v.length) {
        const packed = new Writer();
        for (const item of v) writeScalar(packed, fieldType, item);
        w.tag(number, LENGTH_DELIMITED);
        w.bytes(packed.finish());
      }
    } else {
      writeField(w, number, fieldType, v);
    }
  }
}

function writeField(w: Writer, number: number, type: string, value: any) {
  if (schema.messages[type]) {
    w.tag(number, LENGTH_DELIMITED);
    w.bytes(encode(type, value));
    return;
  }
  w.tag(number, wireType(type));
  writeScalar(w, type, value);
}

function encode(type: string, value: object): Uint8Array {
  const w = new Writer();
  writeMessage(w, type, value);
  return w.finish();
}

class Reader {
  pos: number;
  end: number;
  view: DataView;
  constructor(
    public buf: Uint8Array,
    start = 0,
    end = buf.length,
  ) {
    this.pos = start;
    this.end = end;
    this.view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  }

  varint(): bigint | number {
    let result = 0;
    let shift = 0;
    while (shift < 28) {
      if (this.pos >= this.end) throw new Error("nsolid: truncated varint");
      const byte = this.buf[this.pos++];
      result |= (byte & 0x7f) << shift;
      if (byte < 0x80) return result >>> 0;
      shift += 7;
    }
    let big = BigInt(result >>> 0);
    let bigShift = 28n;
    for (;;) {
      if (this.pos >= this.end) throw new Error("nsolid: truncated varint");
      const byte = this.buf[this.pos++];
      big |= BigInt(byte & 0x7f) << bigShift;
      if (byte < 0x80) return big;
      bigShift += 7n;
      if (bigShift > 70n) throw new Error("nsolid: varint too long");
    }
  }

  length(): number {
    const n = Number(this.varint());
    if (this.pos + n > this.end) throw new Error("nsolid: truncated field");
    return n;
  }

  skip(wire: number) {
    if (wire === VARINT) this.varint();
    else if (wire === FIXED64) this.pos += 8;
    else if (wire === LENGTH_DELIMITED) this.pos += this.length();
    else if (wire === FIXED32) this.pos += 4;
    else throw new Error(`nsolid: unsupported wire type ${wire}`);
  }
}

function toNumberIfSafe(value: bigint): number | bigint {
  return value <= BigInt(Number.MAX_SAFE_INTEGER) && value >= BigInt(Number.MIN_SAFE_INTEGER) ? Number(value) : value;
}

function readScalar(r: Reader, type: string): any {
  switch (type) {
    case "string": {
      const n = r.length();
      const s = utf8Decoder.decode(r.buf.subarray(r.pos, r.pos + n));
      r.pos += n;
      return s;
    }
    case "bytes": {
      const n = r.length();
      const b = r.buf.slice(r.pos, r.pos + n);
      r.pos += n;
      return b;
    }
    case "bool":
      return r.varint() != 0;
    case "double": {
      const v = r.view.getFloat64(r.pos, true);
      r.pos += 8;
      return v;
    }
    case "float": {
      const v = r.view.getFloat32(r.pos, true);
      r.pos += 4;
      return v;
    }
    case "fixed64":
    case "sfixed64": {
      const raw = r.view.getBigUint64(r.pos, true);
      r.pos += 8;
      return toNumberIfSafe(type === "sfixed64" ? BigInt.asIntN(64, raw) : raw);
    }
    case "fixed32": {
      const v = r.view.getUint32(r.pos, true);
      r.pos += 4;
      return v;
    }
    case "sfixed32": {
      const v = r.view.getInt32(r.pos, true);
      r.pos += 4;
      return v;
    }
    case "sint32":
    case "sint64": {
      const raw = BigInt(r.varint());
      return toNumberIfSafe(raw & 1n ? -(raw >> 1n) - 1n : raw >> 1n);
    }
    case "int32":
    case "enum":
      return Number(BigInt.asIntN(32, BigInt(r.varint())));
    case "int64":
      return toNumberIfSafe(BigInt.asIntN(64, BigInt(r.varint())));
    case "uint32":
      return Number(BigInt.asUintN(32, BigInt(r.varint())));
    default: {
      const v = r.varint();
      return typeof v === "number" ? v : toNumberIfSafe(v);
    }
  }
}

function readValue(r: Reader, type: string): any {
  if (schema.messages[type]) {
    const n = r.length();
    const value = decodeRange(type, r.buf, r.pos, r.pos + n);
    r.pos += n;
    return value;
  }
  return readScalar(r, type);
}

function decodeRange(type: string, buf: Uint8Array, start: number, end: number): any {
  const fields = fieldsOf(type);
  const out: any = {};
  for (const [, name, , flags] of fields) {
    if (flags & MAP) out[name] = {};
    else if (flags & REPEATED) out[name] = [];
  }
  const r = new Reader(buf, start, end);
  while (r.pos < r.end) {
    const key = Number(r.varint());
    const number = Math.floor(key / 8);
    const wire = key & 7;
    const field = fields.find(f => f[0] === number);
    if (!field) {
      r.skip(wire);
      continue;
    }
    const [, name, fieldType, flags, keyType] = field;
    if (flags & MAP) {
      const n = r.length();
      const entry = new Reader(buf, r.pos, r.pos + n);
      r.pos += n;
      let k: any = "";
      let v: any = undefined;
      while (entry.pos < entry.end) {
        const entryKey = Number(entry.varint());
        if (entryKey >> 3 === 1) k = readScalar(entry, keyType!);
        else if (entryKey >> 3 === 2) v = readValue(entry, fieldType);
        else entry.skip(entryKey & 7);
      }
      out[name][String(k)] = v;
    } else if (flags & REPEATED) {
      if (wire === LENGTH_DELIMITED && wireType(fieldType) !== LENGTH_DELIMITED) {
        const n = r.length();
        const packed = new Reader(buf, r.pos, r.pos + n);
        r.pos += n;
        while (packed.pos < packed.end) out[name].push(readScalar(packed, fieldType));
      } else {
        out[name].push(readValue(r, fieldType));
      }
    } else {
      out[name] = readValue(r, fieldType);
    }
  }
  return out;
}

function decode(type: string, bytes: Uint8Array): any {
  return decodeRange(type, bytes, 0, bytes.length);
}

// gRPC's length-prefixed message: one byte for compression (always off here),
// then the length as a 32-bit big-endian number.
function frame(message: Uint8Array): Uint8Array {
  const out = new Uint8Array(GRPC_HEADER_LENGTH + message.length);
  new DataView(out.buffer).setUint32(1, message.length);
  out.set(message, GRPC_HEADER_LENGTH);
  return out;
}

// Splits a stream of gRPC frames into messages, whatever the chunking.
class FrameReader {
  pending = new Uint8Array(0);

  push(chunk: Uint8Array): Uint8Array[] {
    if (this.pending.length) {
      const joined = new Uint8Array(this.pending.length + chunk.length);
      joined.set(this.pending);
      joined.set(chunk, this.pending.length);
      chunk = joined;
    }
    const messages: Uint8Array[] = [];
    let pos = 0;
    while (chunk.length - pos >= GRPC_HEADER_LENGTH) {
      const length = new DataView(chunk.buffer, chunk.byteOffset + pos + 1, 4).getUint32(0);
      if (chunk.length - pos < GRPC_HEADER_LENGTH + length) break;
      messages.push(chunk.slice(pos + GRPC_HEADER_LENGTH, pos + GRPC_HEADER_LENGTH + length));
      pos += GRPC_HEADER_LENGTH + length;
    }
    this.pending = chunk.slice(pos);
    return messages;
  }
}

export default { encode, decode, frame, FrameReader, services: schema.services, enums: schema.enums };
