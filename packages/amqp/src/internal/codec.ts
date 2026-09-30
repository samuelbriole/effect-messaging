import { AMQPProtocolError } from "../AMQPError.ts"
import type * as AMQPTypes from "../AMQPTypes.ts"

export interface Frame {
  readonly type: number
  readonly channel: number
  readonly payload: Uint8Array
}

export interface Method {
  readonly classId: number
  readonly methodId: number
  readonly fields: Record<string, AMQPTypes.FieldValue>
}

export const PROTOCOL_HEADER = new Uint8Array([65, 77, 81, 80, 0, 0, 9, 1])

const MAX_VALUE_BYTES = 16 * 1024 * 1024
const MAX_DEPTH = 32
const ZERO = BigInt(0)
const MAX_UINT64 = BigInt("18446744073709551615")
const MAX_INT64 = BigInt("9223372036854775807")
const MIN_INT64 = BigInt("-9223372036854775808")
const encoder = new TextEncoder()
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true })
const fail = (reason: string): never => {
  throw new AMQPProtocolError({ reason })
}

const integer = (value: unknown, minimum: number, maximum: number): number => {
  if (typeof value !== "number" || !Number.isInteger(value) || value < minimum || value > maximum) {
    return fail("Integer outside wire range")
  }
  return value
}

const uint64 = (value: unknown): bigint => {
  if (typeof value === "number" && Number.isSafeInteger(value)) value = BigInt(value)
  if (typeof value !== "bigint" || value < ZERO || value > MAX_UINT64) {
    return fail("Unsigned 64-bit integer outside wire range")
  }
  return value
}

const utf8 = (value: unknown): Uint8Array => {
  if (typeof value !== "string") return fail("Expected a string")
  // TextEncoder replaces lone surrogates, which would silently change a wire value.
  for (let i = 0; i < value.length; i++) {
    const code = value.charCodeAt(i)
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(++i)
      if (!(next >= 0xdc00 && next <= 0xdfff)) fail("Invalid UTF-16 string")
    } else if (code >= 0xdc00 && code <= 0xdfff) fail("Invalid UTF-16 string")
  }
  return encoder.encode(value)
}

class Writer {
  private bytes = new Uint8Array(128)
  private view = new DataView(this.bytes.buffer)
  length = 0

  private reserve(size: number): number {
    const start = this.length
    const length = start + size
    if (length > MAX_VALUE_BYTES) fail("Encoded value exceeds size limit")
    if (length > this.bytes.length) {
      const bytes = new Uint8Array(Math.min(MAX_VALUE_BYTES, Math.max(length, this.bytes.length * 2)))
      bytes.set(this.bytes.subarray(0, start))
      this.bytes = bytes
      this.view = new DataView(bytes.buffer)
    }
    this.length = length
    return start
  }

  u8(value: unknown): void {
    const v = integer(value, 0, 255)
    const offset = this.reserve(1)
    this.view.setUint8(offset, v)
  }
  u16(value: unknown): void {
    const v = integer(value, 0, 65535)
    const offset = this.reserve(2)
    this.view.setUint16(offset, v)
  }
  u32(value: unknown): void {
    const v = integer(value, 0, 0xffffffff)
    const offset = this.reserve(4)
    this.view.setUint32(offset, v)
  }
  u64(value: unknown): void {
    const v = uint64(value)
    const offset = this.reserve(8)
    this.view.setBigUint64(offset, v)
  }
  signed(value: number | bigint, size: number): void {
    const offset = this.reserve(size)
    if (size === 1) this.view.setInt8(offset, integer(value, -128, 127))
    else if (size === 2) this.view.setInt16(offset, integer(value, -32768, 32767))
    else if (size === 4) this.view.setInt32(offset, integer(value, -2147483648, 2147483647))
    else {
      const v = typeof value === "number" ? BigInt(value) : value
      if (v < MIN_INT64 || v > MAX_INT64) fail("Signed 64-bit integer outside wire range")
      this.view.setBigInt64(offset, v)
    }
  }
  float(value: number): void {
    const offset = this.reserve(8)
    this.view.setFloat64(offset, value)
  }
  raw(value: Uint8Array): void {
    const offset = this.reserve(value.length)
    this.bytes.set(value, offset)
  }
  short(value: unknown): void {
    const bytes = utf8(value)
    if (bytes.length > 255) fail("Short string exceeds 255 UTF-8 bytes")
    if (bytes.includes(0)) fail("Short string contains a zero octet")
    this.u8(bytes.length)
    this.raw(bytes)
  }
  long(value: unknown): void {
    const bytes = value instanceof Uint8Array ? value : utf8(value)
    this.u32(bytes.length)
    this.raw(bytes)
  }
  sized(write: () => void): void {
    const start = this.reserve(4)
    write()
    this.view.setUint32(start, this.length - start - 4)
  }
  finish(): Uint8Array {
    return this.bytes.slice(0, this.length)
  }
}

class Reader {
  private readonly view: DataView
  readonly bytes: Uint8Array
  offset = 0
  constructor(bytes: Uint8Array) {
    this.bytes = bytes
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  }
  private take(size: number): number {
    if (size > this.bytes.length - this.offset) return fail("Truncated wire value")
    const start = this.offset
    this.offset += size
    return start
  }
  u8(): number {
    return this.view.getUint8(this.take(1))
  }
  u16(): number {
    return this.view.getUint16(this.take(2))
  }
  u32(): number {
    return this.view.getUint32(this.take(4))
  }
  u64(): bigint {
    return this.view.getBigUint64(this.take(8))
  }
  signed(size: number): number | bigint {
    const start = this.take(size)
    if (size === 1) return this.view.getInt8(start)
    if (size === 2) return this.view.getInt16(start)
    if (size === 4) return this.view.getInt32(start)
    return this.view.getBigInt64(start)
  }
  float(size: number): number {
    const start = this.take(size)
    return size === 4 ? this.view.getFloat32(start) : this.view.getFloat64(start)
  }
  raw(size: number): Uint8Array {
    if (size > MAX_VALUE_BYTES) return fail("Wire value exceeds size limit")
    const start = this.take(size)
    return this.bytes.subarray(start, start + size)
  }
  text(bytes: Uint8Array): string {
    try {
      return decoder.decode(bytes)
    } catch {
      return fail("Invalid UTF-8 string")
    }
  }
  short(): string {
    const bytes = this.raw(this.u8())
    if (bytes.includes(0)) fail("Short string contains a zero octet")
    return this.text(bytes)
  }
  long(): Uint8Array {
    return this.raw(this.u32())
  }
  done(): void {
    if (this.offset !== this.bytes.length) fail("Trailing wire data")
  }
}

const checkDepth = (depth: number): void => {
  if (depth > MAX_DEPTH) fail("Field table nesting exceeds limit")
}

const writeTable = (writer: Writer, value: unknown, depth: number): void => {
  checkDepth(depth)
  if (
    value === null || typeof value !== "object" || Array.isArray(value) ||
    (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null)
  ) {
    fail("Expected a field table")
  }
  writer.sized(() => {
    for (const [key, field] of Object.entries(value as AMQPTypes.FieldTable)) {
      writer.short(key)
      writeField(writer, field, depth + 1)
    }
  })
}

const writeField = (writer: Writer, value: AMQPTypes.FieldValue, depth: number): void => {
  checkDepth(depth)
  const tag = (text: string) => writer.u8(text.charCodeAt(0))
  if (value === null) tag("V")
  else if (typeof value === "boolean") {
    tag("t")
    writer.u8(value ? 1 : 0)
  } else if (typeof value === "string") {
    tag("S")
    writer.long(value)
  } else if (typeof value === "bigint") {
    // RabbitMQ treats both long-long field tags as signed, unlike delivery tags.
    tag("l")
    writer.signed(value, 8)
  } else if (typeof value === "number") {
    if (Number.isSafeInteger(value)) {
      const size = value >= -128 && value <= 127 ? 1 : value >= -32768 && value <= 32767 ?
        2 :
        value >= -2147483648 && value <= 2147483647
        ? 4
        : 8
      tag(size === 1 ? "b" : size === 2 ? "s" : size === 4 ? "I" : "l")
      writer.signed(value, size)
    } else {
      if (!Number.isFinite(value)) fail("Non-finite field number")
      tag("d")
      writer.float(value)
    }
  } else if (value instanceof Uint8Array) {
    tag("x")
    writer.long(value)
  } else if (value instanceof Date) {
    const milliseconds = value.getTime()
    if (!Number.isSafeInteger(milliseconds) || milliseconds < 0) {
      fail("Timestamp must be a valid nonnegative Date")
    }
    tag("T")
    writer.u64(BigInt(Math.floor(milliseconds / 1000)))
  } else if (Array.isArray(value)) {
    tag("A")
    writer.sized(() => {
      for (const field of value) writeField(writer, field, depth + 1)
    })
  } else if (typeof value === "object" && "_tag" in value && value._tag === "Decimal") {
    const decimal = value as AMQPTypes.Decimal
    tag("D")
    writer.u8(decimal.scale)
    writer.u32(decimal.value)
  } else {
    tag("F")
    writeTable(writer, value, depth)
  }
}

const readTable = (reader: Reader, depth: number): AMQPTypes.FieldTable => {
  checkDepth(depth)
  const nested = new Reader(reader.long())
  const table: Record<string, AMQPTypes.FieldValue> = Object.create(null)
  while (nested.offset < nested.bytes.length) {
    const key = nested.short()
    if (Object.hasOwn(table, key)) fail("Duplicate field table key")
    table[key] = readField(nested, depth + 1)
  }
  return table
}

const readField = (reader: Reader, depth: number): AMQPTypes.FieldValue => {
  checkDepth(depth)
  const tag = reader.u8()
  switch (String.fromCharCode(tag)) {
    case "V":
      return null
    case "t": {
      const value = reader.u8()
      if (value > 1) fail("Invalid boolean field")
      return value === 1
    }
    case "b":
      return reader.signed(1)
    case "B":
      return reader.u8()
    case "s":
    case "U":
      return reader.signed(2)
    case "u":
      return reader.u16()
    case "I":
      return reader.signed(4)
    case "i":
      return reader.u32()
    case "l":
    case "L":
      return reader.signed(8)
    case "f":
    case "d": {
      const value = reader.float(tag === 102 ? 4 : 8)
      if (!Number.isFinite(value)) fail("Non-finite field number")
      return value
    }
    case "D":
      return { _tag: "Decimal", scale: reader.u8(), value: reader.u32() }
    case "S": {
      // Long strings in field tables are binary values on the wire. Keep opaque
      // non-UTF-8 strings from other clients instead of rejecting the delivery.
      const bytes = reader.long()
      try {
        return decoder.decode(bytes)
      } catch {
        return bytes.slice()
      }
    }
    case "x":
      return reader.long().slice()
    case "T": {
      const seconds = reader.u64()
      if (seconds > BigInt("8640000000000")) fail("Timestamp exceeds Date range")
      return new Date(Number(seconds) * 1000)
    }
    case "F":
      return readTable(reader, depth)
    case "A": {
      const nested = new Reader(reader.long())
      const values: Array<AMQPTypes.FieldValue> = []
      while (nested.offset < nested.bytes.length) values.push(readField(nested, depth + 1))
      return values
    }
    default:
      return fail(`Unknown field type ${tag}`)
  }
}

// Consecutive bit fields share an octet, least significant bit first.
type Kind = "u8" | "u16" | "u32" | "u64" | "short" | "text" | "bytes" | "table" | "bit"
type Spec = ReadonlyArray<readonly [string, Kind]>
const specs = new Map<string, Spec>()
const define = (classId: number, methodId: number, description = ""): void => {
  specs.set(
    `${classId}:${methodId}`,
    description === "" ? [] : description.split(" ").map((field) => {
      const [name, kind] = field.split(":")
      return [name, kind as Kind] as const
    })
  )
}

define(10, 10, "versionMajor:u8 versionMinor:u8 serverProperties:table mechanisms:text locales:text")
define(10, 11, "clientProperties:table mechanism:short response:bytes locale:short")
define(10, 30, "channelMax:u16 frameMax:u32 heartbeat:u16")
define(10, 31, "channelMax:u16 frameMax:u32 heartbeat:u16")
define(10, 40, "virtualHost:short reserved1:short outOfBand:bit")
define(10, 41, "reserved1:short")
define(10, 50, "replyCode:u16 replyText:short classId:u16 methodId:u16")
define(10, 51)
define(10, 60, "reason:short")
define(10, 61)
define(10, 70, "newSecret:bytes reason:short")
define(10, 71)
define(20, 10, "reserved1:short")
define(20, 11, "reserved1:text")
define(20, 20, "active:bit")
define(20, 21, "active:bit")
define(20, 40, "replyCode:u16 replyText:short classId:u16 methodId:u16")
define(20, 41)
define(
  40,
  10,
  "reserved1:u16 exchange:short type:short passive:bit durable:bit autoDelete:bit internal:bit noWait:bit arguments:table"
)
define(40, 11)
define(40, 20, "reserved1:u16 exchange:short ifUnused:bit noWait:bit")
define(40, 21)
define(40, 30, "reserved1:u16 destination:short source:short routingKey:short noWait:bit arguments:table")
define(40, 31)
define(40, 40, "reserved1:u16 destination:short source:short routingKey:short noWait:bit arguments:table")
define(40, 51)
define(
  50,
  10,
  "reserved1:u16 queue:short passive:bit durable:bit exclusive:bit autoDelete:bit noWait:bit arguments:table"
)
define(50, 11, "queue:short messageCount:u32 consumerCount:u32")
define(50, 20, "reserved1:u16 queue:short exchange:short routingKey:short noWait:bit arguments:table")
define(50, 21)
define(50, 30, "reserved1:u16 queue:short noWait:bit")
define(50, 31, "messageCount:u32")
define(50, 40, "reserved1:u16 queue:short ifUnused:bit ifEmpty:bit noWait:bit")
define(50, 41, "messageCount:u32")
define(50, 50, "reserved1:u16 queue:short exchange:short routingKey:short arguments:table")
define(50, 51)
define(60, 10, "prefetchSize:u32 prefetchCount:u16 global:bit")
define(60, 11)
define(
  60,
  20,
  "reserved1:u16 queue:short consumerTag:short noLocal:bit noAck:bit exclusive:bit noWait:bit arguments:table"
)
define(60, 21, "consumerTag:short")
define(60, 30, "consumerTag:short noWait:bit")
define(60, 31, "consumerTag:short")
define(60, 40, "reserved1:u16 exchange:short routingKey:short mandatory:bit immediate:bit")
define(60, 50, "replyCode:u16 replyText:short exchange:short routingKey:short")
define(60, 60, "consumerTag:short deliveryTag:u64 redelivered:bit exchange:short routingKey:short")
define(60, 70, "reserved1:u16 queue:short noAck:bit")
define(60, 71, "deliveryTag:u64 redelivered:bit exchange:short routingKey:short messageCount:u32")
define(60, 72, "reserved1:short")
define(60, 80, "deliveryTag:u64 multiple:bit")
define(60, 90, "deliveryTag:u64 requeue:bit")
define(60, 100, "requeue:bit")
define(60, 110, "requeue:bit")
define(60, 111)
define(60, 120, "deliveryTag:u64 multiple:bit requeue:bit")
define(85, 10, "noWait:bit")
define(85, 11)

const getSpec = (classId: number, methodId: number): Spec =>
  specs.get(`${classId}:${methodId}`) ?? fail(`Unsupported method ${classId}:${methodId}`)

export const encodeFrame = (type: number, channel: number, payload: Uint8Array): Uint8Array => {
  integer(channel, 0, 65535)
  if (![1, 2, 3, 8].includes(type)) fail("Unknown frame type")
  if (type === 8 && (channel !== 0 || payload.length !== 0)) fail("Invalid heartbeat frame")
  if (payload.length > MAX_VALUE_BYTES) fail("Frame payload exceeds size limit")
  const bytes = new Uint8Array(payload.length + 8)
  const view = new DataView(bytes.buffer)
  view.setUint8(0, type)
  view.setUint16(1, channel)
  view.setUint32(3, payload.length)
  bytes.set(payload, 7)
  bytes[bytes.length - 1] = 0xce
  return bytes
}

export const encodeMethod = (
  channel: number,
  classId: number,
  methodId: number,
  fields: Record<string, AMQPTypes.FieldValue> = {}
): Uint8Array => {
  const spec = getSpec(classId, methodId)
  const writer = new Writer()
  writer.u16(classId)
  writer.u16(methodId)
  let bits = 0
  let bitCount = 0
  const flush = () => {
    if (bitCount !== 0) writer.u8(bits)
    bits = 0
    bitCount = 0
  }
  for (const [name, kind] of spec) {
    const value = Object.hasOwn(fields, name) ?
      fields[name] :
      kind === "bit" ?
      false :
      kind === "table" ?
      {} :
      kind === "short" || kind === "text" || kind === "bytes"
      ? ""
      : 0
    if (kind === "bit") {
      if (typeof value !== "boolean") fail("Expected a method boolean")
      if (value) bits |= 1 << bitCount
      if (++bitCount === 8) flush()
    } else {
      flush()
      if (kind === "short") writer.short(value)
      else if (kind === "text" || kind === "bytes") writer.long(value)
      else if (kind === "table") writeTable(writer, value, 0)
      else writer[kind](value)
    }
  }
  flush()
  return encodeFrame(1, channel, writer.finish())
}

export const decodeMethod = (payload: Uint8Array): Method => {
  const reader = new Reader(payload)
  const classId = reader.u16()
  const methodId = reader.u16()
  const spec = getSpec(classId, methodId)
  const fields: Record<string, AMQPTypes.FieldValue> = {}
  let bits = 0
  let bitCount = 0
  const finishBits = () => {
    if (bitCount > 0 && (bits >>> bitCount) !== 0) fail("Unknown method bit flags")
    bitCount = 0
  }
  for (const [name, kind] of spec) {
    if (kind === "bit") {
      if (bitCount === 0) bits = reader.u8()
      fields[name] = (bits & (1 << bitCount)) !== 0
      if (++bitCount === 8) bitCount = 0
    } else {
      finishBits()
      fields[name] = kind === "short" ? reader.short() : kind === "text" ?
        reader.text(reader.long()) :
        kind === "bytes"
        ? reader.long().slice()
        : kind === "table"
        ? readTable(reader, 0)
        : reader[kind]()
    }
  }
  finishBits()
  reader.done()
  return { classId, methodId, fields }
}

const properties: ReadonlyArray<readonly [keyof AMQPTypes.MessageProperties, Kind]> = [
  ["contentType", "short"],
  ["contentEncoding", "short"],
  ["headers", "table"],
  ["deliveryMode", "u8"],
  ["priority", "u8"],
  ["correlationId", "short"],
  ["replyTo", "short"],
  ["expiration", "short"],
  ["messageId", "short"],
  ["timestamp", "u64"],
  ["type", "short"],
  ["userId", "short"],
  ["appId", "short"],
  ["clusterId", "short"]
]

export const encodeContentHeader = (
  channel: number,
  bodySize: bigint,
  value: AMQPTypes.MessageProperties
): Uint8Array => {
  const writer = new Writer()
  writer.u16(60)
  writer.u16(0)
  writer.u64(bodySize)
  let flags = 0
  properties.forEach(([name], index) => {
    if (value[name] !== undefined) flags |= 1 << (15 - index)
  })
  writer.u16(flags)
  for (const [name, kind] of properties) {
    const field = value[name]
    if (field === undefined) continue
    if (kind === "short") writer.short(name === "expiration" && typeof field === "number" ? String(field) : field)
    else if (kind === "table") writeTable(writer, field, 0)
    else if (kind === "u64") writer.u64(field)
    else writer.u8(field)
  }
  return encodeFrame(2, channel, writer.finish())
}

export const decodeContentHeader = (
  payload: Uint8Array
): { bodySize: bigint; properties: AMQPTypes.MessageProperties } => {
  const reader = new Reader(payload)
  if (reader.u16() !== 60) fail("Unsupported content class")
  if (reader.u16() !== 0) fail("Invalid content weight")
  const bodySize = reader.u64()
  const flags = reader.u16()
  if ((flags & 3) !== 0) fail("Unknown content property flags")
  const value: Record<string, AMQPTypes.FieldValue> = {}
  properties.forEach(([name, kind], index) => {
    if ((flags & (1 << (15 - index))) === 0) return
    value[name] = kind === "short" ? reader.short() : kind === "table" ?
      readTable(reader, 0) :
      kind === "u64"
      ? reader.u64()
      : reader.u8()
  })
  reader.done()
  return { bodySize, properties: value as AMQPTypes.MessageProperties }
}

/** Buffers only the current frame, copying each incoming byte at most once. */
export class FrameDecoder {
  private readonly header = new Uint8Array(7)
  private headerLength = 0
  private payload: Uint8Array | undefined
  private payloadLength = 0
  private type = 0
  private channel = 0
  private readonly frameLimit: number
  private readonly bufferLimit: number

  constructor(maxFrameSize = 131072, maxBufferedBytes?: number) {
    integer(maxFrameSize, 0, 0xffffffff)
    this.frameLimit = maxFrameSize === 0 ? MAX_VALUE_BYTES + 8 : maxFrameSize
    this.bufferLimit = maxBufferedBytes ?? this.frameLimit
    integer(this.bufferLimit, 8, Number.MAX_SAFE_INTEGER)
  }

  feed(chunk: Uint8Array): Array<Frame> {
    const frames: Array<Frame> = []
    let offset = 0
    while (offset < chunk.length) {
      if (this.headerLength < 7) {
        const size = Math.min(7 - this.headerLength, chunk.length - offset)
        this.header.set(chunk.subarray(offset, offset + size), this.headerLength)
        this.headerLength += size
        offset += size
        if (this.headerLength < 7) continue
        const view = new DataView(this.header.buffer)
        this.type = view.getUint8(0)
        this.channel = view.getUint16(1)
        const sizeOfPayload = view.getUint32(3)
        if (![1, 2, 3, 8].includes(this.type)) fail("Unknown frame type")
        if (this.type === 8 && (this.channel !== 0 || sizeOfPayload !== 0)) fail("Invalid heartbeat frame")
        if (sizeOfPayload + 8 > this.frameLimit) fail("Frame exceeds negotiated frame maximum")
        if (sizeOfPayload + 8 > this.bufferLimit || sizeOfPayload > MAX_VALUE_BYTES) {
          fail("Frame exceeds buffer limit")
        }
        this.payload = new Uint8Array(sizeOfPayload)
      }
      const payload = this.payload!
      const size = Math.min(payload.length - this.payloadLength, chunk.length - offset)
      payload.set(chunk.subarray(offset, offset + size), this.payloadLength)
      this.payloadLength += size
      offset += size
      if (this.payloadLength < payload.length || offset === chunk.length) continue
      if (chunk[offset++] !== 0xce) fail("Invalid frame end marker")
      frames.push({ type: this.type, channel: this.channel, payload })
      this.headerLength = 0
      this.payloadLength = 0
      this.payload = undefined
    }
    return frames
  }

  end(): void {
    if (this.headerLength !== 0) fail("Incomplete frame at end of stream")
  }
}
