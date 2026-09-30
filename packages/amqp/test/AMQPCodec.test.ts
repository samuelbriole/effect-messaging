import { describe, expect, it } from "@effect/vitest"
import { AMQPProtocolError } from "../src/AMQPError.ts"
import type * as AMQPTypes from "../src/AMQPTypes.ts"
import {
  decodeContentHeader,
  decodeMethod,
  encodeContentHeader,
  encodeFrame,
  encodeMethod,
  FrameDecoder,
  PROTOCOL_HEADER
} from "../src/internal/codec.ts"

const bytes = (...values: Array<number>): Uint8Array => new Uint8Array(values)
const payload = (frame: Uint8Array): Uint8Array => frame.subarray(7, frame.length - 1)
const concat = (...chunks: Array<Uint8Array>): Uint8Array => {
  const result = new Uint8Array(chunks.reduce((size, chunk) => size + chunk.length, 0))
  let offset = 0
  for (const chunk of chunks) {
    result.set(chunk, offset)
    offset += chunk.length
  }
  return result
}
const malformed = (run: () => unknown): void => {
  expect(run).toThrow(AMQPProtocolError)
}
const tablePayload = (entries: Uint8Array): Uint8Array => {
  const length = new Uint8Array(4)
  new DataView(length.buffer).setUint32(0, entries.length)
  // Basic content class, weight, body size, headers property flag, field table.
  return concat(bytes(0, 60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 0), length, entries)
}
const tableEntry = (tag: string, value: Uint8Array, key = "a"): Uint8Array =>
  concat(bytes(key.length), new TextEncoder().encode(key), bytes(tag.charCodeAt(0)), value)

describe("AMQP 0-9-1 wire codec", () => {
  it("matches the protocol header and specification method golden bytes", () => {
    expect(PROTOCOL_HEADER).toEqual(bytes(65, 77, 81, 80, 0, 0, 9, 1))
    expect(encodeMethod(0, 10, 31, { channelMax: 2047, frameMax: 131072, heartbeat: 60 })).toEqual(
      bytes(1, 0, 0, 0, 0, 0, 12, 0, 10, 0, 31, 7, 255, 0, 2, 0, 0, 0, 60, 206)
    )
    expect(encodeMethod(1, 60, 10, { prefetchCount: 10, global: true })).toEqual(
      bytes(1, 0, 1, 0, 0, 0, 11, 0, 60, 0, 10, 0, 0, 0, 0, 0, 10, 1, 206)
    )
    expect(encodeMethod(1, 60, 40, { exchange: "x", routingKey: "rk", mandatory: true })).toEqual(
      bytes(1, 0, 1, 0, 0, 0, 12, 0, 60, 0, 40, 0, 0, 1, 120, 2, 114, 107, 1, 206)
    )
    expect(encodeMethod(1, 50, 10, { queue: "q", durable: true, exclusive: true })).toEqual(
      bytes(1, 0, 1, 0, 0, 0, 13, 0, 50, 0, 10, 0, 0, 1, 113, 6, 0, 0, 0, 0, 206)
    )
    expect(encodeFrame(8, 0, bytes())).toEqual(bytes(8, 0, 0, 0, 0, 0, 0, 206))
  })

  it("encodes and decodes every supported method with protocol defaults", () => {
    const methods: ReadonlyArray<readonly [number, ReadonlyArray<number>]> = [
      [10, [10, 11, 30, 31, 40, 41, 50, 51, 60, 61, 70, 71]],
      [20, [10, 11, 20, 21, 40, 41]],
      [40, [10, 11, 20, 21, 30, 31, 40, 51]],
      [50, [10, 11, 20, 21, 30, 31, 40, 41, 50, 51]],
      [60, [10, 11, 20, 21, 30, 31, 40, 50, 60, 70, 71, 72, 80, 90, 100, 110, 111, 120]],
      [85, [10, 11]]
    ]
    for (const [classId, ids] of methods) {
      for (const methodId of ids) {
        const encoded = encodeMethod(1, classId, methodId)
        const decoded = decodeMethod(payload(encoded))
        expect(decoded.classId).toBe(classId)
        expect(decoded.methodId).toBe(methodId)
        expect(encodeMethod(1, classId, methodId, decoded.fields)).toEqual(encoded)
      }
    }
  })

  it("keeps all 64 delivery-tag bits and packs adjacent flags", () => {
    const tag = 0xffffffffffffffffn
    const frame = encodeMethod(65535, 60, 120, { deliveryTag: tag, multiple: true, requeue: true })
    expect(payload(frame)).toEqual(bytes(0, 60, 0, 120, 255, 255, 255, 255, 255, 255, 255, 255, 3))
    expect(decodeMethod(payload(frame)).fields).toEqual({ deliveryTag: tag, multiple: true, requeue: true })
    const delivery = {
      consumerTag: "ct",
      deliveryTag: 0x8000000000000000n,
      redelivered: true,
      exchange: "e",
      routingKey: "r"
    }
    expect(decodeMethod(payload(encodeMethod(2, 60, 60, delivery))).fields).toEqual(delivery)
    malformed(() => encodeMethod(1, 60, 80, { deliveryTag: -1n }))
    malformed(() => encodeMethod(1, 60, 80, { deliveryTag: 1n << 64n }))
    malformed(() => encodeMethod(1, 60, 80, { deliveryTag: Number.MAX_SAFE_INTEGER + 1 }))
  })

  it("decodes textual negotiation long strings and preserves binary secrets", () => {
    const start = decodeMethod(payload(encodeMethod(0, 10, 10, {
      serverProperties: { product: "broker" },
      mechanisms: "PLAIN EXTERNAL",
      locales: "en_US"
    })))
    expect(start.fields.mechanisms).toBe("PLAIN EXTERNAL")
    expect(start.fields.locales).toBe("en_US")
    const response = bytes(0, 255, 0, 254)
    expect(decodeMethod(payload(encodeMethod(0, 10, 11, { response }))).fields.response).toEqual(response)
    expect(decodeMethod(payload(encodeMethod(0, 10, 70, { newSecret: "secret", reason: "rotation" }))).fields)
      .toEqual({ newSecret: new TextEncoder().encode("secret"), reason: "rotation" })
  })

  it("matches content header golden bytes and preserves large body sizes", () => {
    expect(encodeContentHeader(1, 3n, { contentType: "a", deliveryMode: 2 })).toEqual(
      bytes(2, 0, 1, 0, 0, 0, 17, 0, 60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 144, 0, 1, 97, 2, 206)
    )
    const properties: AMQPTypes.MessageProperties = {
      contentType: "application/json",
      contentEncoding: "utf-8",
      headers: { tracing: "abc" },
      deliveryMode: 2,
      priority: 0,
      correlationId: "c",
      replyTo: "q",
      expiration: "1000",
      messageId: "m",
      timestamp: 0xffffffffffffffffn,
      type: "event",
      userId: "guest",
      appId: "app",
      clusterId: "cluster"
    }
    expect(decodeContentHeader(payload(encodeContentHeader(1, 0xffffffffffffffffn, properties))))
      .toEqual({ bodySize: 0xffffffffffffffffn, properties })
    expect(decodeContentHeader(payload(encodeContentHeader(1, 0n, { expiration: 5000 }))).properties.expiration)
      .toBe("5000")
  })

  it("round-trips nested field tables, decimals, byte arrays, dates and null", () => {
    const table: AMQPTypes.FieldTable = {
      text: "你好",
      yes: true,
      no: false,
      empty: null,
      small: -128,
      medium: -32768,
      large: -2147483648,
      signed: -0x8000000000000000n,
      maximum: 0x7fffffffffffffffn,
      fraction: 1.25,
      decimal: { _tag: "Decimal", scale: 2, value: 0xffffffff },
      binary: bytes(0, 255),
      date: new Date("2026-09-30T00:00:00Z"),
      nested: { array: ["a", true, null, { inner: 5 }] }
    }
    const result = decodeContentHeader(payload(encodeContentHeader(1, 0n, { headers: table })))
    expect(result.properties.headers).toEqual(table)
    const copied = result.properties.headers!.binary as Uint8Array
    expect(copied).not.toBe(table.binary)
    const grown = { many: "x".repeat(1000), last: 127 }
    expect(decodeContentHeader(payload(encodeContentHeader(1, 0n, { headers: grown }))).properties.headers)
      .toEqual(grown)
  })

  it("handles signed and unsigned wire field tags independently", () => {
    const cases: ReadonlyArray<readonly [string, Uint8Array, AMQPTypes.FieldValue]> = [
      ["b", bytes(255), -1],
      ["B", bytes(255), 255],
      ["s", bytes(255, 255), -1],
      ["U", bytes(128, 0), -32768],
      ["u", bytes(255, 255), 65535],
      ["I", bytes(255, 255, 255, 255), -1],
      ["i", bytes(255, 255, 255, 255), 4294967295],
      ["l", bytes(255, 255, 255, 255, 255, 255, 255, 255), -1n],
      ["L", bytes(255, 255, 255, 255, 255, 255, 255, 255), -1n],
      ["f", bytes(63, 160, 0, 0), 1.25]
    ]
    for (const [tag, value, expected] of cases) {
      expect(decodeContentHeader(tablePayload(tableEntry(tag, value))).properties.headers!.a).toEqual(expected)
    }
  })

  it("retains pollution-sensitive keys without mutating object prototypes", () => {
    const headers = Object.create(null) as Record<string, AMQPTypes.FieldValue>
    headers.__proto__ = { polluted: true }
    Object.defineProperty(headers, "constructor", { value: "ordinary value", enumerable: true })
    headers.prototype = 42
    const decoded = decodeContentHeader(payload(encodeContentHeader(1, 0n, { headers }))).properties.headers!
    expect(Object.getPrototypeOf(decoded)).toBeNull()
    expect(Object.hasOwn(decoded, "__proto__")).toBe(true)
    expect(decoded.__proto__).toEqual({ polluted: true })
    expect(decoded.constructor).toBe("ordinary value")
    expect(Object.hasOwn({}, "polluted")).toBe(false)
  })

  it("supports every two-chunk split and bytewise fragmentation", () => {
    const frame = encodeMethod(42, 60, 60, {
      consumerTag: "test",
      deliveryTag: 0xffffffffffffffffn,
      exchange: "e",
      routingKey: "r"
    })
    for (let split = 0; split <= frame.length; split++) {
      const decoder = new FrameDecoder()
      const frames = [...decoder.feed(frame.subarray(0, split)), ...decoder.feed(frame.subarray(split))]
      expect(frames).toEqual([{ type: 1, channel: 42, payload: payload(frame) }])
      decoder.end()
    }
    const decoder = new FrameDecoder()
    const frames = Array.from(frame).flatMap((byte) => decoder.feed(bytes(byte)))
    expect(frames).toHaveLength(1)
    expect(frames[0].payload).toEqual(payload(frame))
    decoder.end()
  })

  it("coalesces frames and owns payload bytes independently of input", () => {
    const body = bytes(1, 2, 3)
    const input = concat(encodeFrame(8, 0, bytes()), encodeFrame(3, 1, body), encodeMethod(1, 85, 11))
    const decoder = new FrameDecoder(16, 16)
    const frames = decoder.feed(input)
    expect(frames.map((frame) => frame.type)).toEqual([8, 3, 1])
    input.fill(0)
    expect(frames[1].payload).toEqual(body)
    decoder.end()
  })

  it("rejects oversized and malformed frames before buffering payloads", () => {
    malformed(() => new FrameDecoder(16).feed(bytes(3, 0, 1, 0, 0, 0, 9)))
    malformed(() => new FrameDecoder(100, 16).feed(bytes(3, 0, 1, 0, 0, 0, 9)))
    malformed(() => new FrameDecoder(0).feed(bytes(3, 0, 1, 255, 255, 255, 255)))
    malformed(() => new FrameDecoder().feed(bytes(4, 0, 0, 0, 0, 0, 0)))
    malformed(() => new FrameDecoder().feed(bytes(8, 0, 1, 0, 0, 0, 0)))
    malformed(() => new FrameDecoder().feed(bytes(8, 0, 0, 0, 0, 0, 1)))
    malformed(() => new FrameDecoder().feed(bytes(3, 0, 1, 0, 0, 0, 0, 0)))
    malformed(() => encodeFrame(4, 0, bytes()))
    malformed(() => encodeFrame(8, 1, bytes()))
    malformed(() => encodeFrame(8, 0, bytes(1)))
    malformed(() => encodeFrame(3, 65536, bytes()))
    const complete = encodeFrame(3, 1, bytes(1, 2, 3))
    for (let size = 1; size < complete.length; size++) {
      const decoder = new FrameDecoder()
      decoder.feed(complete.subarray(0, size))
      malformed(() => decoder.end())
    }
  })

  it("validates UTF-8, short string byte limits and method ranges", () => {
    expect(decodeMethod(payload(encodeMethod(1, 60, 21, { consumerTag: "é".repeat(127) }))).fields.consumerTag)
      .toBe("é".repeat(127))
    malformed(() => encodeMethod(1, 60, 21, { consumerTag: "é".repeat(128) }))
    malformed(() => encodeMethod(1, 60, 21, { consumerTag: "\ud800" }))
    malformed(() => encodeMethod(1, 60, 21, { consumerTag: "a\u0000b" }))
    malformed(() => decodeMethod(bytes(0, 60, 0, 21, 1, 0)))
    malformed(() => decodeMethod(bytes(0, 60, 0, 21, 2, 192, 175)))
    malformed(() => decodeMethod(bytes(0, 60, 0, 21, 1, 255)))
    malformed(() => decodeMethod(bytes(0, 60, 0, 21, 3, 97)))
    malformed(() => encodeMethod(0, 10, 30, { channelMax: 65536 }))
    malformed(() => encodeMethod(0, 10, 30, { heartbeat: -1 }))
    malformed(() => encodeMethod(0, 10, 30, { frameMax: 1.5 }))
    malformed(() => encodeMethod(1, 60, 10, { global: 1 }))
    malformed(() => encodeMethod(1, 99, 10))
    malformed(() => decodeMethod(bytes(0, 99, 0, 10)))
    malformed(() => decodeMethod(bytes(0, 85, 0, 11, 0)))
    malformed(() => decodeMethod(bytes(0, 85, 0, 10, 2)))
  })

  it("rejects invalid content classes, weights, flags and trailing data", () => {
    const valid = payload(encodeContentHeader(1, 0n, {}))
    for (const [offset, value] of [[1, 61], [3, 1], [13, 1], [13, 2]]) {
      const invalid = valid.slice()
      invalid[offset] = value
      malformed(() => decodeContentHeader(invalid))
    }
    malformed(() => decodeContentHeader(concat(valid, bytes(0))))
    malformed(() => decodeContentHeader(valid.subarray(0, valid.length - 1)))
    malformed(() => encodeContentHeader(1, -1n, {}))
    malformed(() => encodeContentHeader(1, 0n, { priority: 256 }))
  })

  it("rejects bad field tags, invalid lengths, duplicate keys and excessive nesting", () => {
    malformed(() => decodeContentHeader(tablePayload(tableEntry("?", bytes()))))
    malformed(() => decodeContentHeader(tablePayload(tableEntry("t", bytes(2)))))
    malformed(() => decodeContentHeader(tablePayload(tableEntry("S", bytes(0, 0, 0, 2, 97)))))
    malformed(() => decodeContentHeader(tablePayload(tableEntry("S", bytes(255, 255, 255, 255)))))
    expect(decodeContentHeader(tablePayload(tableEntry("S", bytes(0, 0, 0, 1, 255)))).properties.headers!.a)
      .toEqual(bytes(255))
    malformed(() => decodeContentHeader(tablePayload(tableEntry("A", bytes(0, 0, 0, 2, 73, 1)))))
    malformed(() => decodeContentHeader(tablePayload(tableEntry("F", bytes(0, 0, 0, 1, 1)))))
    malformed(() => decodeContentHeader(tablePayload(tableEntry("T", bytes(255, 255, 255, 255, 255, 255, 255, 255)))))
    malformed(() => decodeContentHeader(tablePayload(concat(tableEntry("V", bytes()), tableEntry("V", bytes())))))
    let nested: AMQPTypes.FieldValue = null
    for (let i = 0; i < 40; i++) nested = { nested }
    malformed(() => encodeContentHeader(1, 0n, { headers: { nested } }))
    const cyclic: Record<string, AMQPTypes.FieldValue> = {}
    cyclic.self = cyclic
    malformed(() => encodeContentHeader(1, 0n, { headers: cyclic }))
    malformed(() => encodeContentHeader(1, 0n, { headers: { invalid: Number.NaN } }))
    malformed(() => encodeContentHeader(1, 0n, { headers: { invalid: 0xffffffffffffffffn } }))
    malformed(() => encodeContentHeader(1, 0n, { headers: { invalid: { _tag: "Decimal", scale: 2, value: -1 } } }))
    malformed(() => encodeContentHeader(1, 0n, { headers: { invalid: new Date(Number.NaN) } }))
    let nestedWire = tableEntry("V", bytes())
    for (let i = 0; i < 40; i++) {
      const length = new Uint8Array(4)
      new DataView(length.buffer).setUint32(0, nestedWire.length)
      nestedWire = tableEntry("F", concat(length, nestedWire))
    }
    malformed(() => decodeContentHeader(tablePayload(nestedWire)))
  })
})
