import { describe, expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import { AMQPProtocolError } from "../src/AMQPError.ts"
import type * as AMQPTypes from "../src/AMQPTypes.ts"
import * as Codec from "../src/internal/codec.ts"
import * as Protocol from "../src/internal/protocol.ts"

const payload = (frame: Uint8Array): Uint8Array => frame.subarray(7, frame.length - 1)
const method = (fields: Record<string, AMQPTypes.FieldValue>): Codec.Method => ({ classId: 50, methodId: 11, fields })

describe("AMQP protocol vocabulary", () => {
  it.effect("validates every supported decoded method without changing its field shape", () =>
    Effect.gen(function*() {
      for (const descriptor of Object.values(Protocol.methods)) {
        const bytes = payload(Codec.encodeMethod(1, descriptor))
        const raw = Codec.decodeMethod(bytes)
        expect(yield* Codec.decodeMethodEffect(bytes)).toEqual(raw)
        expect(yield* Protocol.decodeFields(descriptor, raw.fields)).toEqual(raw.fields)
      }
    }))

  it.effect("rejects unknown fields, missing values, and invalid scalar types with Schema causes", () =>
    Effect.gen(function*() {
      const samples: Array<readonly [Protocol.MethodDescriptor, unknown]> = [
        [Protocol.ChannelFlow, null],
        [Protocol.ChannelFlow, []],
        [Protocol.ChannelFlow, {}],
        [Protocol.ChannelFlow, { active: 1 }],
        [Protocol.ChannelFlow, { active: true, extra: false }],
        [Protocol.ChannelFlowOk, { active: "true" }],
        [Protocol.ConnectionCloseOk, new Date(0)],
        [Protocol.ConnectionCloseOk, []],
        [Protocol.ConnectionCloseOk, { extra: false }],
        [Protocol.QueueDeclareOk, { queue: new Uint8Array(), messageCount: 0, consumerCount: 0 }],
        [Protocol.QueueDeclareOk, { queue: "q", messageCount: -1, consumerCount: 0 }],
        [Protocol.QueueDeclareOk, { queue: "q", messageCount: 0x100000000, consumerCount: 0 }],
        [Protocol.QueueDeclareOk, { queue: "q", messageCount: 0.5, consumerCount: 0 }],
        [Protocol.ConnectionTune, { channelMax: 65536, frameMax: 0, heartbeat: 0 }],
        [Protocol.ConnectionStart, {
          versionMajor: 256,
          versionMinor: 0,
          serverProperties: {},
          mechanisms: "PLAIN",
          locales: "en_US"
        }],
        [Protocol.BasicAck, { deliveryTag: 1, multiple: false }],
        [Protocol.BasicAck, { deliveryTag: -1n, multiple: false }],
        [Protocol.BasicAck, { deliveryTag: 0x10000000000000000n, multiple: false }],
        [Protocol.ConnectionUpdateSecret, { newSecret: "not bytes", reason: "rotation" }]
      ]
      for (const [descriptor, input] of samples) {
        const result = yield* Effect.result(Protocol.decodeFields(descriptor, input))
        expect(Result.isFailure(result)).toBe(true)
        if (Result.isFailure(result)) {
          expect(result.failure).toBeInstanceOf(AMQPProtocolError)
          expect(Schema.isSchemaError(result.failure.cause)).toBe(true)
        }
      }
    }))

  it.effect("validates recursive field tables, wire decimals, timestamps, and signed bigints", () =>
    Effect.gen(function*() {
      const values: AMQPTypes.FieldTable = {
        decimal: { _tag: "Decimal", scale: 255, value: 0xffffffff },
        date: new Date(0),
        binary: new Uint8Array([255]),
        array: [null, true, "text", 1.5, -0x8000000000000000n, { nested: 0x7fffffffffffffffn }]
      }
      const descriptor = Protocol.ConnectionStart
      const fields = {
        versionMajor: 0,
        versionMinor: 9,
        serverProperties: values,
        mechanisms: "PLAIN",
        locales: "en_US"
      }
      expect(yield* Protocol.decodeFields(descriptor, fields)).toEqual(fields)
      const bytes = payload(Codec.encodeMethod(0, descriptor, fields))
      expect(yield* Codec.decodeMethodEffect(bytes)).toEqual(Codec.decodeMethod(bytes))
      let deep: AMQPTypes.FieldValue = null
      for (let i = 0; i < 31; i++) deep = [deep]
      expect(
        yield* Protocol.decodeFields(descriptor, {
          ...fields,
          serverProperties: { value: deep }
        })
      ).toEqual({ ...fields, serverProperties: { value: deep } })
      deep = [deep]
      const cyclic: Record<string, unknown> = {}
      cyclic.self = cyclic
      const invalid: Array<unknown> = [
        new Date(0),
        [],
        new Uint8Array(),
        { value: undefined },
        { value: Symbol("invalid") },
        { value: Number.NaN },
        { value: Number.POSITIVE_INFINITY },
        { value: new Date(-1) },
        { value: new Date(Number.NaN) },
        { value: 0x8000000000000000n },
        { value: -0x8000000000000001n },
        { value: deep },
        cyclic
      ]
      for (const serverProperties of invalid) {
        const result = yield* Effect.result(Protocol.decodeFields(descriptor, { ...fields, serverProperties }))
        expect(Result.isFailure(result)).toBe(true)
      }
    }))

  it.effect("does not reserve user table keys as scalar decimal discriminants", () =>
    Effect.gen(function*() {
      // Independently encoded connection.start: custom is an F table, not a D decimal.
      // The regular encoder interprets a nested _tag: Decimal object as a scalar decimal.
      const bytes = new Uint8Array([
        0,
        10,
        0,
        10,
        0,
        9,
        0,
        0,
        0,
        48,
        6,
        99,
        117,
        115,
        116,
        111,
        109,
        70,
        0,
        0,
        0,
        36,
        4,
        95,
        116,
        97,
        103,
        83,
        0,
        0,
        0,
        7,
        68,
        101,
        99,
        105,
        109,
        97,
        108,
        5,
        108,
        97,
        98,
        101,
        108,
        83,
        0,
        0,
        0,
        8,
        109,
        101,
        116,
        97,
        100,
        97,
        116,
        97,
        0,
        0,
        0,
        5,
        80,
        76,
        65,
        73,
        78,
        0,
        0,
        0,
        5,
        101,
        110,
        95,
        85,
        83
      ])
      const decoded = yield* Codec.decodeMethodEffect(bytes)
      expect(decoded).toEqual(Codec.decodeMethod(bytes))
      expect(Protocol.readTable(decoded, "serverProperties").custom).toEqual({ _tag: "Decimal", label: "metadata" })
      // A table's ordinary numeric metadata is not constrained by the scalar decimal scale/value ranges.
      expect(Protocol.readTable(method({ arguments: { _tag: "Decimal", scale: 256, value: -1 } }), "arguments"))
        .toEqual({ _tag: "Decimal", scale: 256, value: -1 })
    }))

  it.effect("returns malformed wire payloads as typed failures rather than defects", () =>
    Effect.gen(function*() {
      const malformed = [
        new Uint8Array(),
        new Uint8Array([0, 10, 0, 20]),
        new Uint8Array([0, 20, 0, 20]),
        new Uint8Array([0, 20, 0, 20, 2]),
        new Uint8Array([0, 20, 0, 20, 1, 0])
      ]
      for (const bytes of malformed) {
        const result = yield* Effect.result(Codec.decodeMethodEffect(bytes))
        expect(Result.isFailure(result)).toBe(true)
        if (Result.isFailure(result)) expect(result.failure).toBeInstanceOf(AMQPProtocolError)
      }
    }))

  it("encodes field tables without borrowing an unrelated method envelope", () => {
    expect(Codec.encodeFieldTable({})).toEqual(new Uint8Array([0, 0, 0, 0]))
    expect(Codec.encodeFieldTable({ flag: true })).toEqual(
      new Uint8Array([0, 0, 0, 7, 4, 102, 108, 97, 103, 116, 1])
    )
    expect(() => Codec.encodeFieldTable({ invalid: Number.NaN })).toThrow(AMQPProtocolError)
  })

  it("resolves every named method to one stable immutable descriptor", () => {
    const descriptors = Object.values(Protocol.methods)
    expect(descriptors).toHaveLength(56)
    expect(new Set(descriptors.map((value) => `${value.classId}:${value.methodId}`)).size).toBe(56)
    for (const [name, descriptor] of Object.entries(Protocol.methods)) {
      expect(descriptor.name).toBe(name)
      expect(Protocol.lookup(descriptor.classId, descriptor.methodId)).toBe(descriptor)
      expect(Object.isFrozen(descriptor)).toBe(true)
      expect(Object.isFrozen(descriptor.fields)).toBe(true)
      expect(Object.isFrozen(descriptor.replies)).toBe(true)
      expect(descriptor.replies).toBe(descriptor.replies)
      const encoded = Codec.encodeMethod(1, descriptor.classId, descriptor.methodId)
      const decoded = Codec.decodeMethod(payload(encoded))
      expect(Protocol.lookup(decoded.classId, decoded.methodId)).toBe(descriptor)
      expect(Object.keys(decoded.fields)).toEqual(descriptor.fields.map(([name]) => name))
      expect(Codec.encodeMethod(1, descriptor, decoded.fields)).toEqual(encoded)
      expect(Codec.encodeMethod(1, descriptor)).toEqual(encoded)
      expect(Codec.encodeMethod(1, descriptor.classId, descriptor.methodId, decoded.fields)).toEqual(encoded)
    }
  })

  it("preserves golden wire bytes through named method descriptors", () => {
    const tune = Protocol.ConnectionTuneOk
    expect(Codec.encodeMethod(0, tune, {
      channelMax: 2047,
      frameMax: 131072,
      heartbeat: 60
    })).toEqual(new Uint8Array([1, 0, 0, 0, 0, 0, 12, 0, 10, 0, 31, 7, 255, 0, 2, 0, 0, 0, 60, 206]))
    const declare = Protocol.QueueDeclare
    expect(Codec.encodeMethod(1, declare, {
      queue: "q",
      durable: true,
      exclusive: true
    })).toEqual(new Uint8Array([1, 0, 1, 0, 0, 0, 13, 0, 50, 0, 10, 0, 0, 1, 113, 6, 0, 0, 0, 0, 206]))
    const nack = Protocol.BasicNack
    expect(payload(Codec.encodeMethod(1, nack, {
      deliveryTag: 0xffffffffffffffffn,
      multiple: true,
      requeue: true
    }))).toEqual(new Uint8Array([0, 60, 0, 120, 255, 255, 255, 255, 255, 255, 255, 255, 3]))
    const unbind = Protocol.ExchangeUnbind
    expect(payload(Codec.encodeMethod(1, unbind))).toEqual(
      new Uint8Array([0, 40, 0, 40, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    )
    expect(Protocol.ExchangeUnbind.replies).toEqual([Protocol.ExchangeUnbindOk])
    expect(Protocol.ExchangeUnbindOk.methodId).toBe(51)
  })

  it("owns synchronous reply relationships without treating unsolicited messages as replies", () => {
    type Relationship = readonly [Protocol.MethodDescriptor, ReadonlyArray<Protocol.MethodDescriptor>]
    const relationships: ReadonlyArray<Relationship> = [
      [Protocol.ConnectionStartOk, [Protocol.ConnectionTune]],
      [Protocol.ConnectionOpen, [Protocol.ConnectionOpenOk]],
      [Protocol.ConnectionClose, [Protocol.ConnectionCloseOk]],
      [Protocol.ConnectionUpdateSecret, [Protocol.ConnectionUpdateSecretOk]],
      [Protocol.ChannelOpen, [Protocol.ChannelOpenOk]],
      [Protocol.ChannelFlow, [Protocol.ChannelFlowOk]],
      [Protocol.ChannelClose, [Protocol.ChannelCloseOk]],
      [Protocol.ExchangeDeclare, [Protocol.ExchangeDeclareOk]],
      [Protocol.ExchangeDelete, [Protocol.ExchangeDeleteOk]],
      [Protocol.ExchangeBind, [Protocol.ExchangeBindOk]],
      [Protocol.ExchangeUnbind, [Protocol.ExchangeUnbindOk]],
      [Protocol.QueueDeclare, [Protocol.QueueDeclareOk]],
      [Protocol.QueueBind, [Protocol.QueueBindOk]],
      [Protocol.QueuePurge, [Protocol.QueuePurgeOk]],
      [Protocol.QueueDelete, [Protocol.QueueDeleteOk]],
      [Protocol.QueueUnbind, [Protocol.QueueUnbindOk]],
      [Protocol.BasicQos, [Protocol.BasicQosOk]],
      [Protocol.BasicConsume, [Protocol.BasicConsumeOk]],
      [Protocol.BasicCancel, [Protocol.BasicCancelOk]],
      [Protocol.BasicGet, [Protocol.BasicGetOk, Protocol.BasicGetEmpty]],
      [Protocol.BasicRecover, [Protocol.BasicRecoverOk]],
      [Protocol.ConfirmSelect, [Protocol.ConfirmSelectOk]]
    ]
    for (const [request, replies] of relationships) {
      expect(request.replies).toEqual(replies)
      for (const reply of replies) expect(Protocol.lookup(reply.classId, reply.methodId)).toBe(reply)
    }
    const requests = new Set(relationships.map(([request]) => request))
    for (const descriptor of Object.values(Protocol.methods)) {
      if (!requests.has(descriptor)) expect(descriptor.replies).toEqual([])
    }
  })

  it("keeps unsupported method coverage and encoding validation unchanged", () => {
    for (const [classId, methodId] of [[10, 20], [10, 21], [90, 10]]) {
      expect(() => Protocol.lookup(classId, methodId)).toThrow(AMQPProtocolError)
      expect(() => Codec.encodeMethod(0, classId, methodId)).toThrow(AMQPProtocolError)
      expect(() => Codec.decodeMethod(new Uint8Array([0, classId, 0, methodId]))).toThrow(AMQPProtocolError)
    }
    expect(() =>
      Codec.encodeMethod(1, Protocol.BasicAck.classId, Protocol.BasicAck.methodId, {
        deliveryTag: -1n
      })
    ).toThrow(AMQPProtocolError)
    expect(() =>
      Codec.encodeMethod(1, Protocol.ChannelFlow.classId, Protocol.ChannelFlow.methodId, {
        active: 1
      })
    ).toThrow(AMQPProtocolError)
  })

  it("reads decoded fields and queue replies with their original runtime checks", () => {
    const frame = Codec.encodeMethod(1, Protocol.QueueDeclareOk.classId, Protocol.QueueDeclareOk.methodId, {
      queue: "jobs",
      messageCount: 7,
      consumerCount: 2
    })
    expect(Protocol.queueReply(Codec.decodeMethod(payload(frame)))).toEqual({
      queue: "jobs",
      messageCount: 7,
      consumerCount: 2
    })
    expect(Protocol.readBigInt(method({ deliveryTag: 0xffffffffffffffffn }), "deliveryTag")).toBe(0xffffffffffffffffn)
    const table = Object.assign(Object.create(null), { product: "broker" })
    expect(Protocol.readTable(method({ serverProperties: table }), "serverProperties")).toBe(table)
    expect(Protocol.readTable(method({ arguments: {} }), "arguments")).toEqual({})
    expect(Protocol.readString(method({ value: "" }), "value")).toBe("")
    expect(Protocol.readNumber(method({ value: 0 }), "value")).toBe(0)
  })

  it("rejects missing fields, wrong scalar types, and non-table field values", () => {
    for (const read of [Protocol.readString, Protocol.readNumber, Protocol.readBigInt, Protocol.readTable]) {
      expect(() => read(method({}), "missing")).toThrow(AMQPProtocolError)
      expect(() => read(method({ value: null }), "value")).toThrow(AMQPProtocolError)
    }
    expect(() => Protocol.readString(method({ value: new Uint8Array() }), "value")).toThrow(AMQPProtocolError)
    expect(() => Protocol.readNumber(method({ value: 1n }), "value")).toThrow(AMQPProtocolError)
    expect(() => Protocol.readBigInt(method({ value: 1 }), "value")).toThrow(AMQPProtocolError)
    const invalidTables: Array<AMQPTypes.FieldValue> = [
      null,
      "table",
      1,
      1n,
      false,
      [],
      new Uint8Array(),
      new Date(0),
      { nested: Number.NaN },
      { nested: 0x8000000000000000n }
    ]
    for (const value of invalidTables) {
      expect(() => Protocol.readTable(method({ value }), "value")).toThrow(AMQPProtocolError)
    }
    expect(() => Protocol.queueReply(method({ queue: "jobs", messageCount: 1 }))).toThrow(AMQPProtocolError)
  })

  it("preserves Schema failures as the cause of synchronous field-reader errors", () => {
    for (const read of [Protocol.readString, Protocol.readNumber, Protocol.readBigInt, Protocol.readTable]) {
      try {
        read(method({}), "missing")
        expect.fail("Expected a protocol error")
      } catch (error) {
        expect(error).toBeInstanceOf(AMQPProtocolError)
        if (error instanceof AMQPProtocolError) expect(Schema.isSchemaError(error.cause)).toBe(true)
      }
    }
  })
})
