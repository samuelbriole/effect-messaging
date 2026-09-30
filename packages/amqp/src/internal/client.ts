import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Latch from "effect/Latch"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Redacted from "effect/Redacted"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Socket from "effect/socket/Socket"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import * as AMQPChannel from "../AMQPChannel.ts"
import * as AMQPConnection from "../AMQPConnection.ts"
import type * as AMQPConsumeMessage from "../AMQPConsumeMessage.ts"
import * as AMQPError from "../AMQPError.ts"
import * as AMQPTopology from "../AMQPTopology.ts"
import type * as AMQPTypes from "../AMQPTypes.ts"
import * as Codec from "./codec.ts"

type Error = AMQPError.AMQPError
type Fields = Record<string, AMQPTypes.FieldValue>
type Message = AMQPConsumeMessage.AMQPConsumeMessage

interface Reply {
  readonly method: Codec.Method
  readonly message?: Message
}

interface Pending {
  readonly expected: ReadonlyArray<number>
  readonly done: Deferred.Deferred<Reply, Error>
}

interface Write {
  readonly frames: Array<Uint8Array>
  readonly bytes: number
  readonly done: Deferred.Deferred<void, Error>
  readonly drained: Deferred.Deferred<void>
  readonly publish: boolean
  readonly priority: boolean
  readonly channel: number
  readonly physical?: Physical
  started: boolean
  cancelled: boolean
}

interface PublishAdmission {
  readonly command: Write
  readonly confirm: Deferred.Deferred<void, Error> | undefined
}

interface Envelope<A> {
  readonly value: A
  readonly physical: Physical
  readonly release: () => void
}

interface Consumer {
  readonly queue: AMQPTopology.QueueName
  readonly options: AMQPTypes.ConsumeOptions
  readonly mailbox: Queue.Queue<Envelope<Message>, Error | Cause.Done>
  tag: string
  active: boolean
}

interface QueueDeclaration {
  readonly requested: string
  options: AMQPTypes.QueueOptions
  readonly reference: AMQPTopology.QueueReference
  current: AMQPTypes.QueueReply
}

interface ExchangeDeclaration {
  readonly exchange: string
  readonly type: string
  readonly options: AMQPTypes.ExchangeOptions
}

interface Binding {
  readonly queue?: AMQPTopology.QueueName
  readonly destination?: string
  readonly source: string
  readonly routingKey: string
  readonly arguments: AMQPTypes.FieldTable
}

interface Logical {
  readonly options: AMQPChannel.AMQPChannelOptions
  readonly ready: Latch.Latch
  readonly closeDone: Deferred.Deferred<void>
  readonly operations: Semaphore.Semaphore
  readonly queues: Array<QueueDeclaration>
  readonly exchanges: Map<string, ExchangeDeclaration>
  readonly bindings: Array<Binding>
  readonly consumers: Set<Consumer>
  readonly returned: Queue.Queue<Envelope<AMQPTypes.ReturnedMessage>, Error | Cause.Done>
  physical: Physical | undefined
  closed: boolean
  error: Error | undefined
  prefetch: number
  globalPrefetch: number | undefined
  pendingOperations: number
}

interface Settlement {
  readonly physical: Physical
  readonly tag: bigint
  settled: boolean
  revoked: boolean
}

interface Content {
  readonly method: Codec.Method
  properties: AMQPTypes.MessageProperties | undefined
  body: Uint8Array | undefined
  offset: number
  release: (() => void) | undefined
}

interface Physical {
  readonly id: number
  readonly epoch: Epoch
  readonly logical: Logical
  readonly rpc: Semaphore.Semaphore
  readonly publishing: Semaphore.Semaphore
  readonly flow: Latch.Latch
  readonly confirmCapacity: Latch.Latch
  readonly admissionClosed: Deferred.Deferred<void>
  readonly confirms: Map<bigint, Deferred.Deferred<void, Error>>
  readonly settlements: Map<bigint, Settlement>
  readonly consumers: Map<string, Consumer>
  active: boolean
  closing: boolean
  pending: Pending | undefined
  content: Content | undefined
  sequence: bigint
  lastDeliveryTag: bigint
}

interface Epoch {
  readonly generation: number
  readonly scope: Scope.Scope
  readonly writer: Socket.Writer
  readonly failure: Deferred.Deferred<never, AMQPError.AMQPConnectionError>
  readonly wakeWriter: Latch.Latch
  readonly publishGate: Latch.Latch
  readonly control: Array<Write>
  readonly data: Array<Write>
  readonly channels: Map<number, Physical>
  readonly freeChannels: Array<number>
  readonly rpc: Semaphore.Semaphore
  active: boolean
  nextChannel: number
  pending: Pending | undefined
  writing: Write | undefined
  outboundBytes: number
  outboundCommands: number
  bufferedBytes: number
  frameMax: number
  channelMax: number
  heartbeat: number
  heartbeatWrite: Write | undefined
  lastRead: number
  lastWrite: number
  serverProperties: AMQPTypes.FieldTable
  brokerCloseError: AMQPError.AMQPConnectionError | undefined
  blocked: string | undefined
}

const methodKey = (method: Codec.Method): number => method.classId * 1000 + method.methodId
const connectionError = (reason: string, cause?: unknown, permanent = false) =>
  new AMQPError.AMQPConnectionError({ reason, cause, permanent })
const channelError = (reason: string, cause?: unknown) => new AMQPError.AMQPChannelError({ reason, cause })
const protocolError = (cause: unknown): AMQPError.AMQPProtocolError =>
  cause instanceof AMQPError.AMQPProtocolError
    ? cause
    : new AMQPError.AMQPProtocolError({ reason: "Invalid AMQP wire data", cause })
const publishError = (outcome: "NotSent" | "Unknown" | "Nacked", reason: string, cause?: unknown) =>
  new AMQPError.AMQPPublishError({ outcome, reason, cause })
const finish = <A, E>(deferred: Deferred.Deferred<A, E>, effect: Effect.Effect<A, E>): void => {
  Deferred.doneUnsafe(deferred, effect)
}
const combineReleases = (first: (() => void) | undefined, second: () => void): () => void => {
  return () => {
    first?.()
    second()
  }
}
const nameOf = (queue: AMQPTopology.QueueName): string => typeof queue === "string" ? queue : queue.queue
const integer = (name: string, value: number, minimum: number, maximum: number) =>
  Schema.decodeUnknownEffect(Schema.Int.check(Schema.isBetween({ minimum, maximum })))(value).pipe(
    Effect.mapError((cause) => channelError(`${name} must be an integer from ${minimum} to ${maximum}`, cause))
  )
const wire = <A>(thunk: () => A): Effect.Effect<A, AMQPError.AMQPProtocolError> =>
  Effect.try({ try: thunk, catch: protocolError })
const fieldString = (method: Codec.Method, key: string): string => {
  const value = method.fields[key]
  if (typeof value !== "string") throw new AMQPError.AMQPProtocolError({ reason: `Invalid ${key} field` })
  return value
}
const fieldNumber = (method: Codec.Method, key: string): number => {
  const value = method.fields[key]
  if (typeof value !== "number") throw new AMQPError.AMQPProtocolError({ reason: `Invalid ${key} field` })
  return value
}
const fieldBigInt = (method: Codec.Method, key: string): bigint => {
  const value = method.fields[key]
  if (typeof value !== "bigint") throw new AMQPError.AMQPProtocolError({ reason: `Invalid ${key} field` })
  return value
}
const fieldTable = (method: Codec.Method, key: string): AMQPTypes.FieldTable => {
  const value = method.fields[key]
  if (
    value === null || typeof value !== "object" || Array.isArray(value) || value instanceof Uint8Array ||
    value instanceof Date || "_tag" in value
  ) {
    throw new AMQPError.AMQPProtocolError({ reason: `Invalid ${key} field` })
  }
  return value as AMQPTypes.FieldTable
}
const replyQueue = (method: Codec.Method): AMQPTypes.QueueReply => ({
  queue: fieldString(method, "queue"),
  messageCount: fieldNumber(method, "messageCount"),
  consumerCount: fieldNumber(method, "consumerCount")
})

const canonicalField = (value: AMQPTypes.FieldValue): AMQPTypes.FieldValue => {
  if (value === null || typeof value !== "object" || value instanceof Date || value instanceof Uint8Array) return value
  if (Array.isArray(value)) return value.map(canonicalField)
  if ("_tag" in value && value._tag === "Decimal") return value
  return Object.fromEntries(
    Object.entries(value).sort(([left], [right]) => left.localeCompare(right)).map(
      ([key, field]) => [key, canonicalField(field)]
    )
  )
}

const sameArguments = (left: AMQPTypes.FieldTable, right: AMQPTypes.FieldTable): boolean => {
  const a = Codec.encodeMethod(0, 50, 20, { arguments: canonicalField(left) })
  const b = Codec.encodeMethod(0, 50, 20, { arguments: canonicalField(right) })
  if (a.byteLength !== b.byteLength) return false
  for (let index = 0; index < a.byteLength; index++) if (a[index] !== b[index]) return false
  return true
}

const snapshotField = (value: AMQPTypes.FieldValue, depth = 0): AMQPTypes.FieldValue => {
  if (depth > 32) throw new AMQPError.AMQPProtocolError({ reason: "Field table exceeds nesting limit" })
  if (value === null || typeof value !== "object") return value
  if (value instanceof Uint8Array) return value.slice()
  if (value instanceof Date) return new Date(value.getTime())
  if (Array.isArray(value)) return value.map((field) => snapshotField(field, depth + 1))
  return Object.fromEntries(Object.entries(value).map(([key, field]) => [key, snapshotField(field, depth + 1)]))
}

const snapshotOptions = <A extends { readonly arguments?: AMQPTypes.FieldTable }>(options: A): A => ({
  ...options,
  ...(options.arguments === undefined ? {} : {
    arguments: Object.fromEntries(Object.entries(options.arguments).map(([key, field]) => [key, snapshotField(field)]))
  })
})

// Queue.takeUnsafe never waits. This is used on retirement, not on the application read path.
const discard = <A>(mailbox: Queue.Queue<Envelope<A>, Error | Cause.Done>): void => {
  while (true) {
    const item = Queue.takeUnsafe(mailbox)
    if (item === undefined || Exit.isFailure(item)) return
    item.value.release()
  }
}

const mailboxStream = <A>(mailbox: Queue.Queue<Envelope<A>, Error | Cause.Done>): Stream.Stream<A, Error> =>
  Stream.fromEffectRepeat(Queue.take(mailbox)).pipe(
    Stream.filter((envelope) => {
      envelope.release()
      return envelope.physical.active && envelope.physical.epoch.active
    }),
    Stream.map((envelope) => envelope.value)
  )

/** @internal */
export const make = <R>(
  socketFactory: AMQPConnection.SocketFactory<R>,
  options: AMQPConnection.AMQPConnectionOptions
): Effect.Effect<AMQPConnection.AMQPConnection, Error, Scope.Scope | R> =>
  Effect.gen(function*() {
    options = {
      ...options,
      ...(options.clientProperties === undefined ? {} : {
        clientProperties: yield* wire(() =>
          Object.fromEntries(
            Object.entries(options.clientProperties ?? {}).map(([key, value]) => [key, snapshotField(value)])
          )
        )
      })
    }
    const scope = yield* Effect.scope
    const maxMessageBytes = options.maxMessageBytes ?? 16 * 1024 * 1024
    const maxBufferedBytes = options.maxBufferedBytes ?? 64 * 1024 * 1024
    const maxOutboundBytes = options.maxOutboundBytes ?? 32 * 1024 * 1024
    const maxPending = options.maxPendingOperations ?? 1024
    const requestedFrameMax = options.frameMax ?? 131072
    const requestedChannelMax = options.channelMax ?? 0
    const requestedHeartbeat = options.heartbeat ?? 60
    const connectionTimeout = options.connectionTimeout ?? "10 seconds"
    const waitConnectionTimeout = options.waitConnectionTimeout ?? "10 seconds"
    const shutdownTimeout = options.shutdownTimeout ?? "5 seconds"
    const validate = Effect.gen(function*() {
      yield* integer("heartbeat", requestedHeartbeat, 0, 65535)
      yield* integer("channelMax", requestedChannelMax, 0, 65535)
      yield* integer("frameMax", requestedFrameMax, 0, 0xffffffff)
      if (requestedFrameMax !== 0 && requestedFrameMax < 4096) {
        return yield* channelError("frameMax must be zero or at least 4096")
      }
      yield* integer("maxMessageBytes", maxMessageBytes, 1, 0x7fffffff)
      yield* integer("maxBufferedBytes", maxBufferedBytes, 4096, 0x7fffffff)
      yield* integer("maxOutboundBytes", maxOutboundBytes, 4096, 0x7fffffff)
      yield* integer("maxPendingOperations", maxPending, 1, 65535)
    })
    yield* validate.pipe(Effect.mapError((error) => connectionError(error.reason, error, true)))

    const state = yield* SubscriptionRef.make<AMQPConnection.ConnectionState>({ state: "Connecting", generation: 0 })
    const ready = Latch.makeUnsafe()
    const closeDone = Deferred.makeUnsafe<void>()
    const logicals = new Set<Logical>()
    const settlements = new WeakMap<Message, Settlement>()
    let current: Epoch | undefined
    let generation = 0
    let closing = false
    let terminal: Error | undefined
    let secret = options.password ?? "guest"
    let connection: AMQPConnection.AMQPConnection

    const setState = (value: AMQPConnection.ConnectionState) => SubscriptionRef.set(state, value)

    const failEpoch = (epoch: Epoch, error: AMQPError.AMQPConnectionError): void => {
      if (!epoch.active) return
      // Invalidate immediately, before any scoped fiber is interrupted or a replacement socket is acquired.
      epoch.active = false
      if (current === epoch) current = undefined
      ready.closeUnsafe()
      epoch.publishGate.openUnsafe()
      epoch.wakeWriter.openUnsafe()
      if (epoch.pending !== undefined) finish(epoch.pending.done, Effect.fail(error))
      for (const physical of epoch.channels.values()) retirePhysical(physical, error)
      for (
        const command of [...epoch.control, ...epoch.data, ...(epoch.writing === undefined ? [] : [epoch.writing])]
      ) {
        finish(
          command.done,
          Effect.fail(
            command.publish
              ? publishError(command.started ? "Unknown" : "NotSent", "Connection lost during publish", error)
              : error
          )
        )
      }
      epoch.control.length = 0
      epoch.data.length = 0
      finish(epoch.failure, Effect.fail(error))
    }

    const retirePhysical = (physical: Physical, error: Error): void => {
      if (!physical.active) return
      physical.active = false
      physical.closing = true
      finish(physical.admissionClosed, Effect.void)
      physical.flow.openUnsafe()
      physical.confirmCapacity.openUnsafe()
      physical.logical.ready.closeUnsafe()
      if (physical.logical.physical === physical) physical.logical.physical = undefined
      if (physical.pending !== undefined) finish(physical.pending.done, Effect.fail(error))
      for (const confirm of physical.confirms.values()) {
        finish(confirm, Effect.fail(publishError("Unknown", "Channel lost before broker confirmation", error)))
      }
      physical.confirms.clear()
      physical.settlements.clear()
      physical.consumers.clear()
      const epoch = physical.epoch
      for (
        const command of [...epoch.control, ...epoch.data, ...(epoch.writing === undefined ? [] : [epoch.writing])]
      ) {
        if (command.physical !== physical) continue
        if (!command.started) command.cancelled = true
        finish(
          command.done,
          Effect.fail(
            command.publish
              ? publishError(command.started ? "Unknown" : "NotSent", "Channel retired during publish", error)
              : error
          )
        )
      }
      physical.content?.release?.()
      physical.content = undefined
      for (const consumer of physical.logical.consumers) discard(consumer.mailbox)
      discard(physical.logical.returned)
    }

    const failLogical = (logical: Logical, error: Error): void => {
      logical.error = error
      if (logical.physical !== undefined) retirePhysical(logical.physical, error)
      logical.ready.openUnsafe()
      for (const consumer of logical.consumers) {
        discard(consumer.mailbox)
        Queue.failCauseUnsafe(consumer.mailbox, Cause.fail(error))
      }
      discard(logical.returned)
      Queue.failCauseUnsafe(logical.returned, Cause.fail(error))
    }

    const stop = (error: Error) =>
      Effect.sync(() => {
        terminal = error
        ready.openUnsafe()
        for (const logical of logicals) failLogical(logical, error)
      })

    const awaitEpoch = Effect.gen(function*() {
      yield* ready.await
      if (terminal !== undefined) return yield* Effect.fail(terminal)
      if (closing) return yield* connectionError("Connection is closed", undefined, true)
      if (current === undefined || !current.active) return yield* connectionError("Connection is not ready")
      return current
    }).pipe(
      Effect.timeout(waitConnectionTimeout),
      Effect.catchTag("TimeoutError", () => Effect.fail(connectionError("Timed out waiting for connection readiness")))
    )

    const awaitPhysical = (logical: Logical) =>
      Effect.gen(function*() {
        yield* logical.ready.await
        if (logical.error !== undefined) return yield* Effect.fail(logical.error)
        if (logical.closed || closing) return yield* channelError("Channel is closed")
        const physical = logical.physical
        if (physical === undefined || !physical.active || !physical.epoch.active) {
          return yield* channelError("Channel is not ready")
        }
        return physical
      }).pipe(
        Effect.timeout(logical.options.waitChannelTimeout ?? "10 seconds"),
        Effect.catchTag("TimeoutError", () => Effect.fail(channelError("Timed out waiting for channel readiness")))
      )

    const checkOutboundFrame = (epoch: Epoch, frame: Uint8Array): Uint8Array => {
      if (frame.byteLength > epoch.frameMax) {
        throw new AMQPError.AMQPProtocolError({ reason: "Encoded frame exceeds negotiated frameMax" })
      }
      return frame
    }

    const encodeMethodFrame = (epoch: Epoch, channel: number, classId: number, methodId: number, fields: Fields = {}) =>
      wire(() => checkOutboundFrame(epoch, Codec.encodeMethod(channel, classId, methodId, fields)))

    const submit = (
      epoch: Epoch,
      frames: Array<Uint8Array>,
      publish = false,
      physical?: Physical,
      priority = false
    ): Write => {
      if (
        !epoch.active || (epoch.brokerCloseError !== undefined && !priority) ||
        (physical !== undefined && !physical.active)
      ) {
        throw (publish
          ? publishError("NotSent", "Session retired before write admission")
          : connectionError("Session retired before write admission"))
      }
      const first = frames[0]
      if (first === undefined) throw new AMQPError.AMQPProtocolError({ reason: "Empty outbound command" })
      const bytes = frames.reduce((total, frame) => total + checkOutboundFrame(epoch, frame).byteLength, 0)
      const channel = first === Codec.PROTOCOL_HEADER
        ? 0
        : new DataView(first.buffer, first.byteOffset, first.byteLength).getUint16(1)
      // A small separately bounded reserve keeps flow, close and settlement controls admissible under publish load.
      const commandLimit = maxPending + (priority ? 16 : 0)
      const byteLimit = maxOutboundBytes + (priority ? 65536 : 0)
      if (epoch.outboundCommands >= commandLimit || epoch.outboundBytes + bytes > byteLimit) {
        throw (publish
          ? publishError("NotSent", "Outbound admission limit exceeded")
          : channelError("Outbound admission limit exceeded"))
      }
      const command: Write = {
        frames,
        bytes,
        done: Deferred.makeUnsafe(),
        drained: Deferred.makeUnsafe(),
        publish,
        priority,
        channel,
        ...(physical === undefined ? {} : { physical }),
        started: false,
        cancelled: false
      }
      epoch.outboundBytes += bytes
      epoch.outboundCommands++
      const commands = priority ? epoch.control : epoch.data
      commands.push(command)
      epoch.wakeWriter.openUnsafe()
      return command
    }

    const enqueue = Effect.fnUntraced(function*(
      epoch: Epoch,
      frames: Array<Uint8Array>,
      publish = false,
      physical?: Physical,
      priority = false
    ): Effect.fn.Return<void, Error> {
      const command = yield* Effect.try({
        try: () => submit(epoch, frames, publish, physical, priority),
        catch: (cause) =>
          cause instanceof AMQPError.AMQPPublishError ||
            cause instanceof AMQPError.AMQPChannelError || cause instanceof AMQPError.AMQPConnectionError
            ? cause
            : protocolError(cause)
      })
      return yield* Deferred.await(command.done).pipe(Effect.onInterrupt(() =>
        Effect.sync(() => {
          // The writer, not the caller, owns a complete content sequence after admission.
          if (!command.started) command.cancelled = true
        })
      ))
    })

    const sendMethod = (
      epoch: Epoch,
      channel: number,
      classId: number,
      methodId: number,
      fields: Fields = {},
      priority = false
    ) =>
      encodeMethodFrame(epoch, channel, classId, methodId, fields).pipe(
        Effect.flatMap((frame) => enqueue(epoch, [frame], false, epoch.channels.get(channel), priority))
      )

    // The reader never waits for transport backpressure while acknowledging broker controls.
    const sendControl = (epoch: Epoch, channel: number, classId: number, methodId: number, fields: Fields = {}) =>
      wire(() => submit(epoch, [Codec.encodeMethod(channel, classId, methodId, fields)], false, undefined, true))

    const writerLoop = Effect.fnUntraced(function*(epoch: Epoch): Effect.fn.Return<void> {
      while (epoch.active) {
        let command = epoch.control.shift()
        if (command === undefined) {
          const index = epoch.data.findIndex((item) =>
            !item.publish || item.cancelled ||
            item.physical?.active === false || (epoch.publishGate.isOpen() && (item.physical?.flow.isOpen() ?? true))
          )
          if (index >= 0) command = epoch.data.splice(index, 1)[0]
        }
        if (command === undefined) {
          epoch.wakeWriter.closeUnsafe()
          yield* epoch.wakeWriter.await
          continue
        }
        epoch.writing = command
        if (
          command.cancelled || (epoch.brokerCloseError !== undefined && !command.priority) ||
          (command.physical !== undefined && !command.physical.active)
        ) {
          finish(
            command.done,
            Effect.fail(
              command.publish
                ? publishError("NotSent", "Publish cancelled before transport write")
                : channelError("Channel retired before transport write")
            )
          )
        } else {
          command.started = true
          // Only this fiber writes. A caller cannot interrupt a partially written content sequence.
          const result = yield* Effect.exit(Effect.forEach(command.frames, (frame) =>
            Effect.suspend((): Effect.Effect<void, Socket.SocketError | AMQPError.AMQPChannelError> => {
              if (!epoch.active || command.physical?.active === false) {
                return Effect.fail(channelError("Write session retired"))
              }
              return epoch.writer.write(frame)
            }), {
            discard: true
          }))
          if (Exit.isFailure(result)) {
            if (command.physical?.active === false && epoch.active) {
              finish(
                command.done,
                Effect.fail(
                  command.publish
                    ? publishError("Unknown", "Channel retired while content was being written", result.cause)
                    : channelError("Channel retired while method was being written", result.cause)
                )
              )
            } else failEpoch(epoch, connectionError("Transport write failed", result.cause))
          } else {
            epoch.lastWrite = yield* Clock.currentTimeMillis
            finish(command.done, Effect.void)
          }
        }
        epoch.outboundBytes -= command.bytes
        epoch.outboundCommands--
        epoch.writing = undefined
        finish(command.drained, Effect.void)
      }
    })

    const reclaimPhysical = Effect.fnUntraced(function*(physical: Physical): Effect.fn.Return<void> {
      const epoch = physical.epoch
      retirePhysical(physical, channelError("Channel closed"))
      // A close-ok is the protocol barrier. Also drain local RPC ownership and every old write before reuse.
      yield* physical.rpc.withPermit(Effect.gen(function*() {
        if (!epoch.active || epoch.channels.get(physical.id) !== physical) {
          return
        }
        for (const commands of [epoch.control, epoch.data]) {
          for (let index = commands.length - 1; index >= 0; index--) {
            const command = commands[index]
            if (command === undefined || command.channel !== physical.id) {
              continue
            }
            commands.splice(index, 1)
            epoch.outboundBytes -= command.bytes
            epoch.outboundCommands--
            command.cancelled = true
            finish(
              command.done,
              Effect.fail(
                command.publish
                  ? publishError("NotSent", "Channel closed before queued publish was written")
                  : channelError("Channel closed before queued control was written")
              )
            )
            finish(command.drained, Effect.void)
          }
        }
        const writing = epoch.writing
        if (writing?.channel === physical.id) {
          yield* Deferred.await(writing.drained)
        }
        if (!epoch.active || epoch.channels.get(physical.id) !== physical) {
          return
        }
        epoch.channels.delete(physical.id)
        epoch.freeChannels.push(physical.id)
      }))
    })

    const rpc = Effect.fnUntraced(function*(
      epoch: Epoch,
      physical: Physical | undefined,
      classId: number,
      methodId: number,
      fields: Fields,
      expected: ReadonlyArray<number>,
      timeout: Duration.Input = connectionTimeout
    ): Effect.fn.Return<Reply, Error> {
      // Reject local encoding and size errors before a pending reply slot is installed.
      const frame = yield* encodeMethodFrame(epoch, physical?.id ?? 0, classId, methodId, fields)
      const mutex = physical?.rpc ?? epoch.rpc
      return yield* mutex.withPermit(Effect.gen(function*() {
        if (!epoch.active || (physical !== undefined && !physical.active)) {
          return yield* connectionError("Session retired before RPC admission")
        }
        const pending: Pending = { expected, done: Deferred.makeUnsafe() }
        if (physical === undefined) {
          epoch.pending = pending
        } else physical.pending = pending
        const operation = enqueue(epoch, [frame], false, physical).pipe(
          Effect.andThen(Deferred.await(pending.done)),
          Effect.timeout(timeout),
          Effect.catchTag("TimeoutError", () => {
            const error = connectionError(`Timed out waiting for ${classId}.${methodId}`)
            failEpoch(epoch, error)
            return Effect.fail(error)
          }),
          Effect.onInterrupt(() =>
            Effect.sync(() =>
              failEpoch(
                epoch,
                connectionError(`RPC ${classId}.${methodId} interrupted after admission`)
              )
            )
          )
        )
        return yield* operation.pipe(Effect.ensuring(Effect.sync(() => {
          if (physical === undefined) {
            if (epoch.pending === pending) {
              epoch.pending = undefined
            }
          } else if (physical.pending === pending) {
            physical.pending = undefined
          }
        })))
      }))
    })

    const completeReply = (pending: Pending | undefined, method: Codec.Method, message?: Message): void => {
      if (
        pending === undefined || Deferred.isDoneUnsafe(pending.done) || !pending.expected.includes(methodKey(method))
      ) {
        throw new AMQPError.AMQPProtocolError({ reason: `Unexpected reply ${method.classId}.${method.methodId}` })
      }
      finish(pending.done, Effect.succeed({ method, ...(message === undefined ? {} : { message }) }))
    }

    const reserve = (epoch: Epoch, bytes: number): () => void => {
      if (epoch.bufferedBytes + bytes > maxBufferedBytes) {
        throw new AMQPError.AMQPProtocolError({ reason: "Inbound content memory limit exceeded" })
      }
      epoch.bufferedBytes += bytes
      let released = false
      return () => {
        if (released) {
          return
        }
        released = true
        epoch.bufferedBytes -= bytes
      }
    }

    const requeueBufferedConsumer = (consumer: Consumer): void => {
      const batches = new Map<Physical, Array<Uint8Array>>()
      while (true) {
        const envelope = Queue.takeUnsafe(consumer.mailbox)
        if (envelope === undefined || Exit.isFailure(envelope)) {
          break
        }
        envelope.value.release()
        const capability = settlements.get(envelope.value.value)
        if (capability === undefined || capability.settled) {
          continue
        }
        capability.revoked = true
        const physical = capability.physical
        physical.settlements.delete(capability.tag)
        if (!physical.active || !physical.epoch.active || physical.logical.closed || closing) {
          continue
        }
        const frames = batches.get(physical) ?? []
        frames.push(Codec.encodeMethod(physical.id, 60, 90, { deliveryTag: capability.tag, requeue: true }))
        batches.set(physical, frames)
      }
      for (const [physical, frames] of batches) {
        try {
          // Only mailbox-resident deliveries are revoked. Messages already handed to application handlers
          // retain their original settlement capability, including after a broker-initiated consumer cancel.
          submit(physical.epoch, frames, false, physical, true)
        } catch (cause) {
          failEpoch(
            physical.epoch,
            connectionError("Failed to requeue cancelled consumer's buffered deliveries", cause)
          )
        }
      }
    }

    const deliverContent = (physical: Physical): void => {
      const content = physical.content
      if (content?.body === undefined || content.properties === undefined || content.release === undefined) {
        throw new AMQPError.AMQPProtocolError({ reason: "Content completed without header" })
      }
      physical.content = undefined
      const method = content.method
      if (method.methodId === 50) {
        const value: AMQPTypes.ReturnedMessage = {
          content: content.body,
          properties: content.properties,
          fields: {
            replyCode: fieldNumber(method, "replyCode"),
            replyText: fieldString(method, "replyText"),
            exchange: fieldString(method, "exchange"),
            routingKey: fieldString(method, "routingKey")
          }
        }
        if (!Queue.offerUnsafe(physical.logical.returned, { value, physical, release: content.release })) {
          content.release()
          throw new AMQPError.AMQPProtocolError({ reason: "Returned-message mailbox overflow" })
        }
        return
      }
      const tag = fieldBigInt(method, "deliveryTag")
      if (tag <= physical.lastDeliveryTag) {
        content.release()
        throw new AMQPError.AMQPProtocolError({ reason: "Non-increasing delivery tag" })
      }
      physical.lastDeliveryTag = tag
      const value: Message = {
        content: content.body,
        properties: content.properties,
        fields: {
          consumerTag: method.methodId === 60 ? fieldString(method, "consumerTag") : "",
          deliveryTag: tag,
          redelivered: method.fields.redelivered === true,
          exchange: fieldString(method, "exchange"),
          routingKey: fieldString(method, "routingKey"),
          ...(method.methodId === 71 ? { messageCount: fieldNumber(method, "messageCount") } : {})
        }
      }
      if (method.methodId === 71) {
        const capability: Settlement = { physical, tag, settled: false, revoked: false }
        settlements.set(value, capability)
        physical.settlements.set(tag, capability)
        content.release()
        completeReply(physical.pending, method, value)
        return
      }
      const consumer = physical.consumers.get(value.fields.consumerTag)
      if (consumer === undefined) {
        content.release()
        throw new AMQPError.AMQPProtocolError({ reason: "Delivery for unknown consumer" })
      }
      if (!consumer.active) {
        content.release()
        submit(
          physical.epoch,
          [Codec.encodeMethod(physical.id, 60, 90, { deliveryTag: tag, requeue: true })],
          false,
          physical,
          true
        )
        return
      }
      const capability: Settlement = { physical, tag, settled: false, revoked: false }
      settlements.set(value, capability)
      physical.settlements.set(tag, capability)
      if (!Queue.offerUnsafe(consumer.mailbox, { value, physical, release: content.release })) {
        content.release()
        throw new AMQPError.AMQPProtocolError({ reason: "Consumer mailbox overflow" })
      }
    }

    const openPhysical = Effect.fnUntraced(
      function*(epoch: Epoch, logical: Logical): Effect.fn.Return<Physical, Error> {
        if (logical.closed || closing) {
          return yield* channelError("Channel closed before recovery")
        }
        const reusable = epoch.freeChannels.pop()
        if (reusable === undefined && epoch.nextChannel > epoch.channelMax) {
          if (
            Array.from(epoch.channels.values()).some((physical) =>
              !physical.active
            )
          ) {
            const error = connectionError("Channel number space requires a fresh protocol session")
            failEpoch(epoch, error)
            return yield* error
          }
          return yield* channelError("Negotiated simultaneous channel limit exhausted")
        }
        const physical: Physical = {
          id: reusable ?? epoch.nextChannel++,
          epoch,
          logical,
          rpc: Semaphore.makeUnsafe(1),
          publishing: Semaphore.makeUnsafe(1),
          flow: Latch.makeUnsafe(true),
          confirmCapacity: Latch.makeUnsafe(true),
          admissionClosed: Deferred.makeUnsafe(),
          confirms: new Map(),
          settlements: new Map(),
          consumers: new Map(),
          active: true,
          closing: false,
          pending: undefined,
          content: undefined,
          sequence: BigInt(0),
          lastDeliveryTag: BigInt(0)
        }
        epoch.channels.set(physical.id, physical)
        logical.physical = physical
        yield* rpc(epoch, physical, 20, 10, { reserved1: "" }, [20011])
        if (logical.options.confirm === true) {
          yield* rpc(epoch, physical, 85, 10, { noWait: false }, [85011])
        }
        yield* rpc(epoch, physical, 60, 10, {
          prefetchSize: 0,
          prefetchCount: logical.prefetch,
          global: false
        }, [60011])
        if (logical.globalPrefetch !== undefined) {
          yield* rpc(epoch, physical, 60, 10, {
            prefetchSize: 0,
            prefetchCount: logical.globalPrefetch,
            global: true
          }, [60011])
        }
        return physical
      }
    )

    const declareExchange = (physical: Physical, declaration: ExchangeDeclaration) =>
      rpc(
        physical.epoch,
        physical,
        40,
        10,
        {
          reserved1: 0,
          exchange: declaration.exchange,
          type: declaration.type,
          passive: false,
          durable: declaration.options.durable ?? true,
          autoDelete: declaration.options.autoDelete ?? false,
          internal: declaration.options.internal ?? false,
          noWait: false,
          arguments: declaration.options.arguments ?? {}
        },
        [40011]
      )

    const declareQueue = (physical: Physical, queue: string, opts: AMQPTypes.QueueOptions, passive = false) =>
      rpc(
        physical.epoch,
        physical,
        50,
        10,
        {
          reserved1: 0,
          queue,
          passive,
          durable: opts.durable ?? true,
          exclusive: opts.exclusive ?? false,
          autoDelete: opts.autoDelete ?? false,
          noWait: false,
          arguments: opts.arguments ?? {}
        },
        [50011]
      ).pipe(Effect.flatMap((reply) => wire(() => replyQueue(reply.method))))

    const applyBinding = (physical: Physical, binding: Binding, remove = false) =>
      rpc(
        physical.epoch,
        physical,
        binding.queue === undefined ? 40 : 50,
        remove ? (binding.queue === undefined ? 40 : 50) : (binding.queue === undefined ? 30 : 20),
        {
          reserved1: 0,
          ...(binding.queue === undefined
            ? { destination: binding.destination ?? "", source: binding.source, noWait: false }
            : { queue: nameOf(binding.queue), exchange: binding.source, ...(remove ? {} : { noWait: false }) }),
          routingKey: binding.routingKey,
          arguments: binding.arguments
        },
        [binding.queue === undefined ? (remove ? 40051 : 40031) : (remove ? 50051 : 50021)]
      ).pipe(Effect.asVoid)

    const startConsumer = Effect.fnUntraced(
      function*(physical: Physical, consumer: Consumer): Effect.fn.Return<void, Error> {
        if (!consumer.active) return
        const count = consumer.options.prefetch ?? physical.logical.prefetch
        yield* rpc(physical.epoch, physical, 60, 10, { prefetchSize: 0, prefetchCount: count, global: false }, [60011])
        consumer.tag = ""
        const reply = yield* rpc(physical.epoch, physical, 60, 20, {
          reserved1: 0,
          queue: nameOf(consumer.queue),
          consumerTag: consumer.options.consumerTag ?? "",
          noLocal: false,
          noAck: false,
          exclusive: consumer.options.exclusive ?? false,
          noWait: false,
          arguments: consumer.options.arguments ?? {}
        }, [60021])
        consumer.tag = yield* wire(() => fieldString(reply.method, "consumerTag"))
        physical.consumers.set(consumer.tag, consumer)
      }
    )

    const restorePhase = Effect.fnUntraced(function*(logical: Logical, phase: number): Effect.fn.Return<void, Error> {
      const physical = logical.physical
      if (physical === undefined || logical.closed || logical.error !== undefined) return
      if (phase === 0) {
        for (const declaration of logical.exchanges.values()) yield* declareExchange(physical, declaration)
      } else if (phase === 1) {
        for (const declaration of logical.queues) {
          declaration.current = yield* declareQueue(physical, declaration.requested, declaration.options)
        }
      } else if (phase === 2) {
        for (const binding of logical.bindings) yield* applyBinding(physical, binding)
      } else {
        for (const consumer of logical.consumers) yield* startConsumer(physical, consumer)
        if (physical.active && physical.epoch.active) logical.ready.openUnsafe()
      }
    })

    const restoreOne = Effect.fnUntraced(function*(epoch: Epoch, logical: Logical): Effect.fn.Return<void> {
      const result = yield* Effect.exit(Effect.gen(function*() {
        yield* openPhysical(epoch, logical)
        for (let phase = 0; phase < 4; phase++) yield* restorePhase(logical, phase)
      }))
      if (Exit.isFailure(result) && epoch.active && !logical.closed) {
        const error = Cause.findErrorOption(result.cause)
        failLogical(
          logical,
          Option.isSome(error) ? error.value : channelError("Channel topology recovery failed", result.cause)
        )
      }
    })

    const handleMethod = Effect.fnUntraced(
      function*(
        epoch: Epoch,
        channel: number,
        method: Codec.Method,
        methodBytes: number
      ): Effect.fn.Return<void, Error> {
        const key = methodKey(method)
        if (channel === 0) {
          if (key === 10050) {
            const replyCode = fieldNumber(method, "replyCode")
            const error = new AMQPError.AMQPConnectionError({
              reason: fieldString(method, "replyText"),
              replyCode,
              classId: fieldNumber(method, "classId"),
              methodId: fieldNumber(method, "methodId"),
              permanent: [402, 403, 404, 405, 406, 501, 502, 503, 504, 505, 530, 540].includes(replyCode)
            })
            // Retire application capabilities promptly. The writer gets a bounded chance to send close-ok while
            // this reader remains able to observe transport failure, including a disconnected writer that suspends.
            epoch.brokerCloseError = error
            epoch.blocked = "Broker closing connection"
            epoch.publishGate.closeUnsafe()
            ready.closeUnsafe()
            if (current === epoch) current = undefined
            if (epoch.pending !== undefined) finish(epoch.pending.done, Effect.fail(error))
            for (const physical of epoch.channels.values()) retirePhysical(physical, error)
            yield* sendMethod(epoch, 0, 10, 51, {}, true).pipe(
              Effect.timeout("1 second"),
              Effect.ignore,
              Effect.ensuring(Effect.sync(() => failEpoch(epoch, error))),
              Effect.forkIn(epoch.scope)
            )
          } else if (key === 10060) {
            epoch.blocked = fieldString(method, "reason")
            epoch.publishGate.closeUnsafe()
            yield* SubscriptionRef.update(state, (value) => ({ ...value, blocked: fieldString(method, "reason") }))
          } else if (key === 10061) {
            epoch.blocked = undefined
            epoch.publishGate.openUnsafe()
            epoch.wakeWriter.openUnsafe()
            yield* SubscriptionRef.update(state, (value) => {
              const { blocked: _blocked, ...rest } = value
              return rest
            })
          } else if (key === 10020) {
            return yield* connectionError("Broker requested unsupported authentication challenge", undefined, true)
          } else {
            completeReply(epoch.pending, method)
          }
          return
        }
        const physical = epoch.channels.get(channel)
        if (physical === undefined) {
          return yield* new AMQPError.AMQPProtocolError({ reason: "Frame on unopened channel" })
        }
        if (!physical.active) {
          // Unsafe retirements retain their number until a close barrier has drained every old slot and write.
          if (key === 20040) yield* sendControl(epoch, channel, 20, 41)
          return
        }
        if (physical.content !== undefined && key !== 20040) {
          return yield* new AMQPError.AMQPProtocolError({ reason: "Method interleaved with content frames" })
        }
        if (key === 20040) {
          const error = new AMQPError.AMQPChannelError({
            reason: fieldString(method, "replyText"),
            replyCode: fieldNumber(method, "replyCode"),
            classId: fieldNumber(method, "classId"),
            methodId: fieldNumber(method, "methodId")
          })
          // Admission is invalidated synchronously before recovery starts.
          const recover = physical.logical.ready.isOpen()
          retirePhysical(physical, error)
          const acknowledged = yield* sendControl(epoch, channel, 20, 41)
          if (recover && !physical.logical.closed && physical.logical.error === undefined && !closing) {
            if (physical.logical.queues.some((queue) => queue.requested === "" || queue.options.autoDelete === true)) {
              // Ephemeral queue identity may move, including references held by other channels. Recover all
              // dependent resources together rather than silently leaving their bindings or consumers stale.
              failEpoch(epoch, connectionError("Ephemeral queue owner channel retired", error))
              return
            }
          }
          yield* Effect.gen(function*() {
            yield* Deferred.await(acknowledged.done).pipe(
              Effect.andThen(reclaimPhysical(physical)),
              Effect.timeout(shutdownTimeout),
              Effect.catch((cause) =>
                Effect.sync(() => failEpoch(epoch, connectionError("Channel close barrier failed", cause)))
              )
            )
            if (
              recover && epoch.active && !physical.logical.closed && physical.logical.error === undefined && !closing
            ) {
              yield* restoreOne(epoch, physical.logical)
            }
          }).pipe(Effect.forkIn(epoch.scope))
        } else if (key === 20020) {
          if (method.fields.active === true) {
            physical.flow.openUnsafe()
            epoch.wakeWriter.openUnsafe()
          } else physical.flow.closeUnsafe()
          if (!physical.closing) yield* sendControl(epoch, channel, 20, 21, { active: method.fields.active === true })
        } else if (key === 60080 || key === 60120) {
          const tag = fieldBigInt(method, "deliveryTag")
          if (tag > physical.sequence) {
            return yield* new AMQPError.AMQPProtocolError({ reason: "Invalid publisher confirm tag" })
          }
          const multiple = method.fields.multiple === true
          for (const [sequence, confirm] of physical.confirms) {
            if (sequence === tag || (multiple && (tag === BigInt(0) || sequence <= tag))) {
              physical.confirms.delete(sequence)
              finish(
                confirm,
                key === 60080 ? Effect.void : Effect.fail(publishError("Nacked", "Broker nacked publish"))
              )
            }
          }
          if (physical.confirms.size < (physical.logical.options.maxUnconfirmed ?? 1024)) {
            physical.confirmCapacity.openUnsafe()
          }
        } else if (key === 60030) {
          const tag = fieldString(method, "consumerTag")
          const consumer = Array.from(physical.logical.consumers).find((item) => item.tag === tag)
          if (consumer !== undefined) {
            consumer.active = false
            requeueBufferedConsumer(consumer)
            physical.logical.consumers.delete(consumer)
            yield* Queue.fail(consumer.mailbox, channelError(`Broker cancelled consumer ${tag}`))
          }
          if (method.fields.noWait !== true && !physical.closing) {
            yield* sendControl(epoch, channel, 60, 31, { consumerTag: tag })
          }
        } else if (key === 60060 || key === 60050 || key === 60071) {
          if (physical.content !== undefined) {
            return yield* new AMQPError.AMQPProtocolError({
              reason: "Interleaved content methods"
            })
          }
          if (key === 60071 && !physical.pending?.expected.includes(key)) {
            return yield* new AMQPError.AMQPProtocolError({ reason: "Unsolicited basic.get-ok" })
          }
          const release = yield* wire(() => reserve(epoch, methodBytes + 128))
          physical.content = {
            method,
            properties: undefined,
            body: undefined,
            offset: 0,
            release
          }
        } else {
          // Consume tags must become visible before a following delivery in the same read batch.
          if (key === 60021) {
            const tag = fieldString(method, "consumerTag")
            const consumer = Array.from(physical.logical.consumers).find((item) => item.active && item.tag === "")
            if (consumer !== undefined) {
              consumer.tag = tag
              physical.consumers.set(tag, consumer)
            }
          }
          completeReply(physical.pending, method)
        }
      }
    )

    const dispatch = Effect.fnUntraced(function*(epoch: Epoch, frame: Codec.Frame): Effect.fn.Return<void, Error> {
      if (!epoch.active) return
      if (frame.payload.byteLength + 8 > epoch.frameMax) {
        return yield* new AMQPError.AMQPProtocolError({ reason: "Frame exceeds negotiated frameMax" })
      }
      if (frame.type === 8) {
        if (frame.channel !== 0 || frame.payload.byteLength !== 0) {
          return yield* new AMQPError.AMQPProtocolError({ reason: "Invalid heartbeat frame" })
        }
        return
      }
      if (frame.type === 1) {
        const method = yield* wire(() => Codec.decodeMethod(frame.payload))
        return yield* handleMethod(epoch, frame.channel, method, frame.payload.byteLength + 8)
      }
      const physical = epoch.channels.get(frame.channel)
      if (physical === undefined || !physical.active) {
        return yield* new AMQPError.AMQPProtocolError({ reason: "Content frame on unavailable channel" })
      }
      const content = physical.content
      if (content === undefined) return yield* new AMQPError.AMQPProtocolError({ reason: "Content without method" })
      if (frame.type === 2) {
        if (content.body !== undefined) {
          return yield* new AMQPError.AMQPProtocolError({ reason: "Duplicate content header" })
        }
        yield* wire(() => {
          // Charge raw header bytes and envelope/property overhead before decoding a potentially expanding table.
          content.release = combineReleases(content.release, reserve(epoch, frame.payload.byteLength + 8 + 128))
          const header = Codec.decodeContentHeader(frame.payload)
          if (header.bodySize > BigInt(maxMessageBytes)) {
            throw new AMQPError.AMQPProtocolError({ reason: "Message exceeds maxMessageBytes" })
          }
          const bytes = Number(header.bodySize)
          content.release = combineReleases(content.release, reserve(epoch, bytes))
          content.properties = header.properties
          content.body = new Uint8Array(bytes)
          if (bytes === 0) deliverContent(physical)
        })
      } else if (frame.type === 3) {
        yield* wire(() => {
          if (
            content.body === undefined || frame.payload.byteLength === 0 ||
            content.offset + frame.payload.byteLength > content.body.byteLength
          ) {
            throw new AMQPError.AMQPProtocolError({ reason: "Invalid content body size or sequence" })
          }
          content.body.set(frame.payload, content.offset)
          content.offset += frame.payload.byteLength
          if (content.offset === content.body.byteLength) deliverContent(physical)
        })
      } else {
        return yield* new AMQPError.AMQPProtocolError({ reason: `Unsupported frame type ${frame.type}` })
      }
    })

    const handshake = Effect.fnUntraced(function*(epoch: Epoch): Effect.fn.Return<void, Error> {
      const start: Pending = { expected: [10010], done: Deferred.makeUnsafe() }
      epoch.pending = start
      yield* enqueue(epoch, [Codec.PROTOCOL_HEADER])
      const { method } = yield* Deferred.await(start.done)
      epoch.pending = undefined
      const serverProperties = yield* wire(() => fieldTable(method, "serverProperties"))
      const mechanisms = yield* wire(() => fieldString(method, "mechanisms"))
      const locales = yield* wire(() => fieldString(method, "locales"))
      if (
        method.fields.versionMajor !== 0 || method.fields.versionMinor !== 9 || !mechanisms.split(" ").includes("PLAIN")
      ) {
        return yield* connectionError("Broker does not support AMQP 0-9-1 PLAIN authentication", undefined, true)
      }
      if (!locales.split(" ").includes("en_US")) {
        return yield* connectionError("Broker does not support en_US locale", undefined, true)
      }
      epoch.serverProperties = serverProperties
      const password = Redacted.isRedacted(secret) ? Redacted.value(secret) : secret
      const username = options.username ?? "guest"
      if (username.includes("\0") || password.includes("\0")) {
        return yield* connectionError("PLAIN credentials must not contain NUL", undefined, true)
      }
      const tune = yield* rpc(epoch, undefined, 10, 11, {
        clientProperties: {
          product: "effect-messaging",
          version: "0.8.0",
          platform: "Effect",
          capabilities: {
            publisher_confirms: true,
            "exchange_exchange_bindings": true,
            "basic.nack": true,
            "consumer_cancel_notify": true,
            "connection.blocked": true,
            "authentication_failure_close": true
          },
          ...options.clientProperties,
          ...(options.connectionName === undefined ? {} : { connection_name: options.connectionName })
        },
        mechanism: "PLAIN",
        response: new TextEncoder().encode(`\0${username}\0${password}`),
        locale: "en_US"
      }, [10030])
      const serverFrame = yield* wire(() => fieldNumber(tune.method, "frameMax"))
      const serverChannel = yield* wire(() => fieldNumber(tune.method, "channelMax"))
      const serverHeartbeat = yield* wire(() => fieldNumber(tune.method, "heartbeat"))
      if (serverFrame !== 0 && serverFrame < 4096) {
        return yield* connectionError("Broker advertised invalid frameMax", undefined, true)
      }
      const negotiate = (server: number, client: number) =>
        server === 0 ? client : client === 0 ? server : Math.min(server, client)
      const frameMax = negotiate(serverFrame, requestedFrameMax)
      // Zero means no protocol limit, not an unbounded local allocation.
      epoch.frameMax = frameMax === 0 ? maxBufferedBytes : frameMax
      epoch.channelMax = negotiate(serverChannel, requestedChannelMax) || 65535
      epoch.heartbeat = serverHeartbeat === 0 || requestedHeartbeat === 0
        ? Math.max(serverHeartbeat, requestedHeartbeat)
        : Math.min(serverHeartbeat, requestedHeartbeat)
      yield* sendMethod(epoch, 0, 10, 31, {
        channelMax: negotiate(serverChannel, requestedChannelMax),
        frameMax,
        heartbeat: epoch.heartbeat
      })
      yield* rpc(epoch, undefined, 10, 40, {
        virtualHost: options.virtualHost ?? "/",
        reserved1: "",
        outOfBand: false
      }, [10041])
    })

    const heartbeatPass = Effect.fnUntraced(function*(epoch: Epoch): Effect.fn.Return<void> {
      if (!epoch.active) return
      const now = yield* Clock.currentTimeMillis
      if (now - epoch.lastRead >= epoch.heartbeat * 1000) {
        failEpoch(epoch, epoch.brokerCloseError ?? connectionError("Heartbeat receive timeout"))
        return
      }
      if (now - epoch.lastWrite >= epoch.heartbeat * 500) {
        yield* wire(() => {
          // Receive liveness never waits for the writer. Coalesce heartbeat sends to one bounded command
          // while transport backpressure stalls the writer, so each scheduled pass can still check lastRead.
          if (epoch.heartbeatWrite !== undefined && !Deferred.isDoneUnsafe(epoch.heartbeatWrite.done)) return
          epoch.heartbeatWrite = submit(epoch, [Codec.encodeFrame(8, 0, new Uint8Array())], false, undefined, true)
        }).pipe(
          Effect.catch((error) => Effect.sync(() => failEpoch(epoch, connectionError("Heartbeat write failed", error))))
        )
      }
    })

    const session = Effect.gen(function*() {
      if (closing) return
      generation++
      yield* setState({ state: generation === 1 ? "Connecting" : "Reconnecting", generation })
      return yield* Effect.scoped(
        Effect.gen(function*() {
          const epochScope = yield* Effect.scope
          const socket = yield* socketFactory.pipe(
            Effect.mapError((cause) => connectionError("Transport construction failed", cause))
          )
          // Exactly one acquisition on a fresh Socket owns the entire physical session.
          const pull = yield* Socket.readerBytes(socket).pipe(
            Effect.timeout(connectionTimeout),
            Effect.mapError((cause) => connectionError("Transport connection failed", cause))
          )
          const writer = yield* socket.writer
          const now = yield* Clock.currentTimeMillis
          const epoch: Epoch = {
            generation,
            scope: epochScope,
            writer,
            failure: Deferred.makeUnsafe(),
            wakeWriter: Latch.makeUnsafe(),
            publishGate: Latch.makeUnsafe(true),
            control: [],
            data: [],
            channels: new Map(),
            freeChannels: [],
            rpc: Semaphore.makeUnsafe(1),
            active: true,
            nextChannel: 1,
            pending: undefined,
            writing: undefined,
            outboundBytes: 0,
            outboundCommands: 0,
            bufferedBytes: 0,
            frameMax: Math.max(requestedFrameMax || maxBufferedBytes, 4096),
            channelMax: 65535,
            heartbeat: 0,
            heartbeatWrite: undefined,
            lastRead: now,
            lastWrite: now,
            serverProperties: {},
            brokerCloseError: undefined,
            blocked: undefined
          }
          current = epoch
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => failEpoch(epoch, connectionError("Physical session retired")))
          )
          yield* writerLoop(epoch).pipe(
            Effect.ensuring(Effect.sync(() => {
              const writing = epoch.writing
              epoch.writing = undefined
              if (writing !== undefined) finish(writing.drained, Effect.void)
            })),
            Effect.forkScoped
          )
          const decoder = new Codec.FrameDecoder(maxBufferedBytes, maxBufferedBytes)
          const readerLoop = Effect.gen(function*() {
            while (epoch.active) {
              const batch = yield* pull
              epoch.lastRead = yield* Clock.currentTimeMillis
              for (const chunk of batch) {
                const frames = yield* wire(() => decoder.feed(chunk))
                for (const frame of frames) yield* dispatch(epoch, frame)
              }
            }
          }).pipe(Effect.catchCause((cause) =>
            Effect.sync(() => {
              const error = Cause.findErrorOption(cause)
              failEpoch(
                epoch,
                epoch.brokerCloseError ??
                  connectionError(
                    "AMQP reader failed",
                    cause,
                    Option.isSome(error) && error.value instanceof AMQPError.AMQPProtocolError
                  )
              )
            })
          ))
          yield* readerLoop.pipe(Effect.forkScoped)
          yield* setState({ state: "Handshaking", generation })
          yield* handshake(epoch).pipe(
            Effect.timeout(connectionTimeout),
            Effect.mapError((error) =>
              error instanceof AMQPError.AMQPConnectionError
                ? error
                : connectionError("AMQP handshake failed", error, error instanceof AMQPError.AMQPProtocolError)
            )
          )
          if (epoch.heartbeat > 0) {
            yield* heartbeatPass(epoch).pipe(
              Effect.repeat(Schedule.spaced(epoch.heartbeat * 500)),
              Effect.forkScoped
            )
          }
          yield* setState({
            state: "Recovering",
            generation,
            ...(epoch.blocked === undefined ? {} : { blocked: epoch.blocked })
          })
          const recoverable = Array.from(logicals).filter((logical) => !logical.closed && logical.error === undefined)
          for (const logical of recoverable) {
            yield* openPhysical(epoch, logical).pipe(Effect.catch((error) => {
              if (!epoch.active) return Effect.fail(connectionError("Session lost while opening channels", error))
              failLogical(logical, error)
              return Effect.void
            }))
          }
          // All exchanges and queues precede all bindings, including cross-channel dependencies.
          for (let phase = 0; phase < 4; phase++) {
            for (const logical of recoverable) {
              yield* restorePhase(logical, phase).pipe(Effect.catch((error) => {
                if (!epoch.active) return Effect.fail(connectionError("Session lost during topology recovery", error))
                failLogical(logical, error)
                return Effect.void
              }))
            }
          }
          if (!epoch.active) return yield* Deferred.await(epoch.failure)
          yield* setState({
            state: "Ready",
            generation,
            ...(epoch.blocked === undefined ? {} : { blocked: epoch.blocked })
          })
          ready.openUnsafe()
          return yield* Deferred.await(epoch.failure)
        }).pipe(Effect.ensuring(Effect.sync(() => {
          if (current?.generation === generation) failEpoch(current, connectionError("Physical session retired"))
        })))
      )
    }).pipe(Effect.mapError((error) =>
      error instanceof AMQPError.AMQPConnectionError
        ? error
        : connectionError("Physical connection failed", error)
    ))

    const supervisor = session.pipe(
      Effect.tapError((error) => setState({ state: "Reconnecting", generation, error })),
      Effect.retry({
        schedule: options.retryConnectionSchedule ?? Schedule.exponential("250 millis").pipe(
          Schedule.jittered,
          Schedule.modifyDelay(({ duration }) => Effect.succeed(Duration.min(duration, Duration.seconds(30))))
        ),
        while: (error) => !closing && error.permanent !== true
      }),
      Effect.catch((error) =>
        Effect.gen(function*() {
          if (closing) return
          yield* stop(error)
          yield* setState({ state: "Failed", generation, error })
          yield* Effect.logError("AMQP connection supervisor stopped", error)
        })
      ),
      Effect.catchCause((cause) =>
        Effect.gen(function*() {
          if (closing) return
          const error = connectionError("Connection supervisor stopped", cause, true)
          yield* stop(error)
          yield* setState({ state: "Failed", generation, error })
          yield* Effect.logError("AMQP connection supervisor stopped", cause)
        })
      )
    )
    const supervisorFiber = yield* supervisor.pipe(Effect.forkIn(scope))

    const operation = <A>(
      logical: Logical,
      run: (physical: Physical) => Effect.Effect<A, Error>
    ): Effect.Effect<A, Error> =>
      Effect.suspend(() => {
        if (logical.pendingOperations >= maxPending) {
          return Effect.fail(channelError("Pending operation limit exceeded"))
        }
        logical.pendingOperations++
        return logical.operations.withPermit(awaitPhysical(logical).pipe(Effect.flatMap(run))).pipe(
          Effect.ensuring(Effect.sync(() => {
            logical.pendingOperations--
          }))
        )
      })

    const cancelConsumer = Effect.fnUntraced(
      function*(logical: Logical, consumer: Consumer): Effect.fn.Return<void, Error> {
        if (!consumer.active) return
        // Unregister desired consumption before any network wait, so shutdown cannot resurrect it.
        consumer.active = false
        logical.consumers.delete(consumer)
        const physical = logical.physical
        yield* Effect.gen(function*() {
          if (physical !== undefined && physical.active && physical.epoch.active && consumer.tag !== "") {
            yield* rpc(physical.epoch, physical, 60, 30, { consumerTag: consumer.tag, noWait: false }, [60031])
            physical.consumers.delete(consumer.tag)
          }
        }).pipe(Effect.ensuring(Effect.sync(() => {
          requeueBufferedConsumer(consumer)
          Queue.endUnsafe(consumer.mailbox)
        })))
      }
    )

    const drainConfirms = (physical: Physical) =>
      Effect.forEach(
        Array.from(physical.confirms.values()),
        (confirm) => Deferred.await(confirm).pipe(Effect.ignore),
        { discard: true }
      )

    const closeLogical = Effect.fnUntraced(function*(logical: Logical): Effect.fn.Return<void> {
      if (logical.closed) return yield* Deferred.await(logical.closeDone)
      logical.closed = true
      logicals.delete(logical)
      const physical = logical.physical
      if (physical !== undefined) finish(physical.admissionClosed, Effect.void)
      const shutdown = Effect.gen(function*() {
        for (const consumer of Array.from(logical.consumers)) {
          yield* cancelConsumer(logical, consumer).pipe(Effect.ignore)
        }
        if (physical !== undefined && physical.active && physical.epoch.active) {
          yield* drainConfirms(physical).pipe(Effect.ignore)
          physical.closing = true
          yield* rpc(
            physical.epoch,
            physical,
            20,
            40,
            {
              replyCode: 200,
              replyText: "Goodbye",
              classId: 0,
              methodId: 0
            },
            [20041],
            shutdownTimeout
          )
          yield* reclaimPhysical(physical)
        }
      })
      yield* shutdown.pipe(
        Effect.interruptible,
        Effect.timeout(shutdownTimeout),
        Effect.ignore,
        Effect.ensuring(Effect.sync(() => {
          if (physical !== undefined) retirePhysical(physical, channelError("Channel closed"))
          if (
            physical !== undefined && physical.epoch.active && physical.epoch.channels.get(physical.id) === physical
          ) {
            // Never reuse a number after an uncertain close, including interruption while draining confirms.
            failEpoch(physical.epoch, connectionError("Channel shutdown did not reach a safe close barrier"))
          }
          logical.ready.openUnsafe()
          for (const consumer of logical.consumers) {
            consumer.active = false
            discard(consumer.mailbox)
            Queue.endUnsafe(consumer.mailbox)
          }
          logical.consumers.clear()
          discard(logical.returned)
          Queue.endUnsafe(logical.returned)
          finish(logical.closeDone, Effect.void)
        }))
      )
    })

    const createChannel = Effect.fn("AMQPConnection.createChannel")(function*(
      channelOptions: AMQPChannel.AMQPChannelOptions = {}
    ): Effect.fn.Return<AMQPChannel.AMQPChannel, Error, Scope.Scope> {
      channelOptions = { ...channelOptions }
      yield* integer("prefetch", channelOptions.prefetch ?? 50, 1, 65535)
      yield* integer("maxUnconfirmed", channelOptions.maxUnconfirmed ?? 1024, 1, 65535)
      const logical: Logical = {
        options: channelOptions,
        ready: Latch.makeUnsafe(),
        closeDone: Deferred.makeUnsafe(),
        operations: Semaphore.makeUnsafe(1),
        queues: [],
        exchanges: new Map(),
        bindings: [],
        consumers: new Set(),
        returned: yield* Queue.make<Envelope<AMQPTypes.ReturnedMessage>, Error | Cause.Done>({
          capacity: 64,
          strategy: "dropping"
        }),
        physical: undefined,
        closed: false,
        error: undefined,
        prefetch: channelOptions.prefetch ?? 50,
        globalPrefetch: undefined,
        pendingOperations: 0
      }
      const epoch = yield* awaitEpoch
      logicals.add(logical)
      yield* Effect.addFinalizer(() => closeLogical(logical))
      const physical = yield* openPhysical(epoch, logical).pipe(
        Effect.catch((error) => !epoch.active && !closing ? awaitPhysical(logical) : Effect.fail(error)),
        Effect.tapError(() => closeLogical(logical)),
        Effect.onInterrupt(() => closeLogical(logical))
      )
      if (physical.active) logical.ready.openUnsafe()

      const publishImpl = Effect.fn("AMQPChannel.publish")(function*(
        exchange: string,
        routingKey: string | (() => string),
        content: Uint8Array,
        publishOptions: AMQPTypes.PublishOptions = {}
      ): Effect.fn.Return<void, Error> {
        if (content.byteLength > maxMessageBytes) {
          return yield* publishError("NotSent", "Message exceeds maxMessageBytes")
        }
        const target = yield* awaitPhysical(logical).pipe(
          Effect.mapError((error) => publishError("NotSent", "Channel unavailable", error))
        )
        return yield* Effect.uninterruptibleMask((restore) =>
          Effect.gen(function*() {
            let admitted: PublishAdmission | undefined
            while (admitted === undefined) {
              // Capacity and broker-flow waits own no publish or RPC lock. Closing this physical channel wakes
              // every waiter; none may migrate an unadmitted publish onto a replacement session.
              yield* restore(Effect.raceFirst(
                target.epoch.publishGate.await.pipe(
                  Effect.andThen(target.flow.await),
                  Effect.andThen(target.confirmCapacity.await)
                ),
                Deferred.await(target.admissionClosed).pipe(
                  Effect.andThen(Effect.fail(publishError("NotSent", "Session retired before publish admission")))
                )
              ))
              admitted = yield* target.publishing.withPermit(Effect.try({
                try: (): PublishAdmission | undefined => {
                  if (!target.active || !target.epoch.active || target.closing || logical.closed || closing) {
                    throw publishError("NotSent", "Session retired before publish admission")
                  }
                  // A competing publisher or a broker control may have changed admission after the wakeup.
                  // Recheck synchronously and retry outside the lock instead of consuming a stale permit.
                  if (!target.epoch.publishGate.isOpen() || !target.flow.isOpen()) return undefined
                  if (target.confirms.size >= (channelOptions.maxUnconfirmed ?? 1024)) {
                    target.confirmCapacity.closeUnsafe()
                    return undefined
                  }
                  if (target.epoch.outboundBytes + content.byteLength > maxOutboundBytes) {
                    throw publishError("NotSent", "Outbound byte admission limit exceeded")
                  }
                  const properties: AMQPTypes.MessageProperties = {
                    ...publishOptions,
                    ...(publishOptions.persistent === undefined
                      ? {}
                      : { deliveryMode: publishOptions.persistent ? 2 : 1 })
                  }
                  const frames = [
                    Codec.encodeMethod(target.id, 60, 40, {
                      reserved1: 0,
                      exchange,
                      routingKey: typeof routingKey === "string" ? routingKey : routingKey(),
                      mandatory: publishOptions.mandatory ?? false,
                      immediate: false
                    }),
                    Codec.encodeContentHeader(target.id, BigInt(content.byteLength), properties)
                  ]
                  if (frames.some((frame) => frame.byteLength > target.epoch.frameMax)) {
                    throw new AMQPError.AMQPProtocolError({ reason: "Publish method or properties exceed frameMax" })
                  }
                  const bodyMax = target.epoch.frameMax - 8
                  for (let offset = 0; offset < content.byteLength; offset += bodyMax) {
                    frames.push(Codec.encodeFrame(3, target.id, content.subarray(offset, offset + bodyMax)))
                  }
                  // Commit the command and sequence slot atomically, including installation of interruption
                  // cleanup below. A timed-out slot continues to consume capacity until ack/nack or retirement.
                  const command = submit(target.epoch, frames, true, target)
                  const confirm = channelOptions.confirm === true ? Deferred.makeUnsafe<void, Error>() : undefined
                  if (confirm !== undefined) {
                    target.sequence++
                    target.confirms.set(target.sequence, confirm)
                    if (target.confirms.size >= (channelOptions.maxUnconfirmed ?? 1024)) {
                      target.confirmCapacity.closeUnsafe()
                    }
                  }
                  return { command, confirm }
                },
                catch: (cause) =>
                  cause instanceof AMQPError.AMQPPublishError
                    ? cause
                    : publishError("NotSent", "Publish admission failed", cause)
              }))
            }
            const confirmation = admitted.confirm
            // Once admitted, the writer completes this publish even if its caller is interrupted. Neither
            // transport completion nor confirmation waiting holds the local publish admission lock.
            yield* restore(Deferred.await(admitted.command.done)).pipe(Effect.onInterrupt(() =>
              Effect.sync(() => {
                if (confirmation !== undefined) {
                  finish(confirmation, Effect.fail(publishError("Unknown", "Publisher interrupted after admission")))
                }
              })
            ))
            if (confirmation === undefined) return
            return yield* restore(
              Deferred.await(confirmation).pipe(
                Effect.timeout(channelOptions.confirmTimeout ?? "30 seconds"),
                Effect.onInterrupt(() =>
                  Effect.sync(() => {
                    finish(
                      confirmation,
                      Effect.fail(publishError("Unknown", "Publisher interrupted before confirmation"))
                    )
                  })
                ),
                Effect.catchTag("TimeoutError", () => {
                  const error = publishError("Unknown", "Timed out waiting for publisher confirmation")
                  // Keep the sequence slot until the broker replies, but do not leave shutdown draining an abandoned waiter.
                  finish(confirmation, Effect.fail(error))
                  return Effect.fail(error)
                })
              )
            )
          })
        )
      })

      const publish = (
        exchange: string,
        routingKey: string | (() => string),
        content: Uint8Array,
        opts?: AMQPTypes.PublishOptions
      ): Effect.Effect<void, Error> =>
        Effect.suspend(() => {
          if (logical.pendingOperations >= maxPending) {
            return Effect.fail(publishError("NotSent", "Pending publish admission limit exceeded"))
          }
          logical.pendingOperations++
          return publishImpl(exchange, routingKey, content, opts).pipe(
            Effect.ensuring(Effect.sync(() => {
              logical.pendingOperations--
            }))
          )
        })

      const settle = (message: Message, methodId: number, multiple = false, requeue = false) =>
        Effect.suspend(() => {
          const capability = settlements.get(message)
          if (
            capability === undefined || capability.revoked || capability.physical.logical !== logical ||
            !capability.physical.active || !capability.physical.epoch.active || logical.closed
          ) {
            return Effect.fail(
              new AMQPError.AMQPSettlementError({
                kind: "Stale",
                reason: "Delivery belongs to a retired or different channel"
              })
            )
          }
          if (capability.settled) {
            return Effect.fail(
              new AMQPError.AMQPSettlementError({ kind: "AlreadySettled", reason: "Delivery has already been settled" })
            )
          }
          const origin = capability.physical
          // No awaitReady and no replacement lookup: settlement is irrevocably tied to this physical channel.
          const fields: Fields = { deliveryTag: capability.tag }
          if (methodId !== 90) fields.multiple = multiple
          if (methodId !== 80) fields.requeue = requeue
          return Effect.try({
            try: () => {
              const command = submit(
                origin.epoch,
                [Codec.encodeMethod(origin.id, 60, methodId, fields)],
                false,
                origin,
                true
              )
              for (const [tag, item] of origin.settlements) {
                if (tag === capability.tag || (multiple && tag <= capability.tag)) {
                  item.settled = true
                  origin.settlements.delete(tag)
                }
              }
              return command
            },
            catch: (cause) =>
              cause instanceof AMQPError.AMQPChannelError || cause instanceof AMQPError.AMQPConnectionError
                ? cause
                : protocolError(cause)
          }).pipe(Effect.flatMap((command) => Deferred.await(command.done)))
        })

      const settleAll = (methodId: number, requeue = false) =>
        operation(logical, (origin) =>
          Effect.gen(function*() {
            const command = yield* Effect.try({
              try: () => {
                const frame = Codec.encodeMethod(origin.id, 60, methodId, {
                  deliveryTag: BigInt(0),
                  multiple: true,
                  ...(methodId === 120 ? { requeue } : {})
                })
                const admitted = submit(origin.epoch, [frame], false, origin, true)
                for (const item of origin.settlements.values()) item.settled = true
                origin.settlements.clear()
                return admitted
              },
              catch: (cause) =>
                cause instanceof AMQPError.AMQPChannelError || cause instanceof AMQPError.AMQPConnectionError
                  ? cause
                  : protocolError(cause)
            })
            yield* Deferred.await(command.done)
          }))

      const bind = (binding: Binding, remove: boolean) =>
        operation(logical, (origin) =>
          Effect.gen(function*() {
            const remembered = yield* wire(() => snapshotOptions(binding))
            yield* applyBinding(origin, remembered, remove)
            const sameBinding = (item: Binding) =>
              (
                item.queue === undefined ? binding.queue === undefined : binding.queue !== undefined &&
                  (remove ? nameOf(item.queue) === nameOf(binding.queue) : item.queue === binding.queue)
              ) && item.destination === binding.destination && item.source === binding.source &&
              item.routingKey === binding.routingKey && sameArguments(item.arguments, remembered.arguments)
            yield* wire(() => {
              if (remove) {
                for (const resource of logicals) {
                  for (let index = resource.bindings.length - 1; index >= 0; index--) {
                    const item = resource.bindings[index]
                    if (item !== undefined && sameBinding(item)) resource.bindings.splice(index, 1)
                  }
                }
              } else if (!logical.bindings.some(sameBinding)) logical.bindings.push(remembered)
            })
          }))

      const channel: AMQPChannel.AMQPChannel = {
        [AMQPChannel.TypeId]: AMQPChannel.TypeId,
        connection,
        publish,
        sendToQueue: (queue, content, opts) => publish("", () => nameOf(queue), content, opts),
        ack: (message, multiple) => settle(message, 80, multiple),
        nack: (message, multiple, requeue = true) => settle(message, 120, multiple, requeue),
        reject: (message, requeue = true) => settle(message, 90, false, requeue),
        ackAll: () => settleAll(80),
        nackAll: (requeue = true) => settleAll(120, requeue),
        assertQueue: (queue = "", opts = {}) =>
          operation(logical, (origin) =>
            Effect.gen(function*() {
              const remembered = yield* wire(() => snapshotOptions(opts))
              const reply = yield* declareQueue(origin, queue, remembered)
              const previous = queue === "" ? undefined : logical.queues.find((item) => item.requested === queue)
              if (previous !== undefined) {
                previous.current = reply
                previous.options = remembered
                return previous.reference
              }
              let currentReply = reply
              const declaration: QueueDeclaration = {
                requested: queue,
                options: remembered,
                get current() {
                  return currentReply
                },
                set current(value) {
                  currentReply = value
                },
                reference: {
                  [AMQPTopology.QueueTypeId]: AMQPTopology.QueueTypeId,
                  get queue() {
                    return currentReply.queue
                  },
                  get messageCount() {
                    return currentReply.messageCount
                  },
                  get consumerCount() {
                    return currentReply.consumerCount
                  }
                }
              }
              logical.queues.push(declaration)
              return declaration.reference
            })),
        checkQueue: (queue) => operation(logical, (origin) => declareQueue(origin, nameOf(queue), {}, true)),
        deleteQueue: (queue, opts = {}) =>
          operation(logical, (origin) =>
            Effect.gen(function*() {
              const reply = yield* rpc(origin.epoch, origin, 50, 40, {
                reserved1: 0,
                queue: nameOf(queue),
                ifUnused: opts.ifUnused ?? false,
                ifEmpty: opts.ifEmpty ?? false,
                noWait: false
              }, [50041])
              const name = nameOf(queue)
              for (const resource of logicals) {
                for (let index = resource.queues.length - 1; index >= 0; index--) {
                  if (resource.queues[index]?.current.queue === name) resource.queues.splice(index, 1)
                }
                for (let index = resource.bindings.length - 1; index >= 0; index--) {
                  const binding = resource.bindings[index]
                  if (binding?.queue !== undefined && nameOf(binding.queue) === name) resource.bindings.splice(index, 1)
                }
              }
              return { messageCount: yield* wire(() => fieldNumber(reply.method, "messageCount")) }
            })),
        purgeQueue: (queue) =>
          operation(logical, (origin) =>
            rpc(origin.epoch, origin, 50, 30, {
              reserved1: 0,
              queue: nameOf(queue),
              noWait: false
            }, [50031]).pipe(
              Effect.flatMap((reply) => wire(() => ({ messageCount: fieldNumber(reply.method, "messageCount") })))
            )),
        assertExchange: (exchange, type, opts = {}) =>
          operation(logical, (origin) =>
            Effect.gen(function*() {
              const remembered = yield* wire(() => snapshotOptions(opts))
              const declaration: ExchangeDeclaration = { exchange, type, options: remembered }
              yield* declareExchange(origin, declaration)
              logical.exchanges.set(exchange, declaration)
            })),
        checkExchange: (exchange) =>
          operation(logical, (origin) =>
            rpc(origin.epoch, origin, 40, 10, {
              reserved1: 0,
              exchange,
              type: "",
              passive: true,
              durable: false,
              autoDelete: false,
              internal: false,
              noWait: false,
              arguments: {}
            }, [40011]).pipe(Effect.asVoid)),
        deleteExchange: (exchange, opts = {}) =>
          operation(logical, (origin) =>
            Effect.gen(function*() {
              yield* rpc(origin.epoch, origin, 40, 20, {
                reserved1: 0,
                exchange,
                ifUnused: opts.ifUnused ?? false,
                noWait: false
              }, [40021])
              for (const resource of logicals) {
                resource.exchanges.delete(exchange)
                for (let index = resource.bindings.length - 1; index >= 0; index--) {
                  const binding = resource.bindings[index]
                  if (binding?.source === exchange || binding?.destination === exchange) {
                    resource.bindings.splice(index, 1)
                  }
                }
              }
            })),
        bindQueue: (queue, exchange, routingKey, args = {}) =>
          bind({ queue, source: exchange, routingKey, arguments: args }, false),
        unbindQueue: (queue, exchange, routingKey, args = {}) =>
          bind({ queue, source: exchange, routingKey, arguments: args }, true),
        bindExchange: (destination, source, routingKey, args = {}) =>
          bind({ destination, source, routingKey, arguments: args }, false),
        unbindExchange: (destination, source, routingKey, args = {}) =>
          bind({ destination, source, routingKey, arguments: args }, true),
        consume: (queue, opts = {}) =>
          operation(logical, (origin) =>
            Effect.gen(function*() {
              const prefetch = yield* integer("consumer prefetch", opts.prefetch ?? logical.prefetch, 1, 65535)
              const consumer: Consumer = {
                queue,
                options: yield* wire(() => snapshotOptions(opts)),
                mailbox: yield* Queue.make<Envelope<Message>, Error | Cause.Done>({
                  capacity: prefetch,
                  strategy: "dropping"
                }),
                tag: "",
                active: true
              }
              logical.consumers.add(consumer)
              const unregister = Effect.sync(() => {
                consumer.active = false
                logical.consumers.delete(consumer)
                discard(consumer.mailbox)
                Queue.endUnsafe(consumer.mailbox)
              })
              yield* startConsumer(origin, consumer).pipe(
                Effect.tapError(() => unregister),
                Effect.onInterrupt(() => unregister)
              )
              return mailboxStream(consumer.mailbox).pipe(Stream.ensuring(
                cancelConsumer(logical, consumer).pipe(
                  Effect.interruptible,
                  Effect.timeout(shutdownTimeout),
                  Effect.ignore
                )
              ))
            })),
        cancel: (tag) =>
          operation(logical, (origin) => {
            const consumer = Array.from(logical.consumers).find((item) => item.tag === tag)
            return consumer === undefined
              ? rpc(origin.epoch, origin, 60, 30, { consumerTag: tag, noWait: false }, [60031]).pipe(Effect.asVoid)
              : cancelConsumer(logical, consumer)
          }),
        get: (queue) =>
          operation(logical, (origin) =>
            Effect.gen(function*() {
              if (origin.settlements.size >= maxPending) return yield* channelError("Unsettled delivery limit exceeded")
              const reply = yield* rpc(origin.epoch, origin, 60, 70, {
                reserved1: 0,
                queue: nameOf(queue),
                noAck: false
              }, [60071, 60072])
              return reply.message === undefined ? Option.none() : Option.some(reply.message)
            })),
        prefetch: (count, global = false) =>
          operation(logical, (origin) =>
            Effect.gen(function*() {
              yield* integer("prefetch", count, 1, 65535)
              yield* rpc(origin.epoch, origin, 60, 10, { prefetchSize: 0, prefetchCount: count, global }, [60011])
              if (global) logical.globalPrefetch = count
              else logical.prefetch = count
            })),
        recover: () =>
          operation(logical, (origin) =>
            Effect.gen(function*() {
              // Explicit requeue revokes old delivery capabilities as well as automatic session recovery.
              for (const item of origin.settlements.values()) item.revoked = true
              origin.settlements.clear()
              for (const consumer of logical.consumers) discard(consumer.mailbox)
              yield* rpc(origin.epoch, origin, 60, 110, { requeue: true }, [60111])
            })),
        returns: mailboxStream(logical.returned),
        close: closeLogical(logical)
      }
      return channel
    })

    const close = Effect.gen(function*() {
      if (closing) return yield* Deferred.await(closeDone)
      closing = true
      yield* setState({ state: "Closing", generation })
      const epoch = current
      yield* Effect.gen(function*() {
        for (const logical of Array.from(logicals)) yield* closeLogical(logical)
        if (epoch !== undefined && epoch.active) {
          yield* rpc(
            epoch,
            undefined,
            10,
            50,
            {
              replyCode: 200,
              replyText: "Goodbye",
              classId: 0,
              methodId: 0
            },
            [10051],
            shutdownTimeout
          ).pipe(Effect.ignore)
        }
      }).pipe(Effect.interruptible, Effect.timeout(shutdownTimeout), Effect.ignore)
      if (epoch !== undefined) failEpoch(epoch, connectionError("Connection closed", undefined, true))
      // A global shutdown deadline may have interrupted the channel loop. Retired sessions make this cleanup local.
      for (const logical of Array.from(logicals)) yield* closeLogical(logical)
      yield* Fiber.interrupt(supervisorFiber)
      terminal = connectionError("Connection is closed", undefined, true)
      ready.openUnsafe()
      yield* setState({ state: "Closed", generation })
    }).pipe(
      Effect.uninterruptible,
      Effect.ensuring(Effect.sync(() => finish(closeDone, Effect.void)))
    )

    connection = {
      [AMQPConnection.TypeId]: AMQPConnection.TypeId,
      createChannel,
      serverProperties: awaitEpoch.pipe(Effect.map((epoch) => epoch.serverProperties)),
      state: SubscriptionRef.get(state),
      changes: SubscriptionRef.changes(state),
      awaitReady: awaitEpoch.pipe(Effect.asVoid),
      reconnect: Effect.gen(function*() {
        const epoch = yield* awaitEpoch
        failEpoch(epoch, connectionError("Explicit reconnect requested"))
        yield* awaitEpoch
      }),
      updateSecret: (newSecret, reason) =>
        Effect.gen(function*() {
          const epoch = yield* awaitEpoch
          yield* rpc(epoch, undefined, 10, 70, {
            newSecret: Redacted.isRedacted(newSecret) ? Redacted.value(newSecret) : newSecret,
            reason
          }, [10071])
          secret = newSecret
        }),
      close
    }
    yield* Effect.addFinalizer(() => close)
    yield* connection.awaitReady.pipe(Effect.tapError(() => close), Effect.onInterrupt(() => close))
    return connection
  })
