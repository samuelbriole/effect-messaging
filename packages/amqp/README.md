# `@effect-messaging/amqp`

An Effect-native AMQP 0-9-1 client. The protocol implementation does not depend
on a third-party AMQP library. RabbitMQ is the primary interoperability target.
This initial implementation uses PLAIN authentication; use TLS for remote broker
connections. It covers the messaging operations exposed below, not the entire
AMQP method surface (for example, transactions and alternate SASL mechanisms).

## Connect, publish, and consume

```ts
import { AMQPChannel } from "@effect-messaging/amqp"
import * as AMQPNodeConnection from "@effect-messaging/amqp/AMQPNodeConnection"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"

const program = Effect.gen(function*() {
  const channel = yield* AMQPChannel.AMQPChannel
  const queue = yield* channel.assertQueue("orders", { durable: true })

  yield* channel.sendToQueue(queue, new TextEncoder().encode("order.created"), {
    persistent: true
  })

  const messages = yield* channel.consume(queue, { prefetch: 10 })
  yield* messages.pipe(
    Stream.take(1),
    Stream.runForEach((message) =>
      Effect.gen(function*() {
        yield* Effect.logInfo(new TextDecoder().decode(message.content))
        yield* channel.ack(message)
      })
    )
  )
}).pipe(
  Effect.provide(AMQPChannel.layer({ confirm: true })),
  Effect.provide(AMQPNodeConnection.layer("amqp://guest:guest@localhost:5672"))
)

Effect.runPromise(program)
```

Install the matching `@effect/platform-node` version to use `AMQPNodeConnection`.
It provides TCP and TLS (`amqps://`), including custom CA and client-certificate
options. This layer targets Node and Node-compatible runtimes; Bun and Deno
compatibility must be verified independently rather than inferred.

The package root is runtime-neutral. Other transports can use
`AMQPConnection.make(socketFactory, options)` or `AMQPConnection.layer(...)` with
an Effect returning a **fresh** `effect/socket/Socket` for every connection attempt.
Do not provide a shared socket through `Effect.succeed`: a suspended writer must
never resume old AMQP frames on a replacement connection.

## Recovery and topology

Logical connections, channels, and consumer streams survive transient transport
failures. The client restores channel modes/QoS, successfully registered exchanges,
queues and bindings before restarting consumers. Permanent authentication or
topology failures are surfaced instead of being hidden by perpetual retries.

`assertQueue` returns a stable reference. Passing that reference to `bindQueue`,
`consume`, `get`, or `sendToQueue` follows the current physical name. This matters
for exclusive server-named queues, whose names change after reconnection:

```ts
const queue = yield * channel.assertQueue("", { exclusive: true, autoDelete: true })
yield * channel.bindQueue(queue, "events", "order.created")
const messages = yield * channel.consume(queue)
```

Ordinary string queue names remain supported. Do not save `queue.queue` as a
permanent name for a server-named queue; that is only a current-name snapshot.
Delete/unbind operations remove desired topology. Purges, gets, publishes, and
acknowledgements are never historical commands to replay during recovery.

`connection.state`, `connection.changes`, and `connection.awaitReady` expose
readiness and terminal failures. `connection.reconnect` explicitly retires a
session and waits for recovery; `connection.close` permanently stops recovery.

## Publishing guarantees

- **Default:** a publish completes after a backpressured transport write. This
  does not establish broker acceptance, routing, persistence, or consumption.
- **`confirm: true`:** completion additionally waits for a broker confirmation.
  Publishes can run concurrently with bounded outstanding confirmations.
- **Disconnect/timeout:** `AMQPPublishError.outcome` distinguishes `NotSent`,
  `Unknown`, and `Nacked`. An `Unknown` publish may already have reached the broker.
  The client never silently retries it.
- **Routing:** a confirmed message can still be unroutable. Use `mandatory: true`
  and consume `channel.returns` to observe returned messages independently.
- **Durability:** a confirmation is not a consumer acknowledgement. Durable queues
  and persistent messages are still required for restart durability.

Retrying uncertain publishes can duplicate messages. Use an outbox and/or
idempotent processing where your application requires stronger guarantees.

## Consumption and settlement

Payloads use `Uint8Array`; delivery tags use `bigint`. Consumption uses manual
acknowledgements and finite prefetch. Each delivery is bound to its original
connection/channel generation. A stale or repeated settlement fails locally with
`AMQPSettlementError`; it cannot acknowledge a different delivery after recovery.

A handler may finish after reconnection, but its old delivery tag does **not**
become valid again. The broker may already have requeued that message. Application
side effects must therefore tolerate redelivery.

Cancelling a consumer stream unregisters it from recovery. Protocol shutdown and
confirm draining have deadlines. Arbitrarily uninterruptible application handlers
cannot be guaranteed to drain within a fixed deadline.

A broker-initiated consumer cancellation fails its stream visibly rather than
silently restarting a consumer that the broker rejected. Cancelling or timing out
an already-admitted synchronous protocol operation conservatively retires its
session: late replies cannot be attributed to another operation. A channel failure
affecting ephemeral queue ownership can likewise trigger connection-wide recovery
to restore cross-channel queue references consistently.

## Optional core adapters

`@effect-messaging/core` is an optional peer. Import adapters explicitly:

```ts
import * as AMQPPublisher from "@effect-messaging/amqp/AMQPPublisher"
import * as AMQPSubscriber from "@effect-messaging/amqp/AMQPSubscriber"
import * as AMQPSubscriberResponse from "@effect-messaging/amqp/AMQPSubscriberResponse"
```

The publisher retains distributed trace propagation. Its optional retry schedule
is an explicit opt-in to retries, including their duplicate-delivery risk.
The subscriber retains Ack/Nack/Reject responses, tracing and handler timeouts,
and bounds handler concurrency across reconnects while letting running handlers
finish. Obsolete settlements are handled as session-loss bookkeeping.

## Internal module ownership

The native implementation concentrates three invariant sets behind internal interfaces:

- **Protocol vocabulary:** named methods, wire field layouts, expected replies, and
  Schema-validated decoded fields have one owner. Method schemas derive from the
  same field layouts, and remote decoding exposes typed Effect failures. The binary
  codec retains responsibility for AMQP framing and encoding, not transport or
  recovery policy. Pure byte operations stay synchronous; admission and retirement
  remain atomic.
- **Delivery settlement:** message identity, original channel sessions, outstanding
  delivery tags, revocation, and duplicate settlement have one owner. Validation,
  command admission, and settlement commit run synchronously; failed admission
  does not consume the delivery's settlement authority.
- **Desired topology:** stable queue references, configuration snapshots,
  cross-channel deletion rules, and restoration order have one owner. All channel
  declarations precede bindings and consumers. Live mailboxes and handler lifetimes
  remain with the channel implementation, not the desired-topology registry.

These modules do not change the public interface or introduce publish replay.

## Migration from the amqplib-backed interface

- Move URL/Node transport construction from `AMQPConnection.layer(url, options)`
  to `AMQPNodeConnection.layer(url, options)`.
- Import core adapters through their subpaths rather than the package root.
- Replace `Buffer`-specific operations with `TextEncoder`/`TextDecoder` where needed
  (`Buffer` remains assignable to `Uint8Array`).
- Publishing returns `Effect<void>` rather than a write-buffer boolean.
- `get` returns `Option<Message>` rather than `false` for an empty queue.
- Empty queue declarations return stable references; retain those references for
  automatic rebinding/reconsumption.
- Use package-owned types instead of `amqplib.Options`, `Replies`, or `ConsumeMessage`.
