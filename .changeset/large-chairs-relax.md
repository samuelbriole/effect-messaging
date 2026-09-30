---
"@effect-messaging/amqp": minor
---

Replace amqplib 2.2.0 with a native Effect AMQP 0-9-1 client, using the matching
@effect/platform-node 4.0.0-rc.118 TCP/TLS transport as an optional integration.

This is a breaking interface change: connection transport wiring moves to
AMQPNodeConnection, payloads use Uint8Array, delivery tags use bigint, and publishing
returns an Effect<void> with confirmations opt-in. Recover logical connections,
channels, topology and consumers without replaying uncertain publishes. Stable
queue references follow server-generated names. Core publisher/subscriber adapters
remain available through explicit subpath imports and core is an optional peer.
