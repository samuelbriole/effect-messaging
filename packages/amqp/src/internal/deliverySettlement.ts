import { AMQPProtocolError, AMQPSettlementError } from "../AMQPError.ts"

/**
 * The immutable authority needed to submit a settlement on its original channel.
 * @since 0.8.0
 */
export interface Capability<Origin> {
  readonly origin: Origin
  readonly tag: bigint
}

/**
 * A connection-local ledger. Register every delivery, including deliveries that
 * are immediately revoked. Validate, submit and commit must run synchronously,
 * with commit occurring only after successful command admission.
 * @since 0.8.0
 */
export const make = <Message extends object, Origin extends object, Owner>(options: {
  readonly ownerOf: (origin: Origin) => Owner
  readonly isActive: (origin: Origin) => boolean
}) => {
  interface Entry {
    readonly capability: Capability<Origin>
    settled: boolean
    revoked: boolean
  }
  interface Channel {
    readonly outstanding: Map<bigint, Entry>
    lastTag: bigint
    retired: boolean
  }

  const messages = new WeakMap<Message, Entry>()
  const channels = new WeakMap<Origin, Channel>()

  const revokeAll = (origin: Origin): void => {
    const channel = channels.get(origin)
    if (channel === undefined) return
    for (const entry of channel.outstanding.values()) entry.revoked = true
    channel.outstanding.clear()
  }

  return {
    register(origin: Origin, message: Message, tag: bigint): void {
      let channel = channels.get(origin)
      if (channel === undefined) {
        channel = { outstanding: new Map(), lastTag: BigInt(0), retired: false }
        channels.set(origin, channel)
      }
      // A gracefully closing logical channel may still receive deliveries before cancel-ok.
      // Its owner decides whether settlement is allowed; only physical retirement ends registration.
      if (channel.retired) {
        throw new AMQPProtocolError({ reason: "Delivery for retired channel" })
      }
      if (tag <= channel.lastTag) {
        throw new AMQPProtocolError({ reason: "Non-increasing delivery tag" })
      }
      if (messages.has(message)) {
        throw new AMQPProtocolError({ reason: "Delivery message already registered" })
      }
      const entry: Entry = { capability: Object.freeze({ origin, tag }), settled: false, revoked: false }
      channel.lastTag = tag
      messages.set(message, entry)
      channel.outstanding.set(tag, entry)
    },

    validate(owner: Owner, message: Message): Capability<Origin> {
      const entry = messages.get(message)
      if (
        entry === undefined || entry.revoked || channels.get(entry.capability.origin)?.retired ||
        options.ownerOf(entry.capability.origin) !== owner || !options.isActive(entry.capability.origin)
      ) {
        throw new AMQPSettlementError({
          kind: "Stale",
          reason: "Delivery belongs to a retired or different channel"
        })
      }
      if (entry.settled) {
        throw new AMQPSettlementError({ kind: "AlreadySettled", reason: "Delivery has already been settled" })
      }
      return entry.capability
    },

    revoke(message: Message): Capability<Origin> | undefined {
      const entry = messages.get(message)
      if (entry === undefined || entry.settled || entry.revoked) return undefined
      entry.revoked = true
      channels.get(entry.capability.origin)?.outstanding.delete(entry.capability.tag)
      return entry.capability
    },

    revokeAll,

    retire(origin: Origin): void {
      revokeAll(origin)
      const channel = channels.get(origin)
      if (channel === undefined) {
        channels.set(origin, { outstanding: new Map(), lastTag: BigInt(0), retired: true })
      } else {
        channel.retired = true
      }
    },

    commit(origin: Origin, tag: bigint, multiple = false): void {
      const channel = channels.get(origin)
      if (channel === undefined) return
      if (!multiple) {
        const entry = channel.outstanding.get(tag)
        if (entry !== undefined) {
          entry.settled = true
          channel.outstanding.delete(tag)
        }
        return
      }
      for (const [deliveryTag, entry] of channel.outstanding) {
        if (tag === BigInt(0) || deliveryTag <= tag) {
          entry.settled = true
          channel.outstanding.delete(deliveryTag)
        }
      }
    },

    count(origin: Origin): number {
      return channels.get(origin)?.outstanding.size ?? 0
    }
  }
}
