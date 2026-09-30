import { describe, expect, it } from "@effect/vitest"
import { AMQPProtocolError, AMQPSettlementError } from "../src/AMQPError.ts"
import * as DeliverySettlement from "../src/internal/deliverySettlement.ts"

interface Origin {
  readonly id: number
  readonly owner: object
  active: boolean
  epochActive: boolean
  closed: boolean
}

const setup = () => {
  const owner = {}
  const origin: Origin = { id: 1, owner, active: true, epochActive: true, closed: false }
  const ledger = DeliverySettlement.make<object, Origin, object>({
    ownerOf: (channel) => channel.owner,
    isActive: (channel) => channel.active && channel.epochActive && !channel.closed
  })
  const register = (tag: number, channel = origin) => {
    const message = {}
    ledger.register(channel, message, BigInt(tag))
    return message
  }
  return { owner, origin, ledger, register }
}

const settlementError = (run: () => unknown, kind: "Stale" | "AlreadySettled"): void => {
  expect(run).toThrow(AMQPSettlementError)
  expect(run).toThrow(expect.objectContaining({ kind }))
}

describe("delivery settlement ownership", () => {
  it("binds authority to object identity and owner, not copied message fields", () => {
    const { ledger, origin, owner, register } = setup()
    const message = register(1)
    expect(ledger.validate(owner, message)).toEqual({ origin, tag: BigInt(1) })
    expect(Object.isFrozen(ledger.validate(owner, message))).toBe(true)
    settlementError(() => ledger.validate({}, message), "Stale")
    settlementError(() => ledger.validate(owner, { ...message }), "Stale")
    expect(ledger.count(origin)).toBe(1)
  })

  it("never routes old messages onto a replacement physical channel with the same ID", () => {
    const { ledger, origin, owner, register } = setup()
    const oldMessage = register(1)
    ledger.retire(origin)
    const replacement = { ...origin }
    const newMessage = register(1, replacement)
    settlementError(() => ledger.validate(owner, oldMessage), "Stale")
    expect(ledger.validate(owner, newMessage).origin).toBe(replacement)
    expect(ledger.count(origin)).toBe(0)
    expect(ledger.count(replacement)).toBe(1)
    expect(() => register(2)).toThrow(AMQPProtocolError)
  })

  it("checks physical, epoch and logical lifetime before duplicate settlement", () => {
    for (const field of ["active", "epochActive", "closed"] as const) {
      const { ledger, origin, owner, register } = setup()
      const message = register(1)
      ledger.commit(origin, BigInt(1))
      settlementError(() => ledger.validate(owner, message), "AlreadySettled")
      origin[field] = field === "closed"
      settlementError(() => ledger.validate(owner, message), "Stale")
    }
  })

  it("tracks deliveries received during logical shutdown until physical retirement", () => {
    const { ledger, origin, owner, register } = setup()
    origin.closed = true
    const message = register(1)
    settlementError(() => ledger.validate(owner, message), "Stale")
    expect(ledger.revoke(message)).toEqual({ origin, tag: BigInt(1) })
    expect(ledger.count(origin)).toBe(0)
    ledger.retire(origin)
    expect(() => register(2)).toThrow(AMQPProtocolError)
  })

  it("commits only the selected tag unless multiple is requested", () => {
    const { ledger, origin, owner, register } = setup()
    const first = register(1)
    const second = register(2)
    const third = register(3)
    ledger.commit(origin, BigInt(2))
    settlementError(() => ledger.validate(owner, second), "AlreadySettled")
    expect(ledger.validate(owner, first).tag).toBe(BigInt(1))
    expect(ledger.validate(owner, third).tag).toBe(BigInt(3))
    expect(ledger.count(origin)).toBe(2)
    ledger.commit(origin, BigInt(3), true)
    settlementError(() => ledger.validate(owner, first), "AlreadySettled")
    settlementError(() => ledger.validate(owner, third), "AlreadySettled")
    expect(ledger.count(origin)).toBe(0)
  })

  it("multiple settlement excludes revoked lower tags and other physical channels", () => {
    const { ledger, origin, owner, register } = setup()
    const first = register(1)
    const second = register(2)
    const third = register(3)
    const other = { ...origin }
    const otherMessage = register(1, other)
    expect(ledger.revoke(first)).toEqual({ origin, tag: BigInt(1) })
    expect(ledger.revoke(first)).toBeUndefined()
    ledger.commit(origin, BigInt(2), true)
    settlementError(() => ledger.validate(owner, first), "Stale")
    settlementError(() => ledger.validate(owner, second), "AlreadySettled")
    expect(ledger.validate(owner, third).tag).toBe(BigInt(3))
    expect(ledger.validate(owner, otherMessage).origin).toBe(other)
    expect(ledger.count(origin)).toBe(1)
    expect(ledger.count(other)).toBe(1)
  })

  it("zero with multiple commits all outstanding deliveries without changing revoked history", () => {
    const { ledger, origin, owner, register } = setup()
    const revoked = register(1)
    const first = register(2)
    const second = register(3)
    ledger.revoke(revoked)
    ledger.commit(origin, BigInt(0), true)
    settlementError(() => ledger.validate(owner, revoked), "Stale")
    settlementError(() => ledger.validate(owner, first), "AlreadySettled")
    settlementError(() => ledger.validate(owner, second), "AlreadySettled")
    expect(ledger.revoke(first)).toBeUndefined()
    expect(ledger.revoke({})).toBeUndefined()
    expect(ledger.count(origin)).toBe(0)
  })

  it("leaves deliveries retryable when command admission throws before commit", () => {
    const { ledger, origin, owner, register } = setup()
    const message = register(1)
    const submit = (): void => {
      throw new Error("Admission failed")
    }
    expect(() => {
      const capability = ledger.validate(owner, message)
      submit()
      ledger.commit(capability.origin, capability.tag)
    }).toThrow("Admission failed")
    expect(ledger.validate(owner, message).tag).toBe(BigInt(1))
    expect(ledger.count(origin)).toBe(1)
    ledger.commit(origin, BigInt(1))
    settlementError(() => ledger.validate(owner, message), "AlreadySettled")
  })

  it("recovery revokes only outstanding entries and preserves increasing tag history", () => {
    const { ledger, origin, owner, register } = setup()
    const settled = register(1)
    ledger.commit(origin, BigInt(1))
    const outstanding = register(2)
    ledger.revokeAll(origin)
    settlementError(() => ledger.validate(owner, settled), "AlreadySettled")
    settlementError(() => ledger.validate(owner, outstanding), "Stale")
    expect(ledger.count(origin)).toBe(0)
    expect(() => register(2)).toThrow(AMQPProtocolError)
    const next = register(3)
    expect(ledger.validate(owner, next).tag).toBe(BigInt(3))
    expect(ledger.count(origin)).toBe(1)
  })

  it("rejects zero, decreasing and reused tags even after immediate revocation", () => {
    const { ledger, origin, register } = setup()
    expect(() => register(0)).toThrow(AMQPProtocolError)
    const message = register(4)
    ledger.revoke(message)
    expect(() => register(3)).toThrow(AMQPProtocolError)
    expect(() => register(4)).toThrow(AMQPProtocolError)
    register(5)
    expect(ledger.count(origin)).toBe(1)
    expect(() => ledger.register(origin, message, BigInt(6))).toThrow(AMQPProtocolError)
    register(6)
    expect(ledger.count(origin)).toBe(2)
  })
})
