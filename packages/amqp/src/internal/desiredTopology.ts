import * as Effect from "effect/Effect"
import * as AMQPError from "../AMQPError.ts"
import * as AMQPTopology from "../AMQPTopology.ts"
import type * as AMQPTypes from "../AMQPTypes.ts"
import * as Codec from "./codec.ts"

/** Desired declarations are configuration, never a history of commands. */
export interface ExchangeDeclaration {
  readonly exchange: string
  readonly type: string
  readonly options: AMQPTypes.ExchangeOptions
}

export interface Binding {
  readonly queue?: AMQPTopology.QueueName
  readonly destination?: string
  readonly source: string
  readonly routingKey: string
  readonly arguments: AMQPTypes.FieldTable
}

interface QueueDeclaration {
  readonly requested: string
  options: AMQPTypes.QueueOptions
  readonly reference: AMQPTopology.QueueReference
  current: AMQPTypes.QueueReply
}

interface Channel<Consumer> {
  readonly queues: Array<QueueDeclaration>
  readonly exchanges: Map<string, ExchangeDeclaration>
  readonly bindings: Array<Binding>
  readonly consumers: Set<Consumer>
}

/** Each restorer captures one live channel; transport and failure policy stay with its owner. */
export interface Restorer<Consumer, E> {
  readonly exchange: (declaration: ExchangeDeclaration) => Effect.Effect<void, E>
  readonly queue: (requested: string, options: AMQPTypes.QueueOptions) => Effect.Effect<AMQPTypes.QueueReply, E>
  readonly binding: (binding: Binding) => Effect.Effect<void, E>
  readonly consumer: (consumer: Consumer) => Effect.Effect<void, E>
  readonly ready: () => void
}

export const queueName = (queue: AMQPTopology.QueueName): string => typeof queue === "string" ? queue : queue.queue

const snapshotField = (value: AMQPTypes.FieldValue, depth = 0): AMQPTypes.FieldValue => {
  if (depth > 32) throw new AMQPError.AMQPProtocolError({ reason: "Field table exceeds nesting limit" })
  if (value === null || typeof value !== "object") return value
  if (value instanceof Uint8Array) return new Uint8Array(value)
  if (value instanceof Date) return new Date(value.getTime())
  if (Array.isArray(value)) return value.map((field) => snapshotField(field, depth + 1))
  return Object.fromEntries(Object.entries(value).map(([key, field]) => [key, snapshotField(field, depth + 1)]))
}

/** Snapshot before the network wait so caller mutation cannot change the desired declaration. */
export const snapshotTable = (table: AMQPTypes.FieldTable): AMQPTypes.FieldTable =>
  Object.fromEntries(Object.entries(table).map(([key, field]) => [key, snapshotField(field)]))

export const snapshotOptions = <A extends { readonly arguments?: AMQPTypes.FieldTable }>(options: A): A => ({
  ...options,
  ...(options.arguments === undefined ? {} : { arguments: snapshotTable(options.arguments) })
})

const canonicalField = (value: AMQPTypes.FieldValue): AMQPTypes.FieldValue => {
  if (value === null || typeof value !== "object" || value instanceof Date || value instanceof Uint8Array) return value
  if (Array.isArray(value)) return value.map(canonicalField)
  if ("_tag" in value && value._tag === "Decimal") return value
  return canonicalTable(value as AMQPTypes.FieldTable)
}

const canonicalTable = (table: AMQPTypes.FieldTable): AMQPTypes.FieldTable =>
  Object.fromEntries(
    Object.entries(table).sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0).map(
      ([key, field]) => [key, canonicalField(field)]
    )
  )

const sameArguments = (left: AMQPTypes.FieldTable, right: AMQPTypes.FieldTable): boolean => {
  const a = Codec.encodeFieldTable(canonicalTable(left))
  const b = Codec.encodeFieldTable(canonicalTable(right))
  if (a.byteLength !== b.byteLength) return false
  for (let index = 0; index < a.byteLength; index++) if (a[index] !== b[index]) return false
  return true
}

const removeWhere = <A>(values: Array<A>, predicate: (value: A) => boolean): void => {
  for (let index = values.length - 1; index >= 0; index--) {
    const value = values[index]
    if (value !== undefined && predicate(value)) values.splice(index, 1)
  }
}

/**
 * Owns the cross-channel desired graph, stable queue references, and restoration order.
 * Mutations are recorded only after successful broker replies. Removing an owner does not
 * delete broker resources or desired declarations held by other owners.
 */
export const make = <Owner extends object, Consumer>() => {
  const channels = new Map<Owner, Channel<Consumer>>()
  const channel = (owner: Owner): Channel<Consumer> => {
    let state = channels.get(owner)
    if (state === undefined) {
      state = { queues: [], exchanges: new Map(), bindings: [], consumers: new Set() }
      channels.set(owner, state)
    }
    return state
  }

  const queueDeclared = (
    owner: Owner,
    requested: string,
    options: AMQPTypes.QueueOptions,
    reply: AMQPTypes.QueueReply
  ): AMQPTopology.QueueReference => {
    const state = channel(owner)
    const previous = requested === "" ? undefined : state.queues.find((item) => item.requested === requested)
    if (previous !== undefined) {
      previous.current = reply
      previous.options = options
      return previous.reference
    }
    let current = reply
    const declaration: QueueDeclaration = {
      requested,
      options,
      get current() {
        return current
      },
      set current(value) {
        current = value
      },
      reference: {
        [AMQPTopology.QueueTypeId]: AMQPTopology.QueueTypeId,
        get queue() {
          return current.queue
        },
        get messageCount() {
          return current.messageCount
        },
        get consumerCount() {
          return current.consumerCount
        }
      }
    }
    state.queues.push(declaration)
    return declaration.reference
  }

  const bindingApplied = (owner: Owner, binding: Binding, remove: boolean): void => {
    const sameBinding = (item: Binding) =>
      (
        item.queue === undefined ? binding.queue === undefined : binding.queue !== undefined &&
          (remove ? queueName(item.queue) === queueName(binding.queue) : item.queue === binding.queue)
      ) && item.destination === binding.destination && item.source === binding.source &&
      item.routingKey === binding.routingKey && sameArguments(item.arguments, binding.arguments)
    if (remove) {
      for (const state of channels.values()) removeWhere(state.bindings, sameBinding)
    } else {
      const state = channel(owner)
      if (!state.bindings.some(sameBinding)) state.bindings.push(binding)
    }
  }

  const restore = <E>(
    owners: ReadonlyArray<Owner>,
    restorer: (owner: Owner) => Restorer<Consumer, E> | undefined,
    onError: (owner: Owner, error: E) => Effect.Effect<void, E>
  ): Effect.Effect<void, E> =>
    Effect.gen(function*() {
      // Every channel completes declarations before any channel starts dependent bindings or consumers.
      for (const phase of ["Exchanges", "Queues", "Bindings", "Consumers"] as const) {
        for (const owner of owners) {
          const run = Effect.gen(function*() {
            const state = channels.get(owner)
            const target = restorer(owner)
            if (target === undefined) return
            if (phase === "Exchanges") {
              for (const declaration of state?.exchanges.values() ?? []) yield* target.exchange(declaration)
            } else if (phase === "Queues") {
              for (const declaration of state?.queues ?? []) {
                declaration.current = yield* target.queue(declaration.requested, declaration.options)
              }
            } else if (phase === "Bindings") {
              for (const binding of state?.bindings ?? []) yield* target.binding(binding)
            } else {
              for (const consumer of state?.consumers ?? []) yield* target.consumer(consumer)
              target.ready()
            }
          })
          yield* run.pipe(Effect.catch((error) => onError(owner, error)))
        }
      }
    })

  return {
    queueDeclared,
    exchangeDeclared: (owner: Owner, declaration: ExchangeDeclaration): void => {
      channel(owner).exchanges.set(declaration.exchange, declaration)
    },
    bindingApplied,
    queueDeleted: (queue: AMQPTopology.QueueName): void => {
      const name = queueName(queue)
      for (const state of channels.values()) {
        removeWhere(state.queues, (item) => item.current.queue === name)
        removeWhere(state.bindings, (item) => item.queue !== undefined && queueName(item.queue) === name)
      }
    },
    exchangeDeleted: (exchange: string): void => {
      for (const state of channels.values()) {
        state.exchanges.delete(exchange)
        removeWhere(state.bindings, (item) => item.source === exchange || item.destination === exchange)
      }
    },
    consumers: (owner: Owner): ReadonlySet<Consumer> => channel(owner).consumers,
    addConsumer: (owner: Owner, consumer: Consumer): void => {
      channel(owner).consumers.add(consumer)
    },
    removeConsumer: (owner: Owner, consumer: Consumer): void => {
      channels.get(owner)?.consumers.delete(consumer)
    },
    hasEphemeralQueues: (owner: Owner): boolean =>
      channels.get(owner)?.queues.some((queue) => queue.requested === "" || queue.options.autoDelete === true) ?? false,
    remove: (owner: Owner): void => {
      channels.delete(owner)
    },
    restore
  }
}
