/**
 * Native AMQP 0-9-1 data types. Payloads and field tables are runtime independent.
 * @since 0.8.0
 */

/** Decimal mantissas use RabbitMQ's unsigned 32-bit representation. @since 0.8.0 */
export interface Decimal {
  readonly _tag: "Decimal"
  readonly scale: number
  readonly value: number
}

/** Table bigints are signed 64-bit values; non-UTF-8 long strings decode as bytes. @since 0.8.0 */
export type FieldValue =
  | string
  | number
  | bigint
  | boolean
  | null
  | Uint8Array
  | Date
  | Decimal
  | ReadonlyArray<FieldValue>
  | FieldTable

/** @since 0.8.0 */
export interface FieldTable {
  readonly [key: string]: FieldValue
}

/** @since 0.8.0 */
export interface MessageProperties {
  readonly contentType?: string
  readonly contentEncoding?: string
  readonly headers?: FieldTable
  readonly deliveryMode?: number
  readonly priority?: number
  readonly correlationId?: string
  readonly replyTo?: string
  readonly expiration?: string | number
  readonly messageId?: string
  readonly timestamp?: bigint | number
  readonly type?: string
  readonly userId?: string
  readonly appId?: string
  readonly clusterId?: string
}

/** @since 0.8.0 */
export interface PublishOptions extends MessageProperties {
  readonly mandatory?: boolean
  readonly persistent?: boolean
}

/** @since 0.8.0 */
export interface QueueOptions {
  readonly durable?: boolean
  readonly exclusive?: boolean
  readonly autoDelete?: boolean
  readonly arguments?: FieldTable
}

/** @since 0.8.0 */
export interface ExchangeOptions {
  readonly durable?: boolean
  readonly autoDelete?: boolean
  readonly internal?: boolean
  readonly arguments?: FieldTable
}

/** @since 0.8.0 */
export interface ConsumeOptions {
  readonly prefetch?: number
  readonly exclusive?: boolean
  readonly consumerTag?: string
  readonly arguments?: FieldTable
}

/** @since 0.8.0 */
export interface QueueReply {
  readonly queue: string
  readonly messageCount: number
  readonly consumerCount: number
}

/** @since 0.8.0 */
export interface ReturnedMessage {
  readonly content: Uint8Array
  readonly properties: MessageProperties
  readonly fields: {
    readonly replyCode: number
    readonly replyText: string
    readonly exchange: string
    readonly routingKey: string
  }
}
