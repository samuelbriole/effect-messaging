import * as Layer from "effect/Layer"
import * as AMQPChannel from "../src/AMQPChannel.ts"
import * as AMQPNodeConnection from "../src/AMQPNodeConnection.ts"

export const broker = {
  hostname: "localhost",
  port: 5679,
  username: "guest",
  password: "guest"
}

export const testConnection = AMQPNodeConnection.layer(broker)
export const testChannel = AMQPChannel.layer().pipe(Layer.provideMerge(testConnection))
export const testConfirmChannel = AMQPChannel.layer({ confirm: true }).pipe(Layer.provideMerge(testConnection))
export const encode = (value: string) => new TextEncoder().encode(value)
export const decode = (value: Uint8Array) => new TextDecoder().decode(value)
