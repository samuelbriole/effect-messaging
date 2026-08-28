import type { Channel, ChannelModel } from "amqplib"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"

/** @internal */
const eventStream =
  (eventName: string) => <T extends ChannelModel | Channel>(ref: SubscriptionRef.SubscriptionRef<Option.Option<T>>) =>
    SubscriptionRef.changes(ref).pipe(
      Stream.flatMap(
        (target) => {
          if (Option.isNone(target)) {
            return Stream.never
          } else {
            return Stream.callback<unknown>((queue) =>
              Effect.sync(() => {
                target.value.addListener(eventName, (event: unknown) => Queue.offerUnsafe(queue, event))
                target.value.addListener("close", () => Queue.endUnsafe(queue))
              })
            )
          }
        },
        { concurrency: "unbounded" }
      )
    )

/** @internal */
export const closeStream = eventStream("close")

/** @internal */
export const errorStream = eventStream("error")
