---
"@effect-messaging/core": major
"@effect-messaging/amqp": major
"@effect-messaging/nats": major
---

Migrate all messaging packages to Effect 4.0.0-rc.118 and remove the @effect/platform peer dependency.
Service keys use Context.Service and duration options use Duration.Input.

Fix AMQP recovery with Effect 4.0.0-rc.118 and amqplib 2.2.0 by tracking resource lifecycle synchronously
and cleaning up scoped event listeners. Distinguish unexpected closure from intentional shutdown.

Publish ESM-only packages with explicit public exports and declaration validation.
Internal and index subpaths are not exported.

Update amqplib to 2.2.0 and tooling to pnpm 12.8.1, TypeScript 7.0.2, Babel 8.0.6, Vitest 5.0.2,
@effect/tsgo 0.47.0, Changesets 3.0.3, oxlint 1.86.0, dprint 0.58.0, and TSTyche 7.2.5.
