---
"@effect-messaging/core": major
"@effect-messaging/amqp": major
"@effect-messaging/nats": major
---

Migrate all messaging packages to Effect 4.0.0 and remove the @effect/platform peer dependency.
Service keys use Context.Service and duration options use Duration.Input.

Fix AMQP recovery with Effect 4.0.0 and amqplib 2.2.0 by tracking resource lifecycle synchronously
and cleaning up scoped event listeners. Distinguish unexpected closure from intentional shutdown.
Ensure interrupted confirm draining closes the channel and clears stale references without affecting replacement channels
or cancellation before lock acquisition.
Use scoped finalizers and acquireUseRelease for interruption-safe channel cleanup in Effect 4.0.0.

Publish ESM-only packages with explicit public exports and declaration validation.
Internal and index subpaths are not exported.

Update amqplib to 2.2.0 and tooling to pnpm 12.8.1, TypeScript 7.0.2, Babel 8.0.6, Vitest and coverage-v8 5.0.3,
@effect/vitest 4.0.0, @effect/tsgo 0.47.2, Changesets 3.0.3, oxlint 1.86.0, dprint 0.58.0, and TSTyche 7.2.5.
Use @effect/docgen 4.0.0 with an isolated TypeScript 6.0.3 and tsx 4.23.15 documentation toolchain.
Remove unused React JSX compiler configuration from the TypeScript 7.0.2 build.
Remove current and legacy compiler caches during cleanup so TypeScript 7.0.2 clean rebuilds regenerate package artifacts.
Update dprint plugins to TypeScript 0.96.1, Markdown 0.24.0, and JSON 0.25.0.
Update GitHub Actions to Checkout 7.0.1, Setup Node 7.0.0, pnpm Setup 6.1.0, Changesets 2.1.2,
Upload Pages Artifact 5.0.0, and Deploy Pages 5.0.1, preserving custom release commands.
