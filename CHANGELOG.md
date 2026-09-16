# Changelog

All notable changes to this workspace are recorded here. The four crates
(`queuey`, `queuey-core`, `queuey-macros`, `queuey-rabbitmq`) share one version
number and are released together.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and from
1.0.0 the project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-09-16

First stable release. The semantics have not moved since 0.3.0; what changed is that
the public surface is now committed to, and the few places where that commitment would
have been painful were fixed first.

### Added

- **Dead-letter hook.** `WorkerBuilder::on_dead_letter` takes a `DeadLetterHook` (or a
  closure via `FnDeadLetterHook`) and is called for every job the worker gives up on,
  with a `DeadLetter { envelope, cause, reason, max_attempts }`. `DeadLetterCause`
  distinguishes `NoHandler`, `Decode`, `Fatal` and `Exhausted`, including the two cases
  no handler can observe. The hook is awaited *before* the delivery is settled, so a
  crash in between means the broker redelivers and the hook runs again: at-least-once,
  like the rest of the system.
- **Runtime retry overrides.** `WorkerBuilder::retry_override(queue, policy)` and
  `WorkerBuilder::job_retry_override::<J>(policy)` replace the compile-time-only
  policy, resolved once by `build()` and exposed by `Worker::retry_policy_for`.
- **Per-job retry policy.** `JobHandler::set_retry_policy(&self, job)` is the only hook
  that sees the decoded payload, so it is the only one that can answer "how lenient for
  *this* job". Resolved before `handle`, so `JobContext::max_attempts` and
  `is_last_attempt()` already reflect it.
- **Correlation ids.** `Producer::enqueue_with(&job, EnqueueOptions)` tags an envelope
  with a caller-owned id that survives retries and deferrals, arrives as
  `JobContext::correlation_id`, and is on `DeadLetter::envelope`. Never interpreted by
  the library.
- **Shared connection.** `RabbitMqBackend::create_channel` hands out a `lapin::Channel`
  on the backend's own connection, with `connection_generation` to notice when that
  connection has been replaced. A process that also speaks plain AMQP no longer needs a
  second connection.
- `MemoryBackend::succeeded`, `retried` and `settled` (with `AckKind`) separate the
  acks a retry makes from the ones a success makes.
- `Envelope::raw` and `Envelope::with_correlation_id` for backends and tests that build
  an envelope without a `Job` type in scope.

### Changed

- **Every public struct with public fields and every public enum is
  `#[non_exhaustive]`** (`Envelope`, `QueueConfig`, `JobContext`, `RetryPolicy`,
  `Backoff`, `RetryDecision`, `Error`, `JobError`, `AckKind`). Adding a field or a
  variant stays a minor release for the whole of 1.x. Downstream code constructs these
  through `QueueConfig::new`, `RetryPolicy::new`, `Envelope::new`/`raw` and the
  builders, and needs a wildcard arm when matching.
- Retry precedence is now, highest first: `JobHandler::set_retry_policy` >
  `job_retry_override` > `retry_override` > `#[job(retry)]` > `#[queue(retry)]`.
- `Backend` and `Delivery` are documented as implementable outside the workspace: for
  the whole of 1.x they only ever gain methods that have a default implementation.
- The envelope wire format is frozen: fields are only ever added, always with
  `#[serde(default)]`, so a body written by an older producer keeps decoding.

### Notes

- Classic queues only. Deferral's overtake rides `x-max-priority`, which quorum queues
  do not support; see `crates/rabbitmq/README.md`.
- Delivery is at-least-once. A job in flight when the connection drops is redelivered,
  and `WorkerHandle::settle_failures` counts the times the worker could not tell the
  broker an outcome.

## [0.3.0]

- Automatic reconnection below the `Backend`: publishes wait for the reconnect,
  consumer streams resubscribe, `Worker::run` keeps going across a broker restart.
  Configurable through `RabbitMqOptions::reconnect_with` and the `ReconnectPolicy`
  trait.

## [0.2.1]

- crates.io keywords.

## [0.2.0]

- Retries moved onto per-delay hold queues (`q.deferred.{ttl_ms}`); `q.retry` dropped.
  A short backoff is never stuck behind a long one.

## [0.1.1]

- README for the facade crate.
