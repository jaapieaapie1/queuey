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
- **Named connections.** `RabbitMqOptions::connection_name("orders-worker")` puts a
  readable name on this backend's connection, shown in the RabbitMQ management UI and in
  `rabbitmqctl list_connections client_properties`. Re-sent on every reconnect, so the
  name survives an outage. A plain `String`, not a `lapin` type — see below.
- **`RabbitMqOptions::publish_concurrency`.** How many publishes may be in flight at once,
  default `8`, one confirm-mode channel each. It exists because a confirm channel may only
  carry one publish at a time (see Fixed, below), so this is the ceiling that buys the
  concurrency back. Raise it when the confirm round trip rather than the handler is the
  bottleneck; `0` is clamped to `1`.
- `MemoryBackend::succeeded`, `retried` and `settled` (with `AckKind`) separate the
  acks a retry makes from the ones a success makes.
- `Envelope::raw` and `Envelope::with_correlation_id` for backends and tests that build
  an envelope without a `Job` type in scope.
- **Constructors for the two values user code receives but could not build.**
  `JobContext::new::<J>(attempt, max_attempts)` and
  `DeadLetter::new(envelope, cause, reason)`, each with `with_*` builders for the
  optional parts, so a `JobHandler` or a `DeadLetterHook` can be unit-tested by calling
  it. Both types are `#[non_exhaustive]`, so until now the only way to get one was to
  stand up a `MemoryBackend` plus a `Worker` and drive a real delivery through it.
- `Backoff::exponential_with(base, factor, max, jitter)` and `RetryDecision::retry(delay)`,
  the constructors for the struct variants sealed below. `JobError::Deferred` already had
  `JobError::deferred` / `deferred_msg`.

### Changed

- **Every public struct with public fields and every public enum is
  `#[non_exhaustive]`.** In `queuey-core`: `Envelope`, `QueueConfig`, `JobContext`,
  `RetryPolicy`, `Backoff`, `RetryDecision`, `Error`, `JobError`, `AckKind`,
  `EnqueueOptions`, `DeadLetter`, `DeadLetterCause`. In `queuey-rabbitmq`:
  `RabbitMqOptions`, `BackoffPolicy`, `Attempt`, `Rebuilding`. Adding a field or a
  variant stays a minor release for the whole of 1.x. Downstream code constructs these
  through `QueueConfig::new`, `RetryPolicy::new`, `Envelope::new`/`raw`,
  `JobContext::new`, `DeadLetter::new`, `RabbitMqOptions::default`,
  `BackoffPolicy::default`, `Attempt::first`/`after` and the builders, and needs a
  wildcard arm when matching.
- **The struct *variants* are `#[non_exhaustive]` too**: `Backoff::Exponential`,
  `RetryDecision::Retry` and `JobError::Deferred`. The enum-level attribute only reserves
  the right to add *variants*; a struct variant without its own was still constructible
  and exhaustively matchable downstream, so adding a field to one would have been
  breaking. Build them with `Backoff::exponential_with`, `RetryDecision::retry` and
  `JobError::deferred`/`deferred_msg`, and match them with a `..` rest pattern. A field
  added to `Backoff::Exponential` in 1.x will carry `#[serde(default)]`, so a persisted
  `RetryPolicy` keeps decoding.
- **`queuey-rabbitmq` no longer exposes `lapin` at all.** No `lapin` type appears in any
  public signature, public field, public re-export or public trait impl; `lapin` is an
  ordinary private dependency, so upgrading it is a patch release rather than a major
  one. Without this, a `lapin` 5.0 would have forced a `queuey` 2.0. What went:
  - the `codec` module and `topology::{queue_args, deferred_queue_args, dead_queue_args}`
    are crate-private. They returned `lapin::BasicProperties` / `FieldTable` and only ever
    served this backend's own publisher;
  - `pub use lapin` is gone. Depend on `lapin` directly to name it;
  - `RabbitMqBackend::create_channel` and `connection_generation` are gone, and with them
    the ability to borrow the backend's connection. An application that also speaks raw
    AMQP opens its own connection. This is a real capability removed on purpose;
  - `RabbitMqOptions::connection_properties` (a public `lapin::ConnectionProperties`) is
    replaced by `connection_name: Option<String>` above, which is the one thing it was
    used for. `client_properties` beyond the name, the AMQP `locale` (RabbitMQ advertises
    only `en_US`), a custom executor/reactor/auth provider and `lapin`'s own
    `enable_auto_recover` are deliberately not configurable.

  The string-returning parts of `topology` (`dead_queue_name`, `deferred_queue_name`,
  `deferred_ttl_ms`, the `x-*` key constants, `MAX_TTL_MS`, `MAX_DEFERRAL_MS`) are
  unchanged and stay public: they are what an operator or a cleanup script actually needs.
- **Macro grammar: five things that used to compile clean are now compile errors.** Each
  is spanned on the offending token and pinned by a `trybuild` case.
  - `#[queue(...)]` on the enum and `#[queues(...)]` on a variant. `attributes(queues,
    queue)` makes both inert anywhere on the item, so a misplaced one was dropped in
    silence, unknown keys and all — `#[queue(prefix = "myapp", prefetch = 99,
    nonsense = 1)]` on the enum configured nothing and warned about nothing.
  - `#[job(name = "")]`, the one string key that never went through the emptiness check
    `#[queue(name = "")]` already had. `Job::NAME` is the envelope's `job_type` and the
    handler-dispatch key.
  - Whitespace-only `prefix` and `name` (`"   "`), which produced a queue nobody can type.
  - A `prefix` ending in `.`, which produced a doubled separator (`"a."` -> `a..b`). The
    macro adds the separator; the prefix must not.
  - A `message_ttl` past `"4294967295ms"` (~49.7 days). A broker stores `x-message-ttl` as
    an unsigned 32-bit millisecond count, and the backend clamps to it; a literal in
    source is rejected rather than silently shortened.
- Retry precedence is now, highest first: `JobHandler::set_retry_policy` >
  `job_retry_override` > `retry_override` > `#[job(retry)]` > `#[queue(retry)]`.
- `Backend` and `Delivery` are documented as implementable outside the workspace: for
  the whole of 1.x they only ever gain methods that have a default implementation.
- The envelope wire format is frozen: fields are only ever added, always with
  `#[serde(default)]`, so a body written by an older producer keeps decoding.

### Fixed

- **Concurrent publishes could report an unroutable publish as success, losing the job
  silently.** Every publish shared one confirm-mode channel and released it before awaiting
  the broker's confirmation, so several confirms were outstanding at once. `lapin` does not
  key a `basic.return` to a delivery tag: returns are queued per channel and attached to
  whichever pending tag the confirm handler resolves first, which for the
  `basic.ack(multiple = true)` RabbitMQ sends when it coalesces confirms is `HashMap` order.
  A return meant for one publish could therefore land on another. The publish that wrongly
  received it failed spuriously, which was safe — its original stayed unacked — but the
  publish that was *actually* unroutable resolved as a bare `Ack` and was reported as
  `Ok(())`. `Delivery::dead_letter` then acked an original whose successor went nowhere: a
  job gone, with nothing logged. It affected every publish path (`enqueue`, `retry`, `defer`,
  `dead_letter`, malformed-body forwarding) and needed nothing more exotic than a `q.dead` an
  operator had deleted, or a hold queue the broker had expired. Publishes now take a channel
  from a small pool and hold it across the confirmation, so a channel never has more than one
  publish outstanding and a return can only belong to the publish that caused it. There is no
  upstream fix to wait for; `lapin` 4.11.0 is current. See
  `RabbitMqOptions::publish_concurrency` for the pool size.

- **Both licence texts now ship inside every published crate.** `LICENSE-MIT` and
  `LICENSE-APACHE` lived only at the repository root, which crates.io never sees, so all
  four packages would have gone out in breach of the terms they declare — and a published
  version is immutable. Each crate directory now carries its own copy, and CI fails if a
  package is missing either.
- **The derives resolve `queuey-core` before the facade.** `proc-macro-crate` reads a
  manifest, not a build graph: it cannot tell which target is compiling and reads
  `[dev-dependencies]` alongside `[dependencies]`. A crate with `queuey-core` in
  `[dependencies]` and `queuey` in `[dev-dependencies]` (or as an `optional` dependency
  whose feature is off) was handed `::queuey::__core` for its own lib and failed with
  ``cannot find `queuey` in the crate root``. The core crate is now looked up first, which
  is correct in strictly more manifests because the facade only re-exports it; the facade
  path is kept for facade-only crates and for the facade expanding itself. When neither
  crate is in the manifest, the derives now say so and point at `#[queues(crate = "...")]`
  / `#[job(crate = "...")]` instead of emitting a path rustc can only complain about.
- **The crate-level quickstart compiles without the `rabbitmq` feature.** It named
  `RabbitMqBackend` unconditionally while `default-features = false` was advertised as
  supported, so `cargo test -p queuey --no-default-features --doc` failed: the first thing
  a reader of the no-broker configuration met did not build. It is now one program with
  the backend line swapped per feature, and the `prelude` docs no longer link an item that
  configuration does not have.

### Notes

- Classic queues only. Deferral's overtake rides `x-max-priority`, which quorum queues
  do not support; see `crates/rabbitmq/README.md`.
- Delivery is at-least-once. A job in flight when the connection drops is redelivered,
  and `WorkerHandle::settle_failures` counts the times the worker could not tell the
  broker an outcome.

- CI covers more than the default features: `--no-default-features` (tests, clippy and a
  `RUSTDOCFLAGS=-D warnings` doc build), `cargo publish --workspace --dry-run` with a check
  that both licence texts are inside every package, an MSRV job pinned to 1.88.0, and a
  probe that builds the three dependency shapes a downstream crate can have.

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
