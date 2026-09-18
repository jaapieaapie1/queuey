# queuey architecture (v1)

Type-safe job queues for Rust on RabbitMQ. Queues are declared as an enum, jobs as
structs; derive macros wire them together so a job can only be enqueued to / consumed
from the queue it statically belongs to.

## Crates

| crate | path | role |
|---|---|---|
| `queuey-core` | `crates/core` | traits (`QueueSet`, `Job`, `JobHandler`, `Backend`, `Delivery`), `Envelope`, `RetryPolicy`/`Backoff`, `Producer`, `Worker`, `MemoryBackend` |
| `queuey-macros` | `crates/macros` | `#[derive(Queues)]`, `#[derive(Job)]` |
| `queuey-rabbitmq` | `crates/rabbitmq` | `RabbitMqBackend` on `lapin` |
| `queuey` | `crates/queuey` | facade: re-exports + `prelude`, examples, integration tests |

## Target user code

```rust
use queuey::prelude::*;

#[derive(Queues)]
#[queues(prefix = "myapp")]              // optional; queue names become "myapp.emails"
enum AppQueues {
    #[queue(prefetch = 10)]              // name defaults to snake_case(variant) -> "emails"
    Emails,
    #[queue(name = "img", retry(max_attempts = 3, backoff = "exponential", base = "1s", max = "2m"))]
    Images,
}

#[derive(Job, Serialize, Deserialize)]
#[job(queue = AppQueues::Emails, retry(max_attempts = 5, backoff = "exponential"))]
struct SendEmail { to: String, body: String }

#[derive(Job, Serialize, Deserialize)]
#[job(queue = AppQueues::Images)]        // inherits queue retry policy
struct ResizeImage { path: String }

struct EmailHandler;
#[async_trait]
impl JobHandler for EmailHandler {
    type Job = SendEmail;
    async fn handle(&self, job: SendEmail, ctx: JobContext) -> Result<(), JobError> { Ok(()) }
}

let backend = Arc::new(RabbitMqBackend::connect("amqp://localhost").await?);
let producer = Producer::<AppQueues, _>::new(backend.clone()).await?;
producer.enqueue(&SendEmail { .. }).await?;                 // compiles
// producer.enqueue(&OtherAppJob { .. })                    // compile error: Job::Queue != AppQueues

let worker = Worker::<AppQueues, _>::builder(backend)
    .handler(EmailHandler)
    .handler(FnHandler::<ResizeImage, _>::new(|job, ctx| async move { Ok(()) }))
    .build().await?;
let handle = worker.handle();
worker.run().await?;   // until handle.shutdown()
```

## Worker shutdown contract

`handle.shutdown()` makes `run()` finish in this order, and only then return:

1. the consumer tasks stop pulling from their streams. A delivery is pulled and forwarded
   to the dispatch loop in one step, so none is ever held and then discarded;
2. everything already pulled is dispatched to its handler and settled normally.
   "Stop consuming" never means "throw away what is in hand";
3. the jobs that were already running are awaited (the concurrency cap still applies).

The backend is **not** closed: it is an `Arc` normally shared with a `Producer` and other
workers. `WorkerBuilder::close_backend_on_shutdown(true)` (default `false`) opts in;
otherwise the owner calls `backend.close()` once `run()` has returned.

Failures to ack / retry / dead-letter are logged at `ERROR` and counted in
`WorkerHandle::settle_failures()`: the job ran, the broker was not told, so the message
will be redelivered. This signals duplicate work, not lost work.

## Dead-letter hook

`WorkerBuilder::on_dead_letter(hook)` registers a `DeadLetterHook` that the worker calls for every
job it gives up on. Dead-lettering is the outcome applications usually have to act on (tell the
system that enqueued the job, alert, mark an endpoint dead) and the one no handler can observe by
itself: `NoHandler` and `Decode` never reach user code at all, and a handler returning `Retryable`
cannot know whether the policy will retry or give up without re-deriving the policy.

The hook receives an owned `DeadLetter { envelope, cause, reason, max_attempts }`, where `cause` is
`NoHandler` | `Decode` | `Fatal` | `Exhausted` (`max_attempts` is `None` for `NoHandler`: no handler,
so no policy to resolve) and `reason` is the same text passed to `Delivery::dead_letter`.

Ordering is the contract: **the hook is awaited before the delivery is settled.** A process that dies
in between leaves the delivery unsettled, so the broker redelivers and the hook runs again:
at-least-once, like everything else here. Settling first would instead lose the notification for a job
that is already gone, which is the failure this exists to prevent. Consequences, all deliberate:

- a hook that blocks holds one prefetch slot for its queue;
- a hook that panics is caught (it runs in its own task), logged at `ERROR`, and the job is
  dead-lettered anyway: the hook is a notification, never a veto;
- a hook must tolerate being called twice for one job, exactly as handlers must.

## Macro attribute grammar

`#[derive(Queues)]` on a fieldless enum:
- container `#[queues(prefix = "str", crate = "path")]`, both optional. Name = `prefix + "." + name`
  if set; the separator is added by the macro, so a `prefix` that already ends in `.` is an error
  rather than a silent `a..b`.
- variant `#[queue(name = "str", prefetch = u16, durable = bool, message_ttl = "dur",
  max_priority = u8, retry(...))]`, all optional.
- `retry(max_attempts = u32, backoff = "none" | "fixed" | "exponential", delay = "dur" (fixed), base = "dur", factor = f64, max = "dur", jitter = bool)`.
- durations: humantime-ish literal strings `"500ms"`, `"1s"`, `"2m"`, `"1h"`, `"7d"`; a bare integer
  (`"30"`) is seconds. Parse at macro time; emit `Duration::from_millis(n)`.
- generates `impl QueueSet` (`all`, `name`, `config`) with `&'static str` names.
- errors: non-enum, variants with fields, duplicate names, unknown keys, bad duration -> `compile_error!` spanned on the offending token.
- **Core path resolution**, which `crate = "..."` always overrides and which never reads the
  manifest when it is given: the facade expanding *itself* -> `::queuey::__core`; else
  `queuey-core` in the calling manifest -> `::queuey_core`; else `queuey` ->
  `::queuey::__core`; else a `compile_error!` naming the override — unless there was no
  manifest to read at all (a non-cargo build), where it guesses `::queuey_core`. The core
  crate goes first because `proc-macro-crate` sees one manifest and not the build graph: it
  merges `[dependencies]` with `[dev-dependencies]` and cannot say which target is
  compiling, so a crate that keeps the facade for its tests only would otherwise get a path
  its lib cannot resolve. `::queuey_core` is right whenever the core crate is named at all,
  since the facade only re-exports it. The `Itself` case comes first for the opposite
  reason: `queuey-core` is one of the facade's own dependencies, and this workspace
  exercises `::queuey::__core` nowhere else.
- **Both helper attributes are inert on the whole item** (`attributes(queues, queue)`), so a
  `#[queue(...)]` on the enum or a `#[queues(...)]` on a variant would otherwise be dropped
  silently, unknown keys included. Each is rejected, naming the attribute that belongs there.

`#[derive(Job)]` on any struct/enum that is also `Serialize + DeserializeOwned`:
- `#[job(queue = Path::Variant)]`, required.
- `#[job(name = "str")]`, optional; default `concat!(module_path!(), "::", stringify!(Type))`.
- `#[job(retry(...))]`, optional; same grammar; generates `fn retry_policy() -> Option<RetryPolicy>`.
- `Job::Queue` is inferred as the type of the path minus the last segment (`AppQueues`).

Validation (all `compile_error!` spanned on the offending token):
- `prefix`, `#[queue(name)]` and `#[job(name)]` must not be empty **or only whitespace**, and the
  resolved queue name must not be empty either. All three end up in the wire format: a queue name
  on the broker, a job name in every envelope's `job_type` and in the handler-dispatch key.
- `prefix` must not end with `.`; the separator is added by `qualify`.
- `prefetch` must be an integer in `1..=65535`. `prefetch = 0` is rejected: 0 means *unlimited* in
  AMQP, so omit the attribute (default 16) instead.
- `message_ttl` must be in `1ms..=4294967295ms` (~49.7 days). RabbitMQ parses `x-message-ttl` as an
  unsigned 32-bit millisecond count, and `crates/rabbitmq/src/topology.rs` clamps to `MAX_TTL_MS`
  rather than let the declare close the channel; a value written in source is rejected instead of
  clamped. (Not to be confused with `MAX_DEFERRAL_MS = MAX_TTL_MS / 2`, which bounds a *hold queue*
  delay, not a queue's message TTL.)
- `max_attempts` must be an integer in `1..=u32::MAX`. `max_attempts = 0` is rejected; `1` already
  means "no retries".
- A wrong-typed, negative or out-of-range integer reports the key and its range, never syn's raw
  `invalid digit found in string`.
- Durations must be greater than zero for `message_ttl`, `delay`, `base` and `max`: a zero TTL
  discards on publish, and a zero backoff is spelled `backoff = "none"`.
- `factor` must be finite and `> 0`; for `backoff = "exponential"`, `base` must be `<= max`
  (including against the defaults `base = "1s"`, `max = "5m"`).

Generated code is scope-hygienic: primitives are emitted as `::core::primitive::str` (etc.) so a
user type named `str` cannot break the derive, and each `impl` carries `#[automatically_derived]`
plus `#[allow(clippy::approx_constant)]` for `factor` literals such as `3.14159265358979`.

## Wire format and stability (1.0)

The envelope is the only thing 1.0 can never change: a queue drained after a deploy
holds envelopes written by the version before it, so the format outlives the binary.

```
job_id  job_type  queue  attempt  enqueued_at_ms  deferrals  priority  correlation_id  payload
```

Rules that hold for the whole of 1.x:

- Fields are only ever **added**, never renamed, retyped or removed, and every added
  field carries `#[serde(default)]` so a body written by an older producer still
  decodes. `deferrals`, `priority` and `correlation_id` all arrived that way, and
  `correlation_id` is additionally `skip_serializing_if = "Option::is_none"`, so an
  unset one costs nothing on the wire.
- `correlation_id` is caller-owned and never interpreted: `Producer::enqueue_with`
  sets it, `Envelope::next_attempt` and `Envelope::deferred` carry it, the worker hands
  it to the handler as `JobContext::correlation_id`, and the dead-letter hook sees it on
  `DeadLetter::envelope`. It exists so a job can be tied back to the request that
  created it across services; the library reads nothing from it.
- Every public struct with public fields and every public enum is `#[non_exhaustive]`, so
  adding a field or a variant stays a minor release:
  - `queuey-core`: `Envelope`, `QueueConfig`, `JobContext`, `RetryPolicy`, `Backoff`,
    `RetryDecision`, `Error`, `JobError`, `AckKind`, `EnqueueOptions`, `DeadLetter`,
    `DeadLetterCause`;
  - `queuey-rabbitmq`: `RabbitMqOptions`, `BackoffPolicy`, `Attempt`, `Rebuilding`.
- **Struct *variants* carry it too**, because the enum-level attribute only reserves the
  right to add variants: a struct variant without it is still constructible and
  exhaustively matchable downstream, so adding a field to one would be breaking.
  `Backoff::Exponential`, `RetryDecision::Retry` and `JobError::Deferred` are each
  `#[non_exhaustive]` in their own right.
- Construction goes through `QueueConfig::new`, `RetryPolicy::new`, `Envelope::new` /
  `Envelope::raw`, `Backoff::exponential_with`, `RetryDecision::retry`,
  `JobError::deferred` / `deferred_msg`, `JobContext::new`, `DeadLetter::new`,
  `RabbitMqOptions::default`, `BackoffPolicy::default`, `Attempt::first` / `after` and
  the builders. Downstream `match` arms need a wildcard, and a struct-variant pattern
  needs a `..` rest.
- `JobContext::new` and `DeadLetter::new` exist for one reason: without them a user cannot
  unit-test their own `JobHandler` or `DeadLetterHook`, because the only other way to get
  one of those values is to stand up a `MemoryBackend` plus a `Worker` and drive a real
  delivery through it.
- `Backoff` is `Serialize`/`Deserialize`, so a policy can be persisted. Any field added to
  `Backoff::Exponential` in 1.x carries `#[serde(default)]`, for the same reason the
  envelope's do; `retry::tests::todays_policy_json_round_trips_byte_for_byte` pins the
  current document.
- `Backend` and `Delivery` are implementable outside the workspace and only ever gain
  defaulted methods in 1.x. See the stability note in `crates/core/src/backend.rs`.
- **No `lapin` type appears anywhere in `queuey-rabbitmq`'s public surface** — not in a
  signature, a public field, a re-export or a trait impl. `lapin` went 2 -> 3 -> 4 in
  short order, and one leak would make a `lapin` 5.0 into a `queuey` 2.0; sealed this way,
  a `lapin` upgrade is a patch release. So `codec` and `topology::{queue_args,
  deferred_queue_args, dead_queue_args}` are crate-private, there is no `pub use lapin`,
  and nothing hands out the connection or a channel on it. The cost is accepted: an
  application that also speaks raw AMQP depends on `lapin` and opens its own connection.
  The one handshake detail worth steering from outside is
  `RabbitMqOptions::connection_name: Option<String>`, translated internally into
  `ConnectionProperties::with_connection_name` on every dial, so a named connection keeps
  its name across a reconnect. `client_properties` beyond the name, the AMQP `locale`
  (RabbitMQ advertises only `en_US`), a custom executor/reactor/auth provider and `lapin`'s
  own `enable_auto_recover` are deliberately *not* configurable.

## Retry semantics

- `attempt` is 1-based and lives in the envelope. `RetryPolicy::decide(failed_attempt)`.
- `JobError::Retryable` -> policy; `JobError::Fatal` -> dead-letter immediately.
- Precedence, highest first: `JobHandler::set_retry_policy(&job)` > `WorkerBuilder::job_retry_override::<J>()`
  > `WorkerBuilder::retry_override(queue)` > `Job::retry_policy()` override > `QueueConfig.retry` >
  default (no retries).
- `JobHandler::set_retry_policy` is the only one that sees the *payload*, so it is the only one that can
  answer "how lenient should we be with **this** job", such as a webhook endpoint with a bad history
  earning ten attempts while its neighbour on the same queue gets three. Defaulted to `None`, called once per delivery
  after decode and before `handle`, so `JobContext::max_attempts` already reflects it, and the policy it
  returns is what the failure of that attempt is judged by. It is not pinned across attempts: shrinking a
  policy below a job's current attempt retires that job on its next failure.
- The first two are *runtime* overrides, resolved once by `WorkerBuilder::build` into a
  `Job::NAME -> RetryPolicy` map that dispatch reads; `Worker::retry_policy_for(job_type)` exposes the
  result. They exist because the macro grammar fixes the numbers at compile time, and how many attempts
  a flaky downstream deserves is an operational decision, not a code one. They are per worker process:
  a `Producer` never consults a policy, another worker on the same queue keeps its own, and the
  override affects `JobContext::max_attempts` so `ctx.is_last_attempt()` stays truthful. An override
  naming a queue or job type this worker has no handler for is inert and logged at `DEBUG`.
- `Backoff::Exponential`: `min(max, base * factor^(attempt-1))`, saturating; full jitter `U[0, d]` if enabled.
  Built with `Backoff::exponential_with(base, factor, max, jitter)` (the variant is
  `#[non_exhaustive]`) or `Backoff::exponential()` for the defaults; the derives emit the former.
- `WorkerBuilder::job_timeout(Duration)`, default none: a handler still running after `timeout`
  is aborted and the attempt counts as `JobError::Retryable`, so the policy decides what happens
  next. It is per worker process, like the runtime retry overrides, because how long a job may
  reasonably take is an operational judgement; the handler's own task is aborted, so a handler
  that must clean up does so in a guard, not after the `await`.

## RabbitMQ topology (per queue `q`)

- `q`: main durable queue. Consumers use `basic_qos(prefetch)`.
- `q.dead`: dead-letter queue; failed envelopes published here with headers
  `x-death-reason`, `x-original-queue`, `x-attempts`.
- `q.deferred.{ttl_ms}`: hold queue per distinct delay, shared by retries and deferrals; see
  the Deferral section for its arguments. **There is no `q.retry`.** A shared wait queue with
  per-message `expiration` suffers head-of-line blocking (RabbitMQ only expires the head of a
  classic queue, so a 5-minute retry at the head holds back every 1-second retry behind it),
  and exponential backoff produces exactly that mix. Per-message `expiration` is never set.
- `Delivery::retry`: declare the hold queue for `delay` rounded up to
  `RabbitMqOptions::retry_granularity`, publish `next` there (priority `0`) with publisher
  confirms, then `basic_ack` original.
- `Delivery::dead_letter`: publish to `q.dead` with confirms, then `basic_ack` original.
- `publish(delay = Some)`: same hold as a retry (`retry_granularity`, priority as carried by the
  envelope, `0` for a fresh one). Requires the queue to have been declared through this backend
  (`Error::UnknownQueue` otherwise) and a delay within `MAX_DEFERRAL_MS`, exactly like `defer`.
- All publishes are `mandatory`: an unroutable routing key (a queue nobody declared) comes back
  as a `basic.return` and is reported as an error, so the original is never acked for a message
  that went nowhere. `x-message-ttl` on `q` is clamped to `[1, u32::MAX]` ms.
- With `declare_dead_letter_queues = false` the backend does not own `q.dead`, so `dead_letter`
  (and a malformed body) `basic_reject`s the delivery with `requeue = false` (the broker's own
  DLX policy on `q` applies if configured, otherwise the message is dropped) and logs at `WARN`.
- Envelope JSON body, `content_type = application/json`, `delivery_mode = persistent`,
  `message_id = job_id`, `type = job_type`.
- Connection: single `lapin::Connection`; a pool of at most `RabbitMqOptions::publish_concurrency`
  (default 8) publishing channels in confirm mode, each carrying **one publish at a time**; one
  channel for hold queue declarations; one channel per consumer. All of them are the backend's own;
  nothing hands one out (see the `lapin` bullet under Wire format and stability).
- Publisher confirms: a publish holds its channel until the confirmation arrives, and that is a
  correctness requirement, not tidiness. `lapin` does not key a `basic.return` to a delivery tag —
  returned messages are queued per channel and attached to whichever pending tag the confirm
  handler resolves first, which for the `basic.ack(multiple = true)` RabbitMQ sends when it
  coalesces confirms is `HashMap` order. Two publishes in flight on one channel can therefore swap
  outcomes: the unroutable one resolves as a bare `Ack`, is reported as **success**, and the
  delivery waiting on it acks an original whose successor went nowhere — a job lost with nothing
  logged. One publish per channel makes that impossible; concurrency comes from the pool, and
  `publish_concurrency` is the ceiling on publisher confirms outstanding at once.
- Reconnect: the connection is a slot, not a socket. A drop is repaired by whichever
  operation notices first, single-flight behind a mutex, paced by a `ReconnectPolicy`
  (default `BackoffPolicy`: unlimited, jittered exponential, 500ms base, capped at 30s).
  See the Reconnection section.

## Reconnection

- **Scope.** Everything below the `Backend` trait. `Producer`, `Worker` and job handlers
  are unchanged and see no new error; a dropped connection is a pause, not a failure.
- **Single-flight.** Publishers, declarers and consumers all call
  `ConnectionHandle::ensure_connected`; one of them reconnects while the rest queue behind
  a mutex. N consumers on a backend do not become N connections.
- **Generation.** A counter of successful connections, bumped after the swap. A consumer
  records it when it subscribes, so it can tell "my connection died" from "somebody already
  replaced it". Internal to `ConnectionHandle`: it is not on the public surface, because the
  only caller that ever needed to read it was a holder of a borrowed channel, and nothing
  borrows one.
- **Replay.** Every `QueueConfig` passed to `declare` is remembered on the handle, and
  re-declared on the new connection (`declare_topology`, shared with `declare` itself so
  the two cannot drift). A broker that *restarted* has lost every non-durable queue and
  every `q.dead`; without the replay the reconnect would succeed and then fail every
  publish. Best-effort: a `PRECONDITION_FAILED` (an operator changed the arguments while we
  were away) is logged at `ERROR` and the connection kept, because no connection at all is
  strictly worse.
- **Policy.** `ReconnectPolicy` is a trait with one method, `next_delay(Attempt) -> Option<Duration>`,
  consulted before every attempt including the first: `Some(ZERO)` tries now, `None` gives up.
  `Attempt` (non-exhaustive) carries the consecutive failure count, the last error as
  `&dyn Error`, and a `Rebuilding` discriminant of `Connection` vs `Consumer`, because the two
  fail for different reasons and deserve different answers. Stored as `Arc<dyn ReconnectPolicy>`
  so the one policy is shared by every publisher and consumer; `Debug` is a supertrait so
  `RabbitMqOptions` stays printable. `BackoffPolicy` is the built-in implementation.
- **Consumers.** `consume` returns a stream that outlives the connection it started on:
  when the subscription ends or errors it resubscribes rather than propagating. Resubscribe
  failures are paced by the same policy, because the connection can be healthy while
  `basic_consume` still fails (a deleted queue), which would otherwise spin.
- **Termination.** The stream ends, and `Worker::run` returns `Error::ConsumerStopped` as
  before, in exactly three cases: the backend was closed, reconnection is disabled
  (`reconnect = None`), or a bounded policy is exhausted. With the default unlimited policy
  a dead broker is a stalled worker plus `WARN` logs, never an `Err`.
- **Close is final.** `close` marks the handle closing *before* any channel goes down, so
  consumers see a deliberate shutdown instead of an outage and stop rather than racing to
  reconnect. Nothing reopens the connection afterwards.
- **First connection is exempt.** `connect` / `with_options` do not retry: a process that
  cannot reach its broker at startup should fail, not block its caller in a backoff loop.
- **In-flight jobs are not preserved.** The broker requeues every unacknowledged delivery
  on a drop, so a job mid-handler is redelivered on the new connection and the first run's
  settle fails (counted in `WorkerHandle::settle_failures`). This is the existing
  at-least-once contract, not a new one.
- **Tests.** A TCP proxy in front of the broker (`BrokerProxy` in `tests/broker.rs`) cuts
  the connection on demand, so recovery is tested without the management plugin and without
  touching connections the test does not own.

## Deferral (hold queues, priority return)

Motivating case: an external API answers `429 Too Many Requests` with `Retry-After: 30`. The
job did not fail; it must wait *exactly* that long and then run **before** the backlog that has
piled up on the main queue meanwhile. A retry fits badly: the attempt counter burns down and the
returning message lands at the tail. Deferral is the generic name; rate limiting is the first
user. (Retries originally used a shared `q.retry` with per-message `expiration` and suffered
head-of-line blocking between delays; they now use the hold queues described here as well, with
their own `retry_granularity`.)

### Core API

- `JobError::Deferred { delay: Duration, reason: String }`; constructors
  `JobError::deferred(delay)` (reason `"deferred"`) and `JobError::deferred_msg(delay, msg)`.
  Not a failure: **`attempt` is unchanged**, the retry policy is not consulted, logged at `INFO`.
- `Envelope` gains `#[serde(default)] pub deferrals: u32` (times this job was deferred) and
  `#[serde(default)] pub priority: u8` (AMQP message priority, `0` = normal). Both default so
  envelopes from before this feature still decode. `Envelope::deferred(&self, priority: u8) -> Self`
  = clone with `deferrals + 1` and `priority` set; `attempt`, `job_id`, `enqueued_at_ms` unchanged.
- `Envelope::next_attempt` resets `priority` to `0` and keeps `deferrals`: a retry waits in a
  hold queue like a deferral but must not jump the backlog when it returns; only the scheduling
  action that just happened decides the priority.
- `JobContext` gains `deferrals: u32` and `priority: u8`. There is no built-in deferral cap: a
  handler that wants one checks `ctx.deferrals` and returns `JobError::Fatal`.
- `QueueConfig` gains `pub max_priority: Option<u8>` (default `Some(10)`), builder
  `max_priority(levels: u8)` where `0` stores `None` (not a priority queue). `None` means the
  broker ignores the priority property entirely; deferred jobs then come back FIFO.
- Deferred envelopes are published with `priority = QueueConfig::max_priority.unwrap_or(0)` of
  the job's queue, i.e. the highest level the queue knows, so they beat every normally
  enqueued job (which is priority `0`). "Comes back first" means first among what is still on
  the queue; a consumer with prefetch N may already hold N backlog messages, and those run
  before the deferred job reaches a handler.
- `Delivery::defer(self: Box<Self>, next: Envelope, delay: Duration) -> Result<()>`: durably
  schedule `next` (already `deferrals + 1`, priority set) to reappear on `next.queue` after
  `delay`, **then** ack the original. Same "publish before ack" rule as `retry`.
- `Backend::defer(&self, envelope: &Envelope, delay: Duration) -> Result<()>`: the publish half
  of the above, also used by `Producer::defer(&job, delay) -> Result<Uuid>` (first-attempt
  envelope, `deferrals = 0`, priority set, published to the hold queue). `enqueue_after` uses the
  same hold queues at priority `0`, rounded to `retry_granularity`.
- Worker: `JobOutcome::Deferred { delay, reason }` -> `info!(?delay, deferrals, reason, "job deferred")`
  -> `settle(delivery.defer(envelope.deferred(priority), delay), "defer")`. Job span unchanged.
- `MemoryBackend`: `defer` sleeps `delay` (virtual-time friendly) then inserts by priority:
  behind every pending envelope with `priority >= new.priority`, ahead of the rest (stable
  within a level). `publish` uses the same insertion so priority ordering is one code path.
  Inspection: `deferred(queue) -> usize` = envelopes still waiting in hold. Closed backend ->
  `Error::ShutDown`, like `retry`.

### Macro grammar additions

- variant `#[queue(max_priority = u8)]`, optional. `0..=255`; `0` = not a priority queue
  (emits `.max_priority(0)`). Default when omitted: the `QueueConfig` default (`Some(10)`).
  Out-of-range / wrong type -> `compile_error!` naming the key and range `0..=255`.

### RabbitMQ topology

- Main queue `q` is declared with `x-max-priority = QueueConfig::max_priority` when `Some`.
  **Changing the arguments of an existing queue is refused by the broker**
  (`PRECONDITION_FAILED` closes the channel): existing deployments must either delete `q` or
  set `max_priority = 0`. `q.dead` and hold queues never get `x-max-priority`.
- Every publish sets the AMQP `priority` property from `Envelope::priority` and the header
  `x-deferrals` from `Envelope::deferrals`.
- Hold queue per (queue, TTL): name `{q}{deferred_suffix}.{ttl_ms}` (default suffix
  `".deferred"`, e.g. `myapp.emails.deferred.30000`), declared **on demand right before every
  deferred publish** (idempotent; the redeclare also resets the `x-expires` timer, which is why
  it is not cached). Arguments: `x-message-ttl = ttl_ms`, `x-dead-letter-exchange = ""`,
  `x-dead-letter-routing-key = q`, `x-expires = ttl_ms * 2`. Durable iff `q` is durable.
  `x-expires` is a pure function of the queue's own name, so every process, whatever its
  options, declares byte-identical arguments and no declare can hit `PRECONDITION_FAILED`
  against another's. It is strictly greater than the TTL because delays are capped at
  `MAX_DEFERRAL_MS = MAX_TTL_MS / 2` (about 24.8 days); a longer delay is refused with an error
  rather than released early. Because every message in a hold queue has the same TTL, the queue
  drains strictly in order: no head-of-line blocking. Idle hold queues delete themselves one TTL
  after their last message left.
- Hold queues are declared on a **dedicated declaration channel**, never on a confirm
  publishing channel: a declare the broker rejects closes the channel it ran on, and that must
  not take unrelated in-flight publishes down with it.
- Deferral requires the target queue to have been declared through the same backend instance
  (`Producer::new`, `WorkerBuilder::build`): the hold queue dead-letters to `q` by name, so
  deferring onto a queue this backend never declared would strand the message. Hence
  `Producer::new_undeclared` + `defer` -> `Error::UnknownQueue`.
- `ttl_ms = ceil(delay / granularity) * granularity`, clamped to `[granularity, MAX_DEFERRAL_MS]`;
  the granularity is `deferred_granularity` for deferrals and `retry_granularity` for retries and
  `enqueue_after`, both `1s` by default, so `Retry-After: 30` and a `29.2s` delay share
  `...deferred.30000`. Granularity bounds the number of hold queues; the retry one exists so a
  jittered exponential backoff (a new delay on every retry) can be rounded coarsely without
  touching `Retry-After` precision. A delay above `MAX_DEFERRAL_MS` is an error, not a clamp.
- The worst-case hold-queue name (`{q}{deferred_suffix}.{MAX_DEFERRAL_MS}`) is validated at
  `declare`, so a queue name that could later produce an over-long hold-queue name fails at
  startup rather than on the first deferral.
- `RabbitMqOptions` has `deferred_suffix: String` (`".deferred"`), `deferred_granularity: Duration`
  (`1s`) and `retry_granularity: Duration` (`1s`). Builders for each. `retry_suffix` no longer
  exists.
- `Delivery::defer` and `Delivery::retry` = declare hold queue, `publish_confirmed` (mandatory)
  there with the envelope's priority and `expiration` **unset** (the queue TTL does the timing),
  then ack. Publish failure leaves the original unacked.
- The `x-death` header RabbitMQ adds when the TTL expires is ignored; `deferrals` in the body
  is the source of truth.

### Tests

- core: envelope `deferred()` semantics + serde default round-trip of old JSON; `JobError`
  constructors; worker deferral (attempt unchanged, deferrals incremented, priority = queue
  max, INFO not ERROR, retry policy untouched, deferral works on a queue with `max_priority`
  `None` -> priority 0); memory priority insertion (stable, mixed levels, publish + defer both);
  `Producer::defer`; shutdown while a job is in hold does not lose it.
- macros: `max_priority` parsed (`0`, `1`, `255`), rejected (`256`, `-1`, `"10"`), emitted
  into `config()`; trybuild fail case `queue_max_priority_out_of_range.rs`.
- rabbitmq: pure tests for `deferred_queue_name`, `deferred_ttl_ms` (rounding, granularity,
  refusal above `MAX_DEFERRAL_MS`), `deferred_queue_args` (`x-expires == 2 * ttl_ms` for every
  TTL up to the cap), worst-case hold-queue name length rejected at `declare`, `queue_args`
  carries `x-max-priority` iff `Some`, `props_for` carries `priority` and `x-deferrals`. Broker
  tests (`AMQP_URL`): deferred job reappears after ~TTL on `q`; the hold queue exists during the
  wait and deletes itself once idle for its `x-expires`; a deferred job published while `q`
  already holds two normal jobs is **consumed first**; `max_priority = 0` queue declares without
  `x-max-priority` and deferral still round-trips; redeclaring an existing queue with a
  different `max_priority` surfaces `PRECONDITION_FAILED` as an `Err`, not a hang; a hold queue
  pre-declared by a foreign process with different arguments makes `defer` return `Err` while
  the original delivery stays **unacked** and the confirm publishing channels stay usable
  (the declaration channel is the only casualty); `defer` onto a queue this backend never
  declared -> `Error::UnknownQueue`, nothing published; a delay past `MAX_DEFERRAL_MS` is
  refused with an `Err` instead of being released early; a 500ms retry waits in `q.deferred.1000`
  and comes back with `attempt + 1`; a 1-second retry published *after* a 6-second retry returns
  in about a second (the head-of-line proof); a retry and a deferral with the same delay share
  one hold queue and the deferral is consumed first; `retry_granularity` and
  `deferred_granularity` round the same delay into different hold queues; a delayed publish onto
  an undeclared queue is `Error::UnknownQueue`.

## Testing strategy

- core: unit tests for backoff math, `RetryPolicy::decide`, envelope round-trip; worker tests against
  `MemoryBackend` with `tokio::time::pause` for delays (success, retry-then-success, give-up, fatal,
  no-handler, decode failure, panic, graceful shutdown, buffered deliveries drained at
  shutdown, backend closed only when opted in, settle failures counted, prefetch honoured,
  runtime retry overrides and their precedence, dead-letter hook per cause including its
  before-settle ordering and a panicking hook).
  Tests synchronise on observable state (backend inspection helpers, virtual time), never
  on wall-clock sleeps or scheduling order.
- macros: `trybuild` pass/fail cases + expansion assertions via the generated impl (`QueueSet::all`,
  `name`, `config`, `Job::NAME`, `Job::QUEUE`, `retry_policy`). Core path resolution is unit tested
  as a pure precedence table (`attrs::tests::crate_path_precedence`), because the decision is
  separated from the environment it reads; `tests/crate_path_probe.rs` then writes real crates for
  the three dependency shapes (core only, facade only, core with the facade as a dev-dependency)
  and builds and tests each with a nested cargo. No `trybuild` case can cover this: they all share
  the macro crate's own manifest. The probe is opt-in behind `QUEUEY_PATH_PROBE`, with the same
  skip-notice convention as `AMQP_URL`.
- rabbitmq: pure unit tests for topology naming, header/property mapping, envelope <-> lapin
  `BasicProperties`, and the confirm-pool size clamp; integration tests behind `AMQP_URL` env var
  (`#[ignore]`-free but early-return with an `eprintln!` skip notice when unset), exercising
  declare / publish / consume / retry / defer / dead-letter. Two of them guard the
  publish-before-ack contract at its failing edge: `dead_letter` into a `q.dead` an operator
  deleted must return `Err` and leave the original unacked, and a `basic.return` must be reported
  against the publish that caused it and no other. The second is probabilistic, so it repeats:
  300 rounds of ten concurrent publishes with one unroutable. The constants come from measurement
  against RabbitMQ 4 — below ten in flight, or with the unroutable publish first, the broker's
  confirms happen to arrive in an order that attributes correctly and nothing is proved; at ten
  with it in the middle a shared channel misattributes on roughly one round in fifteen, so 300
  rounds leave a regression passing with probability on the order of `e^-20`.
- facade: compile-fail test proving cross-queue-set enqueue is rejected; end-to-end example; an
  end-to-end pass over the dead-letter hook and retry overrides through the derives only
  (`tests/e2e_dead_letter.rs`), since those are configured on the builder a downstream user holds.

## Conventions

- edition 2024, MSRV 1.88, `#![forbid(unsafe_code)]`, `#![warn(missing_docs)]` on public crates.
- `cargo clippy --workspace --all-targets -- -D warnings` must pass. `cargo fmt` clean.
- `tracing` for logs, never `println!` in library code.
- CI runs, besides the default-feature job: `cargo test --workspace --no-default-features`
  (the no-broker configuration the `rabbitmq` feature advertises, clippy and docs included),
  `cargo publish --workspace --dry-run` with a check that both licence texts are inside every
  package, an MSRV job, and the macro path probe. `RUSTFLAGS` does not reach rustdoc, so the
  doc step sets `RUSTDOCFLAGS` itself.
- The MSRV is compiled, not just declared. `rust-toolchain.toml` pins `channel = "stable"` and
  rustup obeys it over any installed toolchain, so the MSRV job sets `RUSTUP_TOOLCHAIN: "1.88.0"`,
  which outranks the file, and asserts `rustc --version` before checking anything. It uses
  `--locked`: an unpinned `cargo update` is how an MSRV breaks without anyone noticing.
- `LICENSE-MIT` and `LICENSE-APACHE` are duplicated into each crate directory. A published crate
  contains only its own directory, and both licences require their text to travel with the
  distribution. Real files, not symlinks: cargo's handling of symlinks in packages is not worth
  gambling a permanent, immutable release on.
