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

## Macro attribute grammar

`#[derive(Queues)]` on a fieldless enum:
- container `#[queues(prefix = "str")]`, optional. Name = `prefix + "." + name` if set.
- variant `#[queue(name = "str", prefetch = u16, durable = bool, message_ttl = "dur", retry(...))]`, all optional.
- `retry(max_attempts = u32, backoff = "none" | "fixed" | "exponential", delay = "dur" (fixed), base = "dur", factor = f64, max = "dur", jitter = bool)`.
- durations: humantime-ish literal strings `"500ms"`, `"1s"`, `"2m"`, `"1h"`. Parse at macro time; emit `Duration::from_millis(n)`.
- generates `impl QueueSet` (`all`, `name`, `config`) with `&'static str` names.
- errors: non-enum, variants with fields, duplicate names, unknown keys, bad duration -> `compile_error!` spanned on the offending token.

`#[derive(Job)]` on any struct/enum that is also `Serialize + DeserializeOwned`:
- `#[job(queue = Path::Variant)]`, required.
- `#[job(name = "str")]`, optional; default `concat!(module_path!(), "::", stringify!(Type))`.
- `#[job(retry(...))]`, optional; same grammar; generates `fn retry_policy() -> Option<RetryPolicy>`.
- `Job::Queue` is inferred as the type of the path minus the last segment (`AppQueues`).

Validation (all `compile_error!` spanned on the offending token):
- `prefix` and `name` must not be empty, and the resolved queue name must not be empty either.
- `prefetch` must be an integer in `1..=65535`. `prefetch = 0` is rejected: 0 means *unlimited* in
  AMQP, so omit the attribute (default 16) instead.
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

## Retry semantics

- `attempt` is 1-based and lives in the envelope. `RetryPolicy::decide(failed_attempt)`.
- `JobError::Retryable` -> policy; `JobError::Fatal` -> dead-letter immediately.
- Precedence: `Job::retry_policy()` override > `QueueConfig.retry` > default (no retries).
- `Backoff::Exponential`: `min(max, base * factor^(attempt-1))`, saturating; full jitter `U[0, d]` if enabled.

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
- Connection: single `lapin::Connection`, one channel for publishing (confirm mode), one channel
  for hold queue declarations, one channel per consumer.
- Reconnect is **out of scope for v1**; stream ends on connection loss, `Worker::run` returns `Err`.

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
- Hold queues are declared on a **dedicated declaration channel**, never on the confirm
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
  the original delivery stays **unacked** and the confirm publishing channel stays usable
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
  shutdown, backend closed only when opted in, settle failures counted, prefetch honoured).
  Tests synchronise on observable state (backend inspection helpers, virtual time), never
  on wall-clock sleeps or scheduling order.
- macros: `trybuild` pass/fail cases + expansion assertions via the generated impl (`QueueSet::all`,
  `name`, `config`, `Job::NAME`, `Job::QUEUE`, `retry_policy`).
- rabbitmq: pure unit tests for topology naming, header/property mapping, envelope <-> lapin
  `BasicProperties`; integration tests behind `AMQP_URL` env var (`#[ignore]`-free but early-return
  with an `eprintln!` skip notice when unset), exercising declare / publish / consume / retry /
  defer / dead-letter.
- facade: compile-fail test proving cross-queue-set enqueue is rejected; end-to-end example.

## Conventions

- edition 2024, MSRV 1.88, `#![forbid(unsafe_code)]`, `#![warn(missing_docs)]` on public crates.
- `cargo clippy --workspace --all-targets -- -D warnings` must pass. `cargo fmt` clean.
- `tracing` for logs, never `println!` in library code.
