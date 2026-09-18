# queuey

Type-safe job queues for Rust on RabbitMQ.

* Queues are an **enum** with `#[derive(Queues)]`; jobs are **structs** with `#[derive(Job)]`.
* A job statically knows its queue, so `producer.enqueue(&job)` and `worker.handler(h)`
  are checked at compile time. Cross-application mix-ups do not compile.
* Optional retries with **exponential backoff** (base, factor, cap, full jitter), fixed delay, or none,
  configurable per queue, overridable per job, and replaceable at runtime per worker, so the numbers
  can come from configuration instead of a release.
* A **dead-letter hook**: one callback for every job a worker gives up on, including the ones no
  handler ever saw (no handler registered, undecodable payload).
* Fatal vs retryable errors, dead-letter queues, graceful shutdown, `tracing` instrumentation.
* **Deferral** for rate limits: a job that cannot run *yet* waits exactly as long as the
  API asks and comes back ahead of the backlog, without spending an attempt.
* **Automatic reconnection**: a dropped broker connection is a pause, not a failure.
  Publishes wait, consumers resubscribe, `Worker::run` keeps going; policy is pluggable.
* Transport-agnostic core with an in-memory backend for tests that separates attempts from successes.
* One dependency: the derives resolve their own paths through the `queuey`
  facade, so no `crate = "..."` attribute and no direct dependency on the core crate.

```toml
[dependencies]
queuey = "1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```rust
use queuey::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
#[queues(prefix = "myapp")]
enum AppQueues {
    #[queue(prefetch = 10)]
    Emails,
    #[queue(retry(max_attempts = 3, backoff = "exponential", base = "1s", max = "2m"))]
    Images,
}

#[derive(Debug, Serialize, Deserialize, Job)]
#[job(queue = AppQueues::Emails, retry(max_attempts = 5))]
struct SendEmail { to: String, body: String }

struct EmailHandler;

#[async_trait]
impl JobHandler for EmailHandler {
    type Job = SendEmail;
    async fn handle(&self, job: SendEmail, ctx: JobContext) -> Result<(), JobError> {
        tracing::info!(to = %job.to, attempt = ctx.attempt, "sending");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> queuey::Result<()> {
    let backend = Arc::new(RabbitMqBackend::connect("amqp://guest:guest@localhost:5672/%2f").await?);

    let producer = Producer::<AppQueues, _>::new(backend.clone()).await?;
    producer.enqueue(&SendEmail { to: "a@b.c".into(), body: "hi".into() }).await?;

    let worker = Worker::<AppQueues, _>::builder(backend)
        .handler(EmailHandler)
        // The worker leaves the shared backend open by default; this process has
        // nothing else to do with it.
        .close_backend_on_shutdown(true)
        .build()
        .await?;
    worker.run().await?;
    Ok(())
}
```

## Compile-time guarantees

`#[derive(Job)]` ties a job to one queue set, and `Producer<Q, _>` / `Worker<Q, _>`
are generic over that set. Enqueueing another application's job, or registering a
handler for one, is a type error rather than a message that quietly lands on the wrong
queue:

```rust,ignore
producer.enqueue(&BuildReport { id: 1 }).await?;   // BuildReport belongs to OtherQueues
```

```text
error[E0271]: type mismatch resolving `<BuildReport as Job>::Queue == AppQueues`
  --> tests/compile_fail/enqueue_foreign_job.rs:35:22
   |
35 |     producer.enqueue(&BuildReport { id: 1 }).await.unwrap();
   |              ------- ^^^^^^^^^^^^^^^^^^^^^^ type mismatch resolving `<BuildReport as Job>::Queue == AppQueues`
   |              |
   |              required by a bound introduced by this call
   |
note: expected this to be `AppQueues`
  --> tests/compile_fail/enqueue_foreign_job.rs:25:15
   |
25 | #[job(queue = OtherQueues::Reports)]
   |               ^^^^^^^^^^^
note: required by a bound in `queuey::Producer::<Q, B>::enqueue`
   |
   |     pub async fn enqueue<J: Job<Queue = Q>>(&self, job: &J) -> Result<uuid::Uuid> {
   |                                 ^^^^^^^^^ required by this bound in `Producer::<Q, B>::enqueue`
```

The same bound rejects `Worker::<AppQueues, _>::builder(..).handler(h)` when `h`
handles a job from another set, whether it is an `impl JobHandler` or an
`FnHandler` closure. These errors are pinned by
[`crates/queuey/tests/compile_fail`](crates/queuey/tests/compile_fail).

## Deferral (rate limits, `Retry-After`)

A job that cannot run *yet* is not a job that failed. The motivating case: an external
API answers `429 Too Many Requests` with `Retry-After: 30`. Nothing went wrong. The job
has to wait exactly that long, and then run **before** the backlog that piled up
meanwhile. That is a deferral:

```rust,ignore
#[async_trait]
impl JobHandler for CallApiHandler {
    type Job = CallApi;

    async fn handle(&self, job: CallApi, ctx: JobContext) -> Result<(), JobError> {
        let response = self.http.get(&job.url).send().await.map_err(JobError::retryable)?;

        if response.status() == 429 {
            // A handler that wants a cap enforces its own: nothing else will.
            if ctx.deferrals >= 5 {
                return Err(JobError::fatal_msg("still rate limited after five deferrals"));
            }
            let retry_after = parse_retry_after(&response).unwrap_or(Duration::from_secs(30));
            return Err(JobError::deferred_msg(retry_after, "rate limited"));
        }

        Ok(())
    }
}
```

* A deferral costs no attempt. `ctx.attempt` is unchanged and the retry policy is never
  consulted: neither its backoff (the handler states the delay) nor its `max_attempts`
  (nothing failed, so nothing is dead-lettered). The worker logs it at `INFO`, not
  `WARN`/`ERROR`. A job may defer itself indefinitely.
* The job comes back first. The held envelope returns at the highest priority its queue
  knows, while everything enqueued normally sits at `0`. `ctx.deferrals` counts how often
  it happened; `ctx.priority` is the priority the delivery arrived with. "First" means
  first among what is still *on* the queue: a consumer with prefetch 10 may already be
  holding ten backlog messages, and those run first.
* The priority lasts exactly one round. If the job then fails
  and is retried, the retry goes back out at priority `0`. A retry is not a deferral,
  and nothing is owed a permanent place at the front. `ctx.deferrals` still carries.

The producer-side twin skips the first run: park the job for a delay right away, and
have it arrive at the top when the delay is up:

```rust,ignore
producer.defer(&CallApi { url }, Duration::from_secs(30)).await?;
```

Contrast with `enqueue_after`, which waits the same way but returns at priority `0`, behind
the backlog.

### `max_priority`

How many priority levels a queue is declared with, as a queue attribute:

| attribute | meaning |
|---|---|
| `#[queue(max_priority = 10)]` | ten levels; deferred jobs return at `10` (the default, `DEFAULT_MAX_PRIORITY`) |
| `#[queue(max_priority = 0)]` | not a priority queue; the broker ignores priorities and deferred jobs come back FIFO |

```rust,ignore
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
#[queues(prefix = "myapp")]
enum AppQueues {
    #[queue(prefetch = 10, max_priority = 10)]
    Emails,
    /// Order matters more than latency here: plain FIFO, deferral included.
    #[queue(name = "img", max_priority = 0)]
    Images,
}
```

### RabbitMQ topology

* The main queue `q` is declared with `x-max-priority` when `max_priority` is set; every
  publish carries the AMQP `priority` property and an `x-deferrals` header.
* Each wait, deferral and retry alike, goes into a **hold queue per delay**,
  `q.deferred.<ttl_ms>` (e.g. `myapp.emails.deferred.30000`), declared on demand right
  before the publish:
  `x-message-ttl = ttl_ms`, dead-lettering back to `q`, and `x-expires = ttl_ms * 2` so an
  idle hold queue deletes itself. Both arguments follow from the name alone, so every
  process declares the same queue identically. Delays are rounded up to a granularity
  (`deferred_granularity` for deferrals, `retry_granularity` for retries and
  `enqueue_after`, both 1s by default), so `Retry-After: 30` and a 29.2s delay share one
  queue. Every message in a hold queue has the same TTL, so it drains strictly in order: no
  head-of-line blocking between delays, which is why retries use the same mechanism instead
  of a shared wait queue with per-message expirations.
* Hold queues are declared on their own channel, not on the one that publishes, so a
  declare the broker rejects cannot fail the publishes in flight beside it.
* A delay may not exceed **about 24.8 days** (half of RabbitMQ's maximum TTL, which is what
  keeps `x-expires` above the TTL). A longer one is refused with an error rather than
  quietly released early.
* The queue you defer onto, or retry or `enqueue_after` on, has to have been declared
  through the same backend, the one `Producer::new` or `WorkerBuilder::build` gave you. A
  `Producer::new_undeclared` cannot defer or delay: there would be no `q` for the hold queue
  to dead-letter into.
* `q.dead` is untouched by this, and hold queues never get a priority.

### Upgrading an existing deployment

RabbitMQ refuses to change the arguments of a queue that already exists: a `q` declared
before this feature has no `x-max-priority`, and redeclaring it with one closes the
channel with `PRECONDITION_FAILED`. Either:

* delete the existing `q` (drain it first) and let the next declare recreate it with
  priorities, **or**
* set `#[queue(max_priority = 0)]` on those queues. Deferral still works, the held jobs
  come back FIFO instead of ahead of the backlog.

Queues created from now on get `x-max-priority = 10` by default.

## Runtime knobs on the worker

Two things the macros cannot decide for you, because they are operational rather than structural:

```rust,ignore
let worker = Worker::<AppQueues, _>::builder(backend)
    .handler(EmailHandler)
    // Replace the compiled-in policy for this process: whole queue, or one job type.
    .retry_override(AppQueues::Emails, RetryPolicy::exponential(attempts_from_config))
    .job_retry_override::<SendReceipt>(RetryPolicy::none())
    // Learn about every job this worker gives up on, whatever the cause.
    .on_dead_letter(FnDeadLetterHook::new(|dead: DeadLetter| async move {
        tracing::error!(cause = ?dead.cause, attempts = dead.attempts(), "{}", dead.reason);
    }))
    .build()
    .await?;
```

One job at a time, rather than one kind of job, is the defaulted `set_retry_policy` on the handler
trait, the only hook that sees the decoded payload and so the only one that can give this webhook
endpoint ten attempts and its neighbour on the same queue three:

```rust,ignore
fn set_retry_policy(&self, job: &Deliver) -> Option<RetryPolicy> {
    self.endpoints.get(&job.endpoint).map(|e| e.policy.clone())
}
```

Retry precedence, highest first: `JobHandler::set_retry_policy` > `job_retry_override` >
`retry_override` > `#[job(retry(...))]` > `#[queue(retry(...))]`. It runs after decode and before
`handle`, so `ctx.max_attempts` already reflects it. Overrides belong to one worker; a `Producer` never consults a policy.

The hook's `cause` is `NoHandler`, `Decode`, `Fatal` or `Exhausted`, and it is awaited *before* the
delivery is settled, so a notification is never lost for a job that is already gone. It is
at-least-once, like the rest of the system. It never changes the outcome: the job is dead-lettered
either way, and a panicking hook is logged and stepped over.

## Naming the broker connection

`RabbitMqOptions::connection_name("orders-worker")` sets what RabbitMQ shows for this
process in the management UI and in `rabbitmqctl list_connections`, which is the
difference between an operator reading forty rows of `10.0.3.17:52344` and knowing which
one to close. It is re-sent on every reconnect, so the name survives an outage.

That is the entire handshake surface, and deliberately so: `queuey-rabbitmq` exposes no
`lapin` type at all, because one in a public signature would make a `lapin` 5.0 into a
`queuey` 2.0. An application that also speaks raw AMQP depends on `lapin` and opens its
own connection.

## Workspace

| crate | role |
|---|---|
| `queuey` | facade: prelude, re-exports, examples |
| `queuey-core` | traits, `Producer`, `Worker`, retry policies, `MemoryBackend` |
| `queuey-macros` | `#[derive(Queues)]`, `#[derive(Job)]` |
| `queuey-rabbitmq` | `RabbitMqBackend` on `lapin` |

See [ARCHITECTURE.md](ARCHITECTURE.md) for the design, RabbitMQ topology and retry semantics.

## Testing

```sh
cargo test --workspace                       # unit + in-memory tests; broker tests print "skipping"
docker run --rm -d -p 5672:5672 -p 15672:15672 rabbitmq:4-management
AMQP_URL=amqp://guest:guest@localhost:5672/%2f cargo test --workspace   # + RabbitMQ integration tests

cargo run -p queuey --example memory_quickstart    # runnable tour, no broker
cargo run -p queuey --example rabbitmq_end_to_end  # the same against a broker
```

## Stability

1.0 means the public surface is committed to, and semantic versioning applies from here.

- **The envelope is frozen.** A queue drained after a deploy holds messages written by
  the version before it, so fields are only ever added, never renamed, retyped or
  removed, and every addition carries `#[serde(default)]`. Bodies written by 0.2 and
  0.3 producers still decode.
- **Public types are `#[non_exhaustive]`**, so a new field or variant is a minor
  release. That includes the struct *variants* (`Backoff::Exponential`,
  `RetryDecision::Retry`, `JobError::Deferred`), where the enum-level attribute alone
  would not have stopped a downstream struct literal from breaking. Build them with
  `QueueConfig::new`, `RetryPolicy::new`, `Envelope::new`, `Backoff::exponential_with`,
  `JobError::deferred`, `JobContext::new`, `DeadLetter::new`, `RabbitMqOptions::default`
  and the builders; `match` on the enums with a wildcard arm and on the struct variants
  with a `..` rest.
- **Your handlers and hooks are unit-testable.** `JobContext::new::<J>(attempt,
  max_attempts)` and `DeadLetter::new(envelope, cause, reason)`, plus `with_*` builders
  for the optional parts, build the two values a `JobHandler` and a `DeadLetterHook`
  receive, so testing one is a function call rather than a backend and a worker.
- **`Backend` and `Delivery` are implementable out of tree.** For the whole of 1.x they
  only gain methods that have a default implementation, so a backend written against
  1.0 keeps compiling.
- **MSRV is 1.88, edition 2024.** A raise is a minor version bump, never a patch.
- **Both licence texts ship inside every published crate**, not only at the repository
  root, so the tarball crates.io serves satisfies the terms it declares.
- The four crates share one version and are released together.

Known limits, unchanged by 1.0: delivery is at-least-once (a job in flight when the
connection drops is redelivered, and `WorkerHandle::settle_failures` counts outcomes the
worker could not report); queues are classic, because deferral's overtake rides
`x-max-priority`, which quorum queues do not support.

## License

MIT OR Apache-2.0. Both texts are at the repository root and inside every crate directory,
since a published crate contains only its own directory.
