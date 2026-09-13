# queuey

Type-safe job queues for Rust on RabbitMQ.

* Queues are an **enum** with `#[derive(Queues)]`; jobs are **structs** with `#[derive(Job)]`.
* A job statically knows its queue, so `producer.enqueue(&job)` and `worker.handler(h)`
  are checked at compile time. Cross-application mix-ups do not compile.
* Optional retries with **exponential backoff** (base, factor, cap, full jitter), fixed delay, or none,
  configurable per queue and overridable per job.
* Fatal vs retryable errors, dead-letter queues, graceful shutdown, `tracing` instrumentation.
* **Deferral** for rate limits: a job that cannot run *yet* waits exactly as long as the
  API asks and comes back ahead of the backlog, without spending an attempt.
* Transport-agnostic core with an in-memory backend for tests.
* One dependency: the derives resolve their own paths through the `queuey`
  facade, so no `crate = "..."` attribute and no direct dependency on the core crate.

```toml
[dependencies]
queuey = "0.2"
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

## License

MIT OR Apache-2.0
