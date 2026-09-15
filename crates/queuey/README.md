# queuey

Type-safe, macro-configured job queues for Rust on RabbitMQ.

Queues are an **enum** with `#[derive(Queues)]`. Jobs are **structs** with
`#[derive(Job)]`. A job statically knows its queue, so enqueueing it and registering a
handler for it are checked by the compiler. A job that belongs to another
application's queue set does not compile.

This crate is the facade and the only dependency an application needs. It re-exports
`queuey-core` (traits, `Producer`, `Worker`, retry policies, the in-memory backend),
the derive macros from `queuey-macros`, and, behind the default `rabbitmq` feature,
`RabbitMqBackend` from `queuey-rabbitmq`.

* Retries with **exponential backoff** (base, factor, cap, full jitter), fixed delay,
  or none. Configured per queue, overridable per job.
* **Fatal** vs **retryable** errors, dead-letter queues, per-job timeouts.
* **Deferral** for rate limits: a job that cannot run *yet* waits exactly as long as
  the API asks and comes back ahead of the backlog, without spending an attempt.
* Graceful shutdown that finishes what is in flight. `tracing` instrumentation.
* An in-memory backend for tests, with inspection helpers and virtual-time support.

## Installation

```toml
[dependencies]
queuey = "0.3"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

`serde` has to be a direct dependency: `#[derive(Serialize, Deserialize)]` expands to
code that names the `serde` crate, even when imported through the prelude.

| feature | default | effect |
|---|---|---|
| `rabbitmq` | yes | pulls in `queuey-rabbitmq` and re-exports `RabbitMqBackend`, `RabbitMqOptions` and the `rabbitmq` module |

`default-features = false` leaves the core, the macros and `MemoryBackend`, which is
enough for tests or for a backend of your own.

## Quickstart

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

`use queuey::prelude::*` brings in the two derives, the traits they implement
(`QueueSet`, `Job`), the runtime (`Producer`, `Worker`, `WorkerBuilder`,
`WorkerHandle`, `JobHandler`, `FnHandler`, `JobContext`, `JobError`), the
configuration types (`QueueConfig`, `RetryPolicy`, `Backoff`, `DEFAULT_MAX_PRIORITY`),
`MemoryBackend`, and the three foreign items user code cannot avoid naming:
`async_trait`, `Serialize`/`Deserialize` and `Arc`. With the `rabbitmq` feature it also
brings in `RabbitMqBackend`.

## Declaring queues

`#[derive(Queues)]` goes on a fieldless enum. The enum must also derive the trait's
supertraits (`Debug, Clone, Copy, PartialEq, Eq, Hash`); the macro does not add them.

```rust,ignore
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
#[queues(prefix = "myapp")]
enum AppQueues {
    #[queue(prefetch = 10)]
    Emails,                                   // broker name "myapp.emails"
    #[queue(name = "img", message_ttl = "30s", max_priority = 0)]
    ImageResize,                              // broker name "myapp.img"
}
```

Container attribute `#[queues(...)]`, optional:

| key | meaning |
|---|---|
| `prefix = "myapp"` | queue names become `"myapp.<name>"` |
| `crate = "path"` | where generated code finds the core crate; normally unnecessary, see below |

Variant attribute `#[queue(...)]`, optional on every variant:

| key | default | meaning |
|---|---|---|
| `name = "img"` | `snake_case` of the variant | the queue's name, before the prefix |
| `prefetch = 10` | `16` | unacknowledged messages per consumer, `1..=65535` |
| `durable = true` | `true` | whether the queue survives a broker restart |
| `message_ttl = "30s"` | none | per-message TTL applied on publish |
| `max_priority = 10` | `10` | priority levels the queue is declared with; `0` turns priorities off |
| `retry(...)` | no retries | default retry policy for jobs on this queue |

Every mistake is a compile error pointing at the offending token: an empty name, a
duplicate resolved name, `prefetch = 0` (unlimited in AMQP, so omit the key instead),
a zero duration, `base` greater than `max`, an unknown key.

## Declaring jobs

`#[derive(Job)]` goes on any struct or enum that is also `Serialize + Deserialize`.

```rust,ignore
#[derive(Debug, Serialize, Deserialize, Job)]
#[job(queue = AppQueues::Emails, retry(max_attempts = 5))]
struct SendEmail { to: String, body: String }
```

| key | default | meaning |
|---|---|---|
| `queue = AppQueues::Emails` | required | the variant this job lives on; everything before the last segment is the queue set type |
| `name = "emails.send"` | `module_path!() + "::" + type name` | the job type name carried in every envelope and used to route to a handler |
| `retry(...)` | inherit from the queue | retry policy override for this job type |
| `crate = "path"` | auto | same as on `#[queues]` |

### The `retry(...)` grammar

Shared by `#[queue(...)]` and `#[job(...)]`:

```text
retry(
    max_attempts = 3,            // total attempts including the first; 1 means no retries
    backoff = "exponential",     // "none" | "fixed" | "exponential" (default)
    delay = "1s",                // fixed only, required
    base = "1s",                 // exponential only, default "1s", must be <= max
    factor = 2.0,                // exponential only, default 2.0
    max = "5m",                  // exponential only, default "5m"
    jitter = true,               // exponential only, default true
)
```

Exponential backoff waits `min(max, base * factor^(attempt - 1))` before the next
attempt. With jitter the wait is drawn uniformly from `[0, computed]`.

Precedence when a job fails: the job's own `retry(...)` wins over the queue's, and
without either the job is dead-lettered on its first failure.

Durations are string literals parsed at compile time: an integer followed by `ms`,
`s`, `m`, `h` or `d`. A bare integer means seconds. `"500ms"`, `"30s"`, `"2 m"` and
`"30"` are all valid. Zero is rejected everywhere a duration is accepted.

## Handlers

A handler processes one job type. Either implement `JobHandler` on a struct, or wrap an
async closure in `FnHandler`:

```rust,ignore
struct EmailHandler { client: SmtpClient }

#[async_trait]
impl JobHandler for EmailHandler {
    type Job = SendEmail;
    async fn handle(&self, job: SendEmail, ctx: JobContext) -> Result<(), JobError> {
        self.client.send(&job.to, &job.body).await.map_err(JobError::retryable)
    }
}

// The same thing without a struct.
let resize = FnHandler::<ResizeImage, _>::new(|job: ResizeImage, ctx: JobContext| async move {
    tracing::info!(path = %job.path, attempt = ctx.attempt, "resizing");
    Ok(())
});
```

What the handler returns decides what happens to the message:

| result | effect |
|---|---|
| `Ok(())` | acked |
| `Err(JobError::Retryable(_))` | the retry policy decides: re-published after the backoff, or dead-lettered once `max_attempts` is reached |
| `Err(JobError::Fatal(_))` | dead-lettered immediately, policy ignored |
| `Err(JobError::Deferred { delay, .. })` | held for exactly `delay`, then re-delivered ahead of the backlog; no attempt spent |

`JobError::retryable(err)` and `JobError::fatal(err)` wrap any `std::error::Error`;
`retryable_msg` and `fatal_msg` take a plain string. A `Box<dyn Error>` converts into
a retryable error with `?`.

`JobContext` carries `job_id` (stable across retries), `job_type`, `queue`, the
1-based `attempt`, `max_attempts`, `deferrals`, the `priority` the delivery arrived
with, and `age` since first enqueue. `ctx.is_last_attempt()` tells a handler that a
failure now means dead-lettering.

## Deferral

An external API answers `429 Too Many Requests` with `Retry-After: 30`. Nothing went
wrong, so this is not a failure. The job has to wait exactly that long and then run
before the backlog that piled up meanwhile:

```rust,ignore
if response.status() == 429 {
    // There is no built-in cap; a handler that wants one enforces it.
    if ctx.deferrals >= 5 {
        return Err(JobError::fatal_msg("still rate limited after five deferrals"));
    }
    return Err(JobError::deferred_msg(retry_after, "rate limited"));
}
```

A deferral leaves `ctx.attempt` unchanged and never consults the retry policy. The job
comes back at the highest priority its queue knows, while ordinary work sits at `0`.
`#[queue(max_priority = 0)]` turns that off, and deferred jobs then come back FIFO.

The producer-side twin holds a job before its first run:

```rust,ignore
producer.defer(&CallApi { url }, Duration::from_secs(30)).await?;
```

## Producer

`Producer::<AppQueues, _>::new(backend)` declares every queue in the set and returns a
cheaply clonable publisher pinned to that set.

| method | effect |
|---|---|
| `enqueue(&job)` | publish now, returns the job id |
| `enqueue_after(&job, delay)` | publish after `delay`, at normal priority; one hold per distinct delay, so delays never block each other |
| `defer(&job, delay)` | hold for `delay`, then release at the queue's top priority; same holds, different return priority |
| `new_undeclared(backend)` | skip the declaration; the queues must already exist, and such a producer cannot `enqueue_after` or `defer` on RabbitMQ |

## Worker

`Worker::<AppQueues, _>::builder(backend)` configures a worker for one queue set.
`build()` declares the consumed queues and rejects two handlers for the same job type.

| builder method | default | meaning |
|---|---|---|
| `handler(h)` | | register a handler; its job must belong to `AppQueues` |
| `queues(&[AppQueues::Emails])` | every queue in the set | consume only these queues |
| `concurrency(n)` | sum of the consumed queues' prefetch | cap on jobs running at once |
| `job_timeout(d)` | none | abort a handler running longer than `d` and treat it as a retryable failure |
| `close_backend_on_shutdown(true)` | `false` | close the shared backend once `run()` returns |

`worker.run()` consumes until `handle.shutdown()` is called, where `handle` comes from
`worker.handle()` and can be cloned into any task. Shutdown is graceful, in this order:
the consumers stop pulling, everything already pulled is processed normally, and the
jobs already running are awaited. Only then does `run()` return.

The backend is not closed by default. It is usually an `Arc` shared with a `Producer`
and other workers, so closing it is the owner's call: opt in with
`close_backend_on_shutdown(true)`, or call `backend.close()` after `run()` returns.

`handle.settle_failures()` counts the times the worker ran a job but could not tell
the broker the outcome. Each one is also logged at `ERROR`. The message will be
redelivered, so a growing count means duplicate work and usually a sick connection.

A job with no registered handler or an undecodable payload is dead-lettered rather
than left to poison the queue. A handler that panics or exceeds `job_timeout` counts
as a retryable failure, so the retry policy decides what happens next.

## Compile-time guarantees

`Producer<Q, _>::enqueue` and `WorkerBuilder<Q, _>::handler` require
`Job<Queue = Q>`. Enqueueing another application's job, or registering a handler for
one, is a type error rather than a message that quietly lands on the wrong queue:

```text
error[E0271]: type mismatch resolving `<BuildReport as Job>::Queue == AppQueues`
  --> tests/compile_fail/enqueue_foreign_job.rs:35:22
   |
35 |     producer.enqueue(&BuildReport { id: 1 }).await.unwrap();
   |              ------- ^^^^^^^^^^^^^^^^^^^^^^ type mismatch resolving `<BuildReport as Job>::Queue == AppQueues`
```

These errors are pinned by the `trybuild` cases in [`tests/compile_fail`](tests/compile_fail).

## Testing your application

`MemoryBackend` implements `Backend` in-process, with no broker. It honours retry
delays and deferrals through `tokio::time`, so tests can use paused virtual time, and
it exposes what happened:

```rust,ignore
#[tokio::test(start_paused = true)]
async fn flaky_email_is_retried_then_delivered() -> queuey::Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let producer = Producer::<AppQueues, _>::new(backend.clone()).await?;
    let worker = Worker::<AppQueues, _>::builder(backend.clone())
        .handler(EmailHandler)
        .build()
        .await?;
    let handle = worker.handle();
    let running = tokio::spawn(worker.run());

    producer.enqueue(&SendEmail { to: "a@b.c".into(), body: "hi".into() }).await?;
    tokio::time::sleep(Duration::from_secs(10)).await; // virtual

    handle.shutdown();
    running.await.unwrap()?;

    assert_eq!(backend.pending("myapp.emails"), 0);
    assert!(backend.dead_letters("myapp.emails").is_empty());
    Ok(())
}
```

| helper | returns |
|---|---|
| `pending(queue)` | messages waiting to be consumed |
| `acked(queue)` | every envelope acked, in order; a retry acks the envelope it replaces, so this counts attempts |
| `deferred(queue)` | envelopes still in hold |
| `dead_letters(queue)` | dead-lettered envelopes with their reason |
| `queue_names()`, `queue_config(queue)` | what was declared |

Cloning a `MemoryBackend` gives another handle onto the same state.

## Examples

```sh
cargo run -p queuey --example memory_quickstart      # the whole library in one file, no broker
RUST_LOG=debug cargo run -p queuey --example memory_quickstart

docker run --rm -d -p 5672:5672 -p 15672:15672 rabbitmq:4-management
cargo run -p queuey --example rabbitmq_end_to_end    # the same tour against a real broker
AMQP_URL=amqp://user:pass@host:5672/%2f cargo run -p queuey --example rabbitmq_end_to_end
```

The memory example enqueues six jobs: three that succeed, one that fails twice before
succeeding, one that fails fatally, and one that a rate limit defers before it
succeeds. Then it shuts the worker down gracefully and prints what the backend saw.

## How the derive macros find this crate

Generated code needs a path to `queuey-core`. The macros read the calling crate's
`Cargo.toml` and prefer a dependency on `queuey`, emitting `::queuey::__core`, a hidden
re-export of the core crate. Renamed dependencies are handled. So a crate depending on
this facade alone needs no `crate = "..."` attribute. A crate that depends on
`queuey-core` directly gets `::queuey_core` instead. For anything else, a vendored copy
or a re-export under yet another name, `#[queues(crate = "...")]` and
`#[job(crate = "...")]` always win.

## Workspace

| crate | role |
|---|---|
| `queuey` | this crate: prelude, re-exports, examples, compile-fail tests |
| `queuey-core` | traits, `Producer`, `Worker`, retry policies, `MemoryBackend` |
| `queuey-macros` | `#[derive(Queues)]`, `#[derive(Job)]` |
| [`queuey-rabbitmq`](../rabbitmq/README.md) | `RabbitMqBackend` on `lapin`: topology, deferral hold queues, upgrade notes |

[ARCHITECTURE.md](../../ARCHITECTURE.md) covers the design, the RabbitMQ topology and
the retry and deferral semantics in full. A dropped connection is recovered
automatically by the RabbitMQ backend: publishes wait for the reconnect, consumers
resubscribe, and `Worker::run` keeps going. See
[`queuey-rabbitmq`](../rabbitmq/README.md#reconnection) for the policy and its limits.

## Minimum supported Rust version

Rust 1.88, edition 2024. `#![forbid(unsafe_code)]`.

## License

MIT OR Apache-2.0
