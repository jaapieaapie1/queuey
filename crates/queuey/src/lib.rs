//! Type-safe job queues for Rust on RabbitMQ.
//!
//! Queues are an **enum** with [`macro@Queues`]; jobs are **structs** with
//! [`macro@Job`]. A job statically knows its queue, so [`Producer::enqueue`] and
//! [`WorkerBuilder::handler`] are checked at compile time. A job belonging to
//! another application's queue set does not compile.
//!
//! This crate is a facade: it re-exports [`queuey_core`], the derive
//! macros from `queuey-macros`, and (behind the default `rabbitmq`
//! feature) [`queuey_rabbitmq`]. Depending on it alone is enough.
//!
//! ```no_run
//! use queuey::prelude::*;
//!
//! #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
//! #[queues(prefix = "myapp")]
//! enum AppQueues {
//!     #[queue(prefetch = 10)]
//!     Emails,
//!     #[queue(retry(max_attempts = 3, backoff = "exponential", base = "1s", max = "2m"))]
//!     Images,
//! }
//!
//! #[derive(Debug, Serialize, Deserialize, Job)]
//! #[job(queue = AppQueues::Emails, retry(max_attempts = 5))]
//! struct SendEmail { to: String, body: String }
//!
//! struct EmailHandler;
//!
//! #[async_trait]
//! impl JobHandler for EmailHandler {
//!     type Job = SendEmail;
//!     async fn handle(&self, job: SendEmail, ctx: JobContext) -> Result<(), JobError> {
//!         tracing::info!(to = %job.to, attempt = ctx.attempt, "sending");
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> queuey::Result<()> {
//!     let backend = Arc::new(RabbitMqBackend::connect("amqp://guest:guest@localhost:5672/%2f").await?);
//!
//!     let producer = Producer::<AppQueues, _>::new(backend.clone()).await?;
//!     producer.enqueue(&SendEmail { to: "a@b.c".into(), body: "hi".into() }).await?;
//!
//!     let worker = Worker::<AppQueues, _>::builder(backend)
//!         .handler(EmailHandler)
//!         // The worker leaves the shared backend open by default; this process has
//!         // nothing else to do with it.
//!         .close_backend_on_shutdown(true)
//!         .build()
//!         .await?;
//!     worker.run().await?;
//!     Ok(())
//! }
//! ```
//!
//! # Deferral (rate limits, `Retry-After`)
//!
//! A job that cannot run *yet*, because the API answered `429 Too Many Requests` with
//! `Retry-After: 30`, is not a failure. The handler returns
//! [`JobError::deferred`] / [`JobError::deferred_msg`] and the job is parked for
//! exactly that long, then comes back *ahead* of the backlog that piled up
//! meanwhile. It costs no attempt: `ctx.attempt` is unchanged and the retry policy
//! is never consulted, so a job may defer itself indefinitely.
//! [`JobContext::deferrals`] counts how often it happened, which is how a handler
//! caps it (return [`JobError::Fatal`] once it has had enough).
//!
//! Coming back first is a broker priority: `#[queue(max_priority = n)]` declares
//! how many levels the queue has, and deferred jobs return at the top while
//! ordinary work sits at `0`. The default is [`DEFAULT_MAX_PRIORITY`]; `0` turns
//! priorities off, and deferred jobs then come back FIFO. [`Producer::defer`] is
//! the producer-side twin: same hold, same priority, without a first run.
//!
//! ```
//! use std::time::Duration;
//!
//! use queuey::prelude::*;
//!
//! #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
//! #[queues(prefix = "docs")]
//! enum ApiQueues {
//!     #[queue(prefetch = 4, max_priority = 10)]
//!     Calls,
//! }
//!
//! #[derive(Debug, Serialize, Deserialize, Job)]
//! #[job(queue = ApiQueues::Calls)]
//! struct CallApi { url: String }
//!
//! struct CallHandler;
//!
//! #[async_trait]
//! impl JobHandler for CallHandler {
//!     type Job = CallApi;
//!     async fn handle(&self, job: CallApi, ctx: JobContext) -> Result<(), JobError> {
//!         // Pretend the API answered `429` with `Retry-After: 30`.
//!         let retry_after: Option<Duration> = (ctx.deferrals == 0).then_some(Duration::from_secs(30));
//!         match retry_after {
//!             // Not a failure: wait exactly that long, keep the attempt, come back first.
//!             Some(delay) if ctx.deferrals < 5 => Err(JobError::deferred_msg(delay, "rate limited")),
//!             Some(_) => Err(JobError::fatal_msg("still rate limited after five deferrals")),
//!             None => Ok(()),
//!         }
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> queuey::Result<()> {
//!     let backend = Arc::new(MemoryBackend::new());
//!     let producer = Producer::<ApiQueues, _>::new(backend.clone()).await?;
//!     let _worker = Worker::<ApiQueues, _>::builder(backend.clone())
//!         .handler(CallHandler)
//!         .build()
//!         .await?;
//!
//!     // The same hold, without a first run: released in 30s, at the queue's top priority.
//!     producer
//!         .defer(&CallApi { url: "https://example.com".into() }, Duration::from_secs(30))
//!         .await?;
//!
//!     assert_eq!(backend.deferred("docs.calls"), 1, "held, not yet deliverable");
//!     assert_eq!(backend.pending("docs.calls"), 0);
//!     assert_eq!(ApiQueues::Calls.config().max_priority, Some(DEFAULT_MAX_PRIORITY));
//!     Ok(())
//! }
//! ```
//!
//! # Where to look next
//!
//! * [`prelude`]: the one glob import above.
//! * [`macro@Queues`] / [`macro@Job`]: the full attribute grammar.
//! * [`Worker`] / [`Producer`]: the runtime.
//! * [`RetryPolicy`] / [`Backoff`]: retry and backoff semantics, and
//!   [`WorkerBuilder::retry_override`] / [`WorkerBuilder::job_retry_override`] to replace
//!   the compiled-in numbers with what this process reads from its configuration.
//! * [`DeadLetterHook`] (via [`WorkerBuilder::on_dead_letter`]): one callback for every
//!   job the worker gives up on, including the ones no handler ever saw.
//! * [`JobError::Deferred`] / [`Producer::defer`]: deferral semantics.
//! * [`MemoryBackend`]: an in-process backend for tests; see the
//!   `memory_quickstart` example.
//!
//! # How the derive macros find this crate
//!
//! Generated code needs a path to `queuey-core`. The macros read the
//! *calling* crate's `Cargo.toml` and prefer a dependency on `queuey`,
//! emitting `::queuey::__core`, a hidden re-export of the core crate.
//! Depending only on this facade therefore needs no `crate = "..."` attribute.
//! A crate that depends on `queuey-core` directly gets
//! `::queuey_core` instead.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

// Makes `::queuey::__core` (what the derives emit) resolve inside this
// crate itself, so the macros work in its own doctests and examples.
extern crate self as queuey;

pub mod prelude;

pub use queuey_core::*;
pub use queuey_macros::{Job, Queues};

#[cfg(feature = "rabbitmq")]
pub use queuey_rabbitmq::{
    self as rabbitmq, Attempt, BackoffPolicy, RabbitMqBackend, RabbitMqOptions, Rebuilding,
    ReconnectPolicy,
};

/// The core crate, re-exported for generated code. Not part of the public API.
#[doc(hidden)]
pub use queuey_core as __core;
