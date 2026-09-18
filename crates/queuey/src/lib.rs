//! Type-safe job queues for Rust on RabbitMQ.
//!
//! Queues are an **enum** with [`macro@Queues`]; jobs are **structs** with
//! [`macro@Job`]. A job statically knows its queue, so [`Producer::enqueue`] and
//! [`WorkerBuilder::handler`] are checked at compile time. A job belonging to
//! another application's queue set does not compile.
//!
//! This crate is a facade: it re-exports [`queuey_core`], the derive macros from
//! `queuey-macros`, and, behind the default `rabbitmq` feature, the RabbitMQ
//! backend. Depending on it alone is enough.
//!
//! The quickstart below is the same program whichever backend you pick, because
//! everything above the backend line is transport-agnostic. With the default
//! features it connects to a broker; with `default-features = false` there is no
//! `lapin` in the dependency tree and it runs in process on [`MemoryBackend`],
//! which is also what tests and the `memory_quickstart` example use.
//!
#![cfg_attr(
    feature = "rabbitmq",
    doc = "See [`queuey_rabbitmq`] for the topology it declares, its options and how it"
)]
#![cfg_attr(feature = "rabbitmq", doc = "recovers a dropped connection.")]
#![cfg_attr(
    not(feature = "rabbitmq"),
    doc = "Turn the `rabbitmq` feature back on to get `RabbitMqBackend`, which is what"
)]
#![cfg_attr(
    not(feature = "rabbitmq"),
    doc = "swaps in on the backend line below; nothing else in the program changes."
)]
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
// The one line that depends on the feature. Written as two `cfg_attr`s rather
// than two whole examples so the quickstart a newcomer reads stays a single
// program, and so the `rabbitmq`-off build cannot drift out of step with it.
#![cfg_attr(
    feature = "rabbitmq",
    doc = "    let backend = Arc::new(RabbitMqBackend::connect(\"amqp://guest:guest@localhost:5672/%2f\").await?);"
)]
#![cfg_attr(
    not(feature = "rabbitmq"),
    doc = "    let backend = Arc::new(MemoryBackend::new());"
)]
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
//! Generated code needs a path to `queuey-core`. The macros read the *calling*
//! crate's `Cargo.toml`. A crate that names `queuey-core` there gets
//! `::queuey_core`; a crate that names only this facade gets `::queuey::__core`,
//! a hidden re-export of the core crate. Either way, depending on one crate is
//! enough and no `crate = "..."` attribute is needed.
//!
//! The core crate is looked up first on purpose. A manifest is not a build
//! graph: `Cargo.toml` says nothing about which target is being compiled, so a
//! crate that keeps `queuey-core` in `[dependencies]` and this facade in
//! `[dev-dependencies]` (or as an `optional` dependency whose feature is off)
//! would otherwise be handed `::queuey::__core` for its own lib, where the
//! facade is not linked. `::queuey_core` is safe in that situation and in every
//! other one, because this facade only re-exports the core crate.
//!
//! If neither crate is in the manifest, the derives say so and point at
//! `#[queues(crate = "...")]` / `#[job(crate = "...")]`, which names the core
//! crate explicitly and always wins.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(unreachable_pub)]

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
