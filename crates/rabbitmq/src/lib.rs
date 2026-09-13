//! RabbitMQ backend for [`queuey`], built on [`lapin`].
//!
//! [`RabbitMqBackend`] implements [`queuey_core::Backend`]: it owns one
//! AMQP connection, publishes with publisher confirms, and hands the worker
//! runtime a stream of [`RabbitMqDelivery`] values.
//!
//! ```no_run
//! use std::{sync::Arc, time::Duration};
//!
//! use queuey_core::{Backend, QueueConfig};
//! use queuey_rabbitmq::{RabbitMqBackend, RabbitMqOptions};
//!
//! # async fn example() -> queuey_core::Result<()> {
//! let backend = RabbitMqBackend::with_options(
//!     "amqp://guest:guest@localhost:5672/%2f",
//!     RabbitMqOptions::default().retry_suffix(".retry"),
//! )
//! .await?;
//!
//! let emails = QueueConfig::new("myapp.emails").prefetch(10);
//! backend.declare(std::slice::from_ref(&emails)).await?;
//!
//! let backend = Arc::new(backend);
//! // ... hand `backend` to a `Producer` / `Worker` ...
//! backend.close().await?;
//! # Ok(()) }
//! ```
//!
//! # Topology
//!
//! Each logical queue `q` is backed by three broker queues, `q`, `q.retry` and
//! `q.dead`, plus a short-lived *hold* queue `q.deferred.{ttl_ms}` per distinct
//! deferral delay. See [`topology`] for the exact arguments and for the
//! head-of-line caveat that comes with TTL-based retry queues.
//!
//! # Deferral
//!
//! [`Backend::defer`](queuey_core::Backend::defer) and
//! [`Delivery::defer`](queuey_core::Delivery::defer) hold a job for a
//! delay and then put it back on `q` **ahead of the backlog**. That is the shape a
//! `429 Too Many Requests` with `Retry-After: 30` needs, where the job did not
//! fail and must not burn an attempt.
//!
//! Two mechanisms do that:
//!
//! * **Hold queues.** A deferral is published to `q.deferred.{ttl_ms}`, a queue
//!   whose whole purpose is to dead-letter its contents back onto `q` after
//!   `ttl_ms`. The delay is the queue's `x-message-ttl`, never a per-message
//!   `expiration`, so every message in it expires in publish order and short
//!   deferrals are never stuck behind long ones. The delay is rounded up to
//!   [`RabbitMqOptions::deferred_granularity`] (default `1s`) to bound how many
//!   such queues exist, and the queue is declared on demand right before each
//!   deferred publish: an idle hold queue deletes itself one TTL after the last
//!   deferred publish to it (`x-expires = 2 * TTL`), and every declare resets
//!   that timer.
//! * **Priorities.** `q` is declared with `x-max-priority` from
//!   [`QueueConfig::max_priority`](queuey_core::QueueConfig::max_priority)
//!   (default `Some(10)`), and every publish carries the envelope's `priority`.
//!   Normal work is `0`; a deferred envelope carries the queue's top level, so
//!   when it comes back it is served before everything that piled up meanwhile.
//!   "Ahead of the backlog" means ahead of what is still *on* the queue: a
//!   consumer with prefetch `N` already holds up to `N` backlog messages, and
//!   the returning deferral is first among what is left.
//!
//! ## What deferral requires
//!
//! * **The queue must have been declared through this backend, in this
//!   process.** Otherwise the hold queue's durability and the queue it
//!   dead-letters back to would be guesses, and a TTL expiry into a queue that
//!   does not exist is discarded silently by the broker. Unlike a
//!   `mandatory` publish, nothing comes back and nothing is logged. Deferring
//!   onto an unknown queue is
//!   [`Error::UnknownQueue`](queuey_core::Error::UnknownQueue)
//!   instead. `Producer::new` and `WorkerBuilder::build` declare the queue set;
//!   `Producer::new_undeclared` deliberately does not.
//! * **The delay must fit.** It is capped at
//!   [`MAX_DEFERRAL_MS`](topology::MAX_DEFERRAL_MS), about 24.8 days. That is half of
//!   what a 32-bit millisecond TTL can express, because the hold queue's
//!   `x-expires` is twice its TTL. A longer delay is refused rather than
//!   clamped: releasing a job early is the one thing a deferral promises not to
//!   do. Rounding up to the granularity happens first, so a delay just under the
//!   cap can be refused too.
//!
//! Both failures happen *before* anything is acked, so from
//! [`Delivery::defer`](queuey_core::Delivery::defer) they leave the
//! original message unacknowledged and the broker redelivers it.
//!
//! ## Breaking topology change
//!
//! `x-max-priority` is a *declaration* argument, and RabbitMQ refuses to change
//! the arguments of a queue that already exists: the declaration is answered
//! with `PRECONDITION_FAILED`, which closes the channel and surfaces here as an
//! error from [`declare`](queuey_core::Backend::declare).
//!
//! A `q` created before this feature has no `x-max-priority`, so **declaring it
//! again with the default config will fail**. Either:
//!
//! * drain and delete `q`, then let this backend redeclare it. Deferred jobs
//!   then come back ahead of the backlog; or
//! * set `max_priority = 0` on the queue's
//!   [`QueueConfig`](queuey_core::QueueConfig) (or
//!   `#[queue(max_priority = 0)]`), which declares `q` exactly as before.
//!   Deferral still works, it just returns jobs FIFO instead of ahead of the
//!   queue.
//!
//! `q.retry`, `q.dead` and hold queues are unchanged, so only `q` is affected.
//!
//! # Guarantees
//!
//! * Every publish (enqueue, retry, defer, dead-letter) is `mandatory` and
//!   confirmed by the broker before it is reported as successful. A routing key
//!   that matches no queue is returned by the broker and reported as an error
//!   rather than passing for a confirmed publish.
//! * [`Delivery::retry`](queuey_core::Delivery::retry),
//!   [`Delivery::defer`](queuey_core::Delivery::defer) and
//!   [`Delivery::dead_letter`](queuey_core::Delivery::dead_letter)
//!   publish first and ack second, and skip the ack entirely when the publish
//!   fails, so a job is never lost. At worst it is redelivered. With
//!   [`RabbitMqOptions::declare_dead_letter_queues`] off, `dead_letter` rejects
//!   the delivery instead of publishing to a `q.dead` this backend does not own.
//! * Messages whose body is not a valid [`queuey_core::Envelope`] are
//!   moved aside and logged, never surfaced as a stream error, so one poison
//!   message cannot stall a consumer.
//!
//! # Not in scope for v1
//!
//! Reconnection. When the connection drops, consumer streams end and further
//! calls fail; supervising and rebuilding the backend is the caller's job.
//!
//! [`queuey`]: queuey_core

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod backend;
mod delivery;
mod error;
mod options;
mod publisher;

pub mod codec;
pub mod topology;

pub use backend::RabbitMqBackend;
pub use delivery::RabbitMqDelivery;
pub use options::RabbitMqOptions;

/// Re-export of the `lapin` version this backend is built against, so callers
/// can name [`lapin::ConnectionProperties`] without pinning it themselves.
pub use lapin;
