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
//!     RabbitMqOptions::default().retry_granularity(Duration::from_secs(5)),
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
//! Each logical queue `q` is backed by two long-lived broker queues, `q` and
//! `q.dead`, plus a short-lived *hold* queue `q.deferred.{ttl_ms}` per distinct
//! delay. Every wait, whether a retry backoff, a delayed enqueue or a deferral,
//! happens in a hold queue. See [`topology`] for the exact arguments.
//!
//! # Why hold queues, and not one wait queue with per-message expirations
//!
//! RabbitMQ only expires the message at the *head* of a classic queue. In a
//! shared wait queue, a message with a five-minute `expiration` at the head
//! holds back every one-second `expiration` queued behind it, and exponential
//! backoff produces exactly that mix of delays. So instead the delay is part of
//! the queue *name*, the wait is the queue-wide `x-message-ttl`, and every
//! message in `q.deferred.30000` expires in publish order. A short wait is never
//! stuck behind a long one, because the two live in different queues.
//!
//! Delays are rounded **up** to a granularity to bound how many hold queues
//! exist at once: [`RabbitMqOptions::retry_granularity`] for retries and
//! [`Producer::enqueue_after`](queuey_core::Producer::enqueue_after),
//! [`RabbitMqOptions::deferred_granularity`] for deferrals, both `1s` by
//! default. The hold queue is declared on demand right before each publish:
//! an idle hold queue deletes itself one TTL after the last publish to it
//! (`x-expires = 2 * TTL`), and every declare resets that timer.
//!
//! # Retry versus deferral
//!
//! Both wait in the same hold queues. They differ in what happens when the job
//! is back on `q`:
//!
//! * A **retry** ([`Delivery::retry`](queuey_core::Delivery::retry), and a
//!   delayed [`Backend::publish`](queuey_core::Backend::publish)) carries
//!   priority `0` and joins the back of the queue like any other message. Its
//!   attempt counter has been incremented.
//! * A **deferral** ([`Backend::defer`](queuey_core::Backend::defer),
//!   [`Delivery::defer`](queuey_core::Delivery::defer)) is what a
//!   `429 Too Many Requests` with `Retry-After: 30` needs: the job did not fail,
//!   must not burn an attempt, and must run **ahead of the backlog** when it
//!   returns. `q` is declared with `x-max-priority` from
//!   [`QueueConfig::max_priority`](queuey_core::QueueConfig::max_priority)
//!   (default `Some(10)`), every publish carries the envelope's `priority`, and a
//!   deferred envelope carries the queue's top level, so it is served before
//!   everything that piled up meanwhile. "Ahead of the backlog" means ahead of
//!   what is still *on* the queue: a consumer with prefetch `N` already holds up
//!   to `N` backlog messages, and the returning deferral is first among what is
//!   left.
//!
//! ## What a hold requires
//!
//! * **The queue must have been declared through this backend, in this
//!   process.** Otherwise the hold queue's durability and the queue it
//!   dead-letters back to would be guesses, and a TTL expiry into a queue that
//!   does not exist is discarded silently by the broker. Unlike a
//!   `mandatory` publish, nothing comes back and nothing is logged. Holding
//!   onto an unknown queue is
//!   [`Error::UnknownQueue`](queuey_core::Error::UnknownQueue)
//!   instead. `Producer::new` and `WorkerBuilder::build` declare the queue set;
//!   `Producer::new_undeclared` deliberately does not, so a producer built that
//!   way can enqueue, but not enqueue with a delay or defer.
//! * **The delay must fit.** It is capped at
//!   [`MAX_DEFERRAL_MS`](topology::MAX_DEFERRAL_MS), about 24.8 days. That is half of
//!   what a 32-bit millisecond TTL can express, because the hold queue's
//!   `x-expires` is twice its TTL. A longer delay is refused rather than
//!   clamped: releasing a job early is the one thing a hold promises not to
//!   do. Rounding up to the granularity happens first, so a delay just under the
//!   cap can be refused too.
//!
//! Both failures happen *before* anything is acked, so from
//! [`Delivery::retry`](queuey_core::Delivery::retry) and
//! [`Delivery::defer`](queuey_core::Delivery::defer) they leave the
//! original message unacknowledged and the broker redelivers it.
//!
//! ## Upgrading from the `q.retry` wait queue
//!
//! Earlier versions declared a `q.retry` queue per work queue and published
//! retries into it with a per-message `expiration`. This version neither
//! declares nor uses it. Nothing needs migrating: messages still waiting in an
//! existing `q.retry` expire back onto `q` on their own, because the
//! dead-letter routing is an argument of that queue, and workers running the
//! old version keep declaring it themselves. Delete `q.retry` once it is empty
//! and no old worker is left. `RabbitMqOptions::retry_suffix` is gone with it;
//! [`RabbitMqOptions::retry_granularity`] is the retry tunable now.
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
//! `q.dead` and hold queues are unchanged, so only `q` is affected.
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
//!   fails, so a job is never lost. At worst it is redelivered. Different
//!   delays never block each other: each waits in its own hold queue. With
//!   [`RabbitMqOptions::declare_dead_letter_queues`] off, `dead_letter` rejects
//!   the delivery instead of publishing to a `q.dead` this backend does not own.
//! * Messages whose body is not a valid [`queuey_core::Envelope`] are
//!   moved aside and logged, never surfaced as a stream error, so one poison
//!   message cannot stall a consumer.
//!
//! # Reconnection
//!
//! The connection is a slot, not a socket. When it drops, the first operation to
//! notice dials a replacement while the rest queue behind that one attempt,
//! every queue this backend declared is re-declared on the new connection, and
//! the consumer streams resubscribe and keep yielding. A publish issued during
//! the outage waits rather than failing, and
//! [`Worker::run`](queuey_core::Worker::run) keeps running across a broker
//! restart.
//!
//! Pacing is [`RabbitMqOptions::reconnect`]'s job, and it takes any
//! [`ReconnectPolicy`]. The default, [`BackoffPolicy`], is unlimited, so a
//! broker that never returns is a stalled worker and a stream of `WARN` logs
//! rather than an error; bound it with [`BackoffPolicy::max_attempts`], swap in
//! a policy of your own when a backoff curve is not the right shape (a circuit
//! breaker, a schedule, a different answer for an authentication failure than
//! for a refused connection), or pass [`None`] for the original fail-fast
//! behaviour, where consumer streams end and
//! [`Error::ConsumerStopped`](queuey_core::Error::ConsumerStopped) surfaces.
//!
//! ```
//! use std::time::Duration;
//!
//! use queuey_rabbitmq::{Attempt, RabbitMqOptions, Rebuilding, ReconnectPolicy};
//!
//! /// Retries the connection forever, but gives up on a consumer whose queue
//! /// has gone missing: waiting does not bring a deleted queue back.
//! #[derive(Debug)]
//! struct ConnectionOnly;
//!
//! impl ReconnectPolicy for ConnectionOnly {
//!     fn next_delay(&self, attempt: Attempt<'_>) -> Option<Duration> {
//!         match attempt.rebuilding {
//!             Rebuilding::Consumer if attempt.failures >= 3 => None,
//!             _ => Some(Duration::from_secs(2)),
//!         }
//!     }
//! }
//!
//! let options = RabbitMqOptions::default().reconnect_with(ConnectionOnly);
//! ```
//!
//! Two things do not survive an outage. Jobs that were in flight are requeued by
//! the broker and delivered again, and their first run's settle fails (counted
//! in
//! [`WorkerHandle::settle_failures`](queuey_core::WorkerHandle::settle_failures));
//! that is the at-least-once contract above, not a new one. And the *first*
//! connection is not retried at all: [`RabbitMqBackend::connect`] fails if the
//! broker is unreachable at startup, rather than blocking its caller in a
//! backoff loop.
//!
//! [`queuey`]: queuey_core

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod backend;
mod connection;
mod delivery;
mod error;
mod options;
mod publisher;
mod reconnect;

pub mod codec;
pub mod topology;

pub use backend::RabbitMqBackend;
pub use delivery::RabbitMqDelivery;
pub use options::RabbitMqOptions;
pub use reconnect::{Attempt, BackoffPolicy, Rebuilding, ReconnectPolicy};

/// Re-export of the backoff curve [`BackoffPolicy`] is built from, so a policy
/// can be tuned without naming `queuey-core` as a dependency.
pub use queuey_core::Backoff;

/// Re-export of the `lapin` version this backend is built against, so callers
/// can name [`lapin::ConnectionProperties`] without pinning it themselves.
pub use lapin;
