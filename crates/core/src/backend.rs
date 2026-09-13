//! Transport abstraction: [`Backend`], [`Delivery`] and [`DeliveryStream`].

use std::{pin::Pin, time::Duration};

use async_trait::async_trait;
use futures::Stream;

use crate::{envelope::Envelope, error::Result, queue::QueueConfig};

/// A message received from the broker. Must be acked or nacked exactly once.
#[async_trait]
pub trait Delivery: Send + 'static {
    /// The message this delivery carries.
    fn envelope(&self) -> &Envelope;

    /// Successfully processed; remove from broker.
    async fn ack(self: Box<Self>) -> Result<()>;

    /// Failed permanently (or attempts exhausted); route to dead-letter storage.
    async fn dead_letter(self: Box<Self>, reason: &str) -> Result<()>;

    /// Failed transiently; schedule `next` (already `attempt + 1`) to be redelivered
    /// after `delay`. Implementations must ack the original *after* the retry is
    /// durably scheduled so no message is lost.
    async fn retry(self: Box<Self>, next: Envelope, delay: Duration) -> Result<()>;

    /// Did not fail, but must run again in `delay`: durably schedule `next` (already
    /// `deferrals + 1` with its priority set, `attempt` unchanged) to reappear on
    /// `next.queue`, **then** ack the original.
    ///
    /// Same "publish before ack" rule as [`Delivery::retry`]: if the scheduling fails,
    /// the original must be left unacked so the broker redelivers it.
    ///
    /// How the message is held is backend-specific (a dedicated hold queue per delay
    /// on RabbitMQ, a timer in `MemoryBackend`), but the observable contract is the
    /// same: nothing is delivered before `delay` has passed, and when it comes back it
    /// carries `next.priority`, so it overtakes normally enqueued work on a queue that
    /// supports priorities. See [`crate::JobError::Deferred`].
    async fn defer(self: Box<Self>, next: Envelope, delay: Duration) -> Result<()>;
}

/// Stream of deliveries produced by [`Backend::consume`].
pub type DeliveryStream = Pin<Box<dyn Stream<Item = Result<Box<dyn Delivery>>> + Send>>;

/// A transport. Implementations: `MemoryBackend` (this crate), `RabbitMqBackend`.
#[async_trait]
pub trait Backend: Send + Sync + 'static {
    /// Idempotently create all queues (plus any retry / dead-letter infrastructure).
    async fn declare(&self, queues: &[QueueConfig]) -> Result<()>;

    /// Publish `envelope` to `envelope.queue`, optionally delayed.
    async fn publish(&self, envelope: &Envelope, delay: Option<Duration>) -> Result<()>;

    /// Publish `envelope` into a hold that releases it onto `envelope.queue` after
    /// `delay`. This is the publish half of [`Delivery::defer`], also used by
    /// [`crate::Producer::defer`].
    ///
    /// Differs from `publish` with a delay: that one shares a single wait queue per
    /// queue (on RabbitMQ `q.retry`, where mixed per-message TTLs block each other at
    /// the head) and the message returns with whatever priority it carries. A deferral
    /// is held per delay, so equal delays drain strictly in order, and the envelope is
    /// expected to carry the queue's top priority so it overtakes the backlog.
    ///
    /// Hold queue naming and lifetime are backend-specific.
    async fn defer(&self, envelope: &Envelope, delay: Duration) -> Result<()>;

    /// Start consuming `queue` with the given prefetch. The stream ends when the
    /// backend is closed or the connection is lost.
    async fn consume(&self, queue: &QueueConfig) -> Result<DeliveryStream>;

    /// Graceful shutdown: stop all consumers, flush, close connections.
    async fn close(&self) -> Result<()>;
}
