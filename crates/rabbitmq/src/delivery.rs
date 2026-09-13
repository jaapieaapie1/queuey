//! A single message in flight from RabbitMQ.

use std::time::Duration;

use async_trait::async_trait;
use lapin::{
    Acker,
    options::{BasicAckOptions, BasicRejectOptions},
};
use queuey_core::{Delivery, Envelope, Result};
use tracing::warn;

use crate::{
    error::{RabbitMqError, amqp},
    publisher::{Hold, Publisher},
};

/// One RabbitMQ message, decoded into an [`Envelope`].
///
/// Exactly one of [`ack`](Delivery::ack), [`retry`](Delivery::retry),
/// [`defer`](Delivery::defer) or [`dead_letter`](Delivery::dead_letter) must be
/// called; the trait consumes the delivery so the compiler enforces "at most
/// once", and an un-acked delivery is redelivered by the broker when the
/// consumer channel closes.
///
/// [`retry`](Delivery::retry), [`defer`](Delivery::defer) and
/// [`dead_letter`](Delivery::dead_letter) publish *before* they ack, and
/// propagate the publish error without acking, so a failure leaves the original
/// message unacknowledged for the broker to redeliver rather than dropping the
/// job. Because publishes are `mandatory`, a missing `q.dead` counts as a
/// failure. A hold queue cannot be missing: `retry` and `defer` declare it
/// themselves, immediately before publishing, but that declaration can be
/// *refused*, and [`retry`](Delivery::retry) says what happens then.
///
/// When
/// [`declare_dead_letter_queues`](crate::RabbitMqOptions::declare_dead_letter_queues)
/// is `false` this backend does not own `q.dead`, so
/// [`dead_letter`](Delivery::dead_letter) rejects the message (`requeue = false`)
/// instead of publishing to it, and logs why.
pub struct RabbitMqDelivery {
    envelope: Envelope,
    acker: Acker,
    publisher: Publisher,
}

impl std::fmt::Debug for RabbitMqDelivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RabbitMqDelivery")
            .field("job_id", &self.envelope.job_id)
            .field("job_type", &self.envelope.job_type)
            .field("queue", &self.envelope.queue)
            .field("attempt", &self.envelope.attempt)
            .finish_non_exhaustive()
    }
}

impl RabbitMqDelivery {
    /// Pair a decoded envelope with the acker of the message it came from.
    pub(crate) fn new(envelope: Envelope, acker: Acker, publisher: Publisher) -> Self {
        Self {
            envelope,
            acker,
            publisher,
        }
    }

    /// Turn lapin's "the acker was already used or is poisoned" signal into an
    /// error: the broker never recorded the outcome, so reporting success would
    /// tell the worker a job was settled when it will in fact be redelivered.
    fn settled(&self, settled: bool, operation: &'static str) -> Result<()> {
        if settled {
            return Ok(());
        }
        Err(RabbitMqError::AlreadySettled {
            operation,
            job_id: self.envelope.job_id.to_string(),
            queue: self.envelope.queue.clone(),
        }
        .into_core())
    }

    /// Ack the underlying AMQP message.
    async fn ack_original(&self) -> Result<()> {
        let acked = self
            .acker
            .ack(BasicAckOptions::default())
            .await
            .map_err(amqp)?;
        self.settled(acked, "ack")
    }

    /// Reject the underlying AMQP message without requeueing it.
    ///
    /// The broker then applies whatever the queue's own `x-dead-letter-exchange`
    /// policy says, and drops the message if there is none.
    async fn reject_original(&self) -> Result<()> {
        let rejected = self
            .acker
            .reject(BasicRejectOptions { requeue: false })
            .await
            .map_err(amqp)?;
        self.settled(rejected, "reject")
    }
}

#[async_trait]
impl Delivery for RabbitMqDelivery {
    fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    async fn ack(self: Box<Self>) -> Result<()> {
        self.ack_original().await
    }

    async fn dead_letter(self: Box<Self>, reason: &str) -> Result<()> {
        if !self.publisher.options().declare_dead_letter_queues {
            // `q.dead` is not this backend's to write to, and a `mandatory`
            // publish to a queue nobody declared is an error rather than a
            // silent drop. Hand the message back to the broker instead: its own
            // dead-letter policy on `q` applies if the operator configured one,
            // and otherwise the message is discarded.
            warn!(
                job_id = %self.envelope.job_id,
                queue = %self.envelope.queue,
                reason,
                "dead-letter queues are disabled; rejecting the delivery instead of publishing \
                 to the dead-letter queue (the broker's own dead-letter policy applies, if any, \
                 otherwise the job is dropped)"
            );
            return self.reject_original().await;
        }

        self.publisher
            .publish_dead_letter(&self.envelope, reason)
            .await?;
        self.ack_original().await
    }

    /// Publish first, ack second.
    ///
    /// `next` is written into the hold queue for `delay` (declared on the spot,
    /// `mandatory`, waited on for a publisher confirm) and only once the broker
    /// has taken responsibility for it is the original acked. If the publish
    /// fails the `?` returns before the ack, so the original stays
    /// unacknowledged and the broker redelivers it, so the job is retried
    /// rather than silently dropped. The reverse order would lose a job on any
    /// broker hiccup between the two.
    ///
    /// The delay is rounded up to
    /// [`RabbitMqOptions::retry_granularity`](crate::RabbitMqOptions::retry_granularity),
    /// and `next` returns to `q` at the priority it carries, `0` after
    /// [`Envelope::next_attempt`], so it joins the back of the queue.
    ///
    /// Three things count as that failure, and all three leave the original
    /// unacked (the worker counts a settle failure and the broker redelivers the
    /// job when the consumer channel closes):
    ///
    /// * the hold queue exists with different arguments, so the declaration is
    ///   refused with `PRECONDITION_FAILED`. The declaration runs on its own
    ///   channel, so concurrent publishes are untouched;
    /// * `next.queue` was never declared through this backend, so the hold
    ///   queue's durability and dead-letter target are unknown
    ///   ([`Error::UnknownQueue`](queuey_core::Error::UnknownQueue));
    /// * `delay` is longer than
    ///   [`MAX_DEFERRAL_MS`](crate::topology::MAX_DEFERRAL_MS) (~24.8 days),
    ///   which is refused rather than shortened, because a hold never releases
    ///   a job early.
    async fn retry(self: Box<Self>, next: Envelope, delay: Duration) -> Result<()> {
        self.publisher
            .publish_held(&next, delay, Hold::Retry)
            .await?;
        self.ack_original().await
    }

    /// Publish first, ack second, the same rule and the same failure modes as
    /// [`retry`](Delivery::retry).
    ///
    /// The delay is rounded up to
    /// [`RabbitMqOptions::deferred_granularity`](crate::RabbitMqOptions::deferred_granularity)
    /// instead, and `next` carries its queue's top priority, so it returns
    /// ahead of the backlog.
    ///
    /// A caveat on "ahead of the backlog": a consumer with prefetch `N` is
    /// already holding up to `N` messages of that backlog, and the returning
    /// deferral cannot overtake those. It is first among what is still on the
    /// queue.
    async fn defer(self: Box<Self>, next: Envelope, delay: Duration) -> Result<()> {
        self.publisher
            .publish_held(&next, delay, Hold::Deferral)
            .await?;
        self.ack_original().await
    }
}
