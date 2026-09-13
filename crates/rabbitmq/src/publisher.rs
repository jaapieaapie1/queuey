//! The shared publishing channels: one in confirm mode for publishes, one plain
//! channel for the on-demand hold queue declarations.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use lapin::{
    BasicProperties, Channel, Confirmation, Connection,
    options::{BasicPublishOptions, ConfirmSelectOptions},
};
use queuey_core::{Envelope, Error, QueueConfig, Result};
use tokio::sync::{Mutex, MutexGuard};
use tracing::{debug, warn};

use crate::{
    codec,
    error::{RabbitMqError, amqp, short_string},
    options::RabbitMqOptions,
    topology,
};

/// A cheap, cloneable handle to the publishing channels.
///
/// A single channel in confirm mode is shared by the backend and by every
/// in-flight [`RabbitMqDelivery`](crate::RabbitMqDelivery), because a delivery
/// must be able to re-publish (retry / dead-letter) long after the call that
/// produced it returned. The mutex is only held while the frames are written,
/// never while waiting for the broker's confirmation, so concurrent publishers
/// pipeline rather than serialise.
///
/// A channel exception (an unroutable `mandatory` publish is *not* one, but a
/// failed declaration or a broker-side error is) closes the channel for good.
/// The connection is kept so the channel can be reopened lazily on the next
/// publish; see [`Publisher::publish_confirmed`].
///
/// # Why declarations get their own channel
///
/// [`publish_held`](Publisher::publish_held) has to declare a hold queue before
/// it can publish into it, and a declaration is exactly the thing that can be
/// *refused*: a hold queue that already exists with different arguments is
/// answered with `PRECONDITION_FAILED`, which kills the channel it was issued
/// on. On the shared confirm channel that would take down every concurrent
/// publish with it (a dead-letter, an unrelated enqueue), and the declare's
/// round trip would have to be waited out under the publish mutex, serialising
/// publishers for the duration.
///
/// So declarations run on a second, non-confirm channel with its own mutex.
/// A refused declaration then costs exactly the one retry or deferral that
/// asked for it; the confirm channel never notices. Both channels are reopened
/// lazily when the broker has closed them.
#[derive(Clone)]
pub(crate) struct Publisher {
    connection: Arc<Connection>,
    channel: Arc<Mutex<Channel>>,
    /// Channel used only for the on-demand hold queue declarations. Not in
    /// confirm mode: nothing is published on it, and a declaration is
    /// synchronous already.
    declare_channel: Arc<Mutex<Channel>>,
    options: Arc<RabbitMqOptions>,
    /// Every [`QueueConfig`] this backend has declared, by queue name.
    ///
    /// A retry or deferral has to declare a hold queue, and a hold queue must be
    /// durable exactly when its main queue is: a transient hold queue in front
    /// of a durable work queue would silently lose held jobs on a broker restart.
    /// The publisher only ever sees an [`Envelope`], which carries a queue
    /// *name* and nothing else, so the backend records what it declared here and
    /// the publisher looks it up. Kept behind a plain `std` mutex: it is a
    /// handful of clones guarded for microseconds, never across an `await`.
    configs: Arc<StdMutex<HashMap<String, QueueConfig>>>,
}

impl std::fmt::Debug for Publisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Publisher").finish_non_exhaustive()
    }
}

impl Publisher {
    /// Wrap an already-opened confirm-mode `channel` plus a plain
    /// `declare_channel` for hold queue declarations.
    pub(crate) fn new(
        connection: Arc<Connection>,
        channel: Channel,
        declare_channel: Channel,
        options: Arc<RabbitMqOptions>,
    ) -> Self {
        Self {
            connection,
            channel: Arc::new(Mutex::new(channel)),
            declare_channel: Arc::new(Mutex::new(declare_channel)),
            options,
            configs: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    /// Remember what `config` was declared as, for later hold queue declarations.
    ///
    /// Called by [`RabbitMqBackend::declare`](crate::RabbitMqBackend) for every
    /// queue it successfully declares. Re-declaring overwrites.
    pub(crate) fn remember(&self, config: &QueueConfig) {
        self.lock_configs()
            .insert(config.name.clone(), config.clone());
    }

    /// The config `queue` was declared with, or [`None`] if this backend never
    /// declared it.
    ///
    /// There is deliberately no fallback. Guessing a config would guess the hold
    /// queue's *durability*, and a transient hold queue in front of a durable
    /// work queue loses held jobs on a broker restart, while a durable one in
    /// front of a transient queue is refused outright once the names collide.
    /// Worse, the routing key the hold queue dead-letters to would name a queue
    /// that may not exist at all: unlike a `mandatory` publish, a TTL expiry
    /// into a missing queue is discarded by the broker silently, so the job
    /// would vanish an hour later with nothing reported anywhere.
    ///
    /// [`publish_held`](Publisher::publish_held) therefore turns [`None`] into
    /// [`Error::UnknownQueue`].
    fn config_for(&self, queue: &str) -> Option<QueueConfig> {
        self.lock_configs().get(queue).cloned()
    }

    /// Lock the config map, ignoring poisoning.
    ///
    /// The map holds plain data with no invariants to break, so a panic in
    /// another thread while it was locked is no reason to fail every subsequent
    /// publish.
    fn lock_configs(&self) -> std::sync::MutexGuard<'_, HashMap<String, QueueConfig>> {
        self.configs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Open a fresh channel and put it into confirm mode.
    pub(crate) async fn open_confirm_channel(connection: &Connection) -> Result<Channel> {
        let channel = connection.create_channel().await.map_err(amqp)?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(amqp)?;
        Ok(channel)
    }

    /// The options this backend was built with.
    pub(crate) fn options(&self) -> &RabbitMqOptions {
        &self.options
    }

    /// The dead-letter queue for `queue`.
    pub(crate) fn dead_queue(&self, queue: &str) -> String {
        topology::dead_queue_name(queue, &self.options.dead_suffix)
    }

    /// The hold queue holding `ttl_ms`-long waits of `queue`.
    pub(crate) fn deferred_queue(&self, queue: &str, ttl_ms: u32) -> String {
        topology::deferred_queue_name(queue, &self.options.deferred_suffix, ttl_ms)
    }

    /// Lock the shared channel, reopening it first if it has died.
    ///
    /// A channel exception (a broker-side error) closes the channel permanently,
    /// and every user of this publisher would then fail for ever. Reopening
    /// lazily on the next publish keeps one bad operation from disabling the
    /// process.
    ///
    /// Nothing is *declared* on this channel (see
    /// [`with_declare_channel`](Publisher::with_declare_channel)), so the one
    /// operation the broker routinely refuses cannot close it. An unroutable
    /// `mandatory` publish is not a channel exception either.
    ///
    /// The caller decides how long to hold the guard: [`publish_confirmed`] lets
    /// go before awaiting the broker's confirmation, so publishers pipeline.
    ///
    /// [`publish_confirmed`]: Publisher::publish_confirmed
    async fn with_channel(&self, context: &str) -> Result<MutexGuard<'_, Channel>> {
        let mut channel = self.channel.lock().await;
        if !channel.status().connected() {
            warn!(
                queue = context,
                channel = channel.id(),
                "publishing channel is closed; opening a replacement in confirm mode"
            );
            *channel = Self::open_confirm_channel(&self.connection).await?;
        }
        Ok(channel)
    }

    /// Lock the declaration channel, reopening it first if it has died.
    ///
    /// Separate from the publishing channel on purpose: redeclaring a hold queue
    /// whose arguments differ from the existing one's is answered with
    /// `PRECONDITION_FAILED`, which closes the channel. Doing that on the shared
    /// confirm channel would fail every publish in flight on it, and the
    /// declare's round trip would be waited out under the publish mutex. Here it
    /// costs only the retry or deferral that asked for it, plus one reopened
    /// channel.
    async fn with_declare_channel(&self, context: &str) -> Result<MutexGuard<'_, Channel>> {
        let mut channel = self.declare_channel.lock().await;
        if !channel.status().connected() {
            warn!(
                queue = context,
                channel = channel.id(),
                "declaration channel is closed; opening a replacement"
            );
            *channel = self.connection.create_channel().await.map_err(amqp)?;
        }
        Ok(channel)
    }

    /// Publish to the default exchange with `queue` as the routing key and wait
    /// for the broker's confirmation.
    ///
    /// Publishes are `mandatory`: a routing key that matches no queue is handed
    /// straight back by the broker instead of being discarded, and that return
    /// is reported as an error (see [`confirmation_to_result`]). Callers may
    /// therefore treat `Ok(())` as "durably handed over to a real queue", which
    /// is what makes it safe for a delivery to ack the original afterwards.
    ///
    /// If the shared channel has died (a channel exception closes it
    /// permanently) a new confirm-mode channel is opened and swapped in first,
    /// so one bad operation does not disable publishing for the process.
    pub(crate) async fn publish_confirmed(
        &self,
        queue: &str,
        payload: &[u8],
        properties: BasicProperties,
    ) -> Result<()> {
        let routing_key = short_string(queue)?;

        let confirm = {
            let channel = self.with_channel(queue).await?;
            channel
                .basic_publish(
                    // The default exchange routes by queue name.
                    "".into(),
                    routing_key,
                    BasicPublishOptions {
                        // Unroutable means lost; make the broker say so.
                        mandatory: true,
                        immediate: false,
                    },
                    payload,
                    properties,
                )
                .await
                .map_err(amqp)?
            // The guard is dropped here: the confirmation is awaited without
            // blocking other publishers.
        };

        confirmation_to_result(confirm.await.map_err(amqp)?, queue)?;
        debug!(queue, bytes = payload.len(), "publish confirmed");
        Ok(())
    }

    /// Publish `envelope` to its own queue, to be consumed as soon as possible.
    pub(crate) async fn publish_envelope(&self, envelope: &Envelope) -> Result<()> {
        let payload = envelope.to_bytes()?;
        let properties = codec::props_for(envelope);
        self.publish_confirmed(&envelope.queue, &payload, properties)
            .await
    }

    /// Publish `envelope` into the hold queue that releases it onto
    /// `envelope.queue` after `delay`.
    ///
    /// Every wait goes through here: a retry backoff, a delayed enqueue and a
    /// deferral alike. `hold` only decides which granularity the delay is
    /// rounded up to ([`RabbitMqOptions::retry_granularity`] or
    /// [`RabbitMqOptions::deferred_granularity`]) and how the publish is logged;
    /// the hold queue and its arguments are the same for both, and the priority
    /// the job returns with travels on the envelope.
    ///
    /// The rounded value names the hold queue, so equal delays share one strictly
    /// FIFO queue (see [`crate::topology`]).
    ///
    /// The hold queue is declared **immediately before every publish** and the
    /// result is never cached. That is not laziness, it is the mechanism: the
    /// queue carries `x-expires = 2 * ttl`, and an idempotent redeclare is what
    /// resets that timer. An idle hold queue otherwise deletes itself one TTL
    /// after the last publish to it. Caching "I already declared this" would let
    /// the broker delete a hold queue that is still in occasional use, and the
    /// next `mandatory` publish would come back unroutable, which, for a publish
    /// from [`Delivery::retry`](queuey_core::Delivery::retry) or
    /// [`Delivery::defer`](queuey_core::Delivery::defer), is at least a safe
    /// failure: the original is not acked.
    ///
    /// The declaration runs on the dedicated declaration channel, never on the
    /// confirm channel every other publish shares; see
    /// [`with_declare_channel`](Publisher::with_declare_channel).
    ///
    /// Two things are refused rather than approximated:
    ///
    /// * a queue this backend never declared: [`Error::UnknownQueue`], because
    ///   the hold queue's durability and dead-letter target would otherwise be
    ///   guesses, and a TTL expiry into a queue that does not exist is dropped
    ///   by the broker without a word (see [`Publisher::config_for`]);
    /// * a delay past [`MAX_DEFERRAL_MS`](topology::MAX_DEFERRAL_MS), since
    ///   clamping it would release the job early.
    ///
    /// Both leave the caller's delivery unacked, which is the correct observable:
    /// the job comes back rather than disappearing.
    ///
    /// Durability follows the main queue's, from the config the backend recorded
    /// at declare time.
    pub(crate) async fn publish_held(
        &self,
        envelope: &Envelope,
        delay: Duration,
        hold: Hold,
    ) -> Result<()> {
        let Some(config) = self.config_for(&envelope.queue) else {
            return Err(Error::UnknownQueue(envelope.queue.clone()));
        };
        let granularity = hold.granularity(&self.options);
        let Some(ttl_ms) = topology::deferred_ttl_ms(delay, granularity) else {
            return Err(RabbitMqError::DelayTooLong {
                requested: delay,
                max: Duration::from_millis(u64::from(topology::MAX_DEFERRAL_MS)),
            }
            .into_core());
        };
        let hold_queue = self.deferred_queue(&envelope.queue, ttl_ms);
        let hold_name = short_string(&hold_queue)?;

        {
            let channel = self.with_declare_channel(&hold_queue).await?;
            channel
                .queue_declare(
                    hold_name,
                    topology::declare_options(config.durable),
                    topology::deferred_queue_args(&config, ttl_ms),
                )
                .await
                .map_err(amqp)?;
            // The declare guard is dropped here, before the publish takes the
            // publishing channel's lock.
        }

        debug!(
            queue = %envelope.queue,
            hold = %hold_queue,
            ttl_ms,
            attempt = envelope.attempt,
            deferrals = envelope.deferrals,
            priority = envelope.priority,
            "{}",
            hold.log_message()
        );

        let payload = envelope.to_bytes()?;
        self.publish_confirmed(&hold_queue, &payload, codec::props_for(envelope))
            .await
    }

    /// Publish `envelope` to its dead-letter queue with the death headers.
    pub(crate) async fn publish_dead_letter(
        &self,
        envelope: &Envelope,
        reason: &str,
    ) -> Result<()> {
        let payload = envelope.to_bytes()?;
        let properties = codec::dead_letter_props(envelope, reason);
        let target = self.dead_queue(&envelope.queue);
        self.publish_confirmed(&target, &payload, properties).await
    }

    /// Publish an undecodable body verbatim to `queue`'s dead-letter queue.
    pub(crate) async fn publish_malformed(
        &self,
        queue: &str,
        payload: &[u8],
        reason: &str,
    ) -> Result<()> {
        let properties = codec::malformed_props(queue, reason);
        let target = self.dead_queue(queue);
        self.publish_confirmed(&target, payload, properties).await
    }

    /// Close both channels, whichever of them are still open.
    ///
    /// Does *not* reopen them first: closing a dead channel is
    /// already the state the caller wanted. Both are attempted even if the first
    /// fails, so a stuck publishing channel cannot leak the declaration one; the
    /// first error is what is reported.
    pub(crate) async fn close(&self) -> Result<()> {
        let publishing = {
            let channel = self.channel.lock().await;
            close_channel(&channel).await
        };
        let declaring = {
            let channel = self.declare_channel.lock().await;
            close_channel(&channel).await
        };
        publishing.and(declaring)
    }
}

/// Why a message is being put into a hold queue.
///
/// The hold queue itself does not care: its name and arguments depend only on
/// the rounded delay. What differs is the *rounding*, because a retry backoff
/// tolerates coarser steps than a `Retry-After` does, and the log line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Hold {
    /// A retry backoff or a delayed enqueue: rounded to
    /// [`RabbitMqOptions::retry_granularity`], returns at whatever priority the
    /// envelope carries (`0` for both).
    Retry,
    /// A deferral: rounded to [`RabbitMqOptions::deferred_granularity`], returns
    /// at the queue's top priority carried on the envelope.
    Deferral,
}

impl Hold {
    /// The step delays of this kind are rounded up to.
    pub(crate) fn granularity(self, options: &RabbitMqOptions) -> Duration {
        match self {
            Self::Retry => options.retry_granularity,
            Self::Deferral => options.deferred_granularity,
        }
    }

    fn log_message(self) -> &'static str {
        match self {
            Self::Retry => "holding job for a delayed redelivery",
            Self::Deferral => "deferring job",
        }
    }
}

/// Close one channel if it is still open, tolerating "already closing".
///
/// Closing two channels in a row races the connection shutdown that usually
/// follows: lapin flips every channel to `Closing` as soon as the connection
/// starts going down, and a `channel.close` issued a moment later then fails its
/// state check even though nothing is left open. Same tolerance as
/// [`RabbitMqBackend::close`](crate::RabbitMqBackend).
async fn close_channel(channel: &Channel) -> Result<()> {
    if !channel.status().connected() {
        return Ok(());
    }
    match channel.close(200, "OK".into()).await {
        Ok(()) => Ok(()),
        Err(error) if crate::backend::is_benign_close_error(&error) => {
            debug!(%error, channel = channel.id(), "channel already closing; treating close as successful");
            Ok(())
        }
        Err(error) => Err(amqp(error)),
    }
}

/// Decide whether a broker [`Confirmation`] means "durably stored".
///
/// Only a bare `Ack` does. In particular `Ack(Some(returned))` does **not**:
/// with a `mandatory` publish the broker acks the message *and* returns it when
/// nothing was bound to the routing key, so treating that as success is exactly
/// the silent-loss case this guards against: the caller would go on to ack an
/// original that was never re-published anywhere.
///
/// Split out from [`Publisher::publish_confirmed`] so the mapping is testable
/// without a broker. [`lapin::message::BasicReturnMessage`] has no public
/// constructor, so the `Some(..)` arms cannot be exercised in a unit test; they
/// are covered by the `publishing_to_a_missing_queue_is_an_error` broker test.
fn confirmation_to_result(confirmation: Confirmation, routing_key: &str) -> Result<()> {
    match confirmation {
        Confirmation::Ack(None) => Ok(()),
        Confirmation::Ack(Some(returned)) | Confirmation::Nack(Some(returned)) => {
            Err(RabbitMqError::Returned {
                reply_code: returned.reply_code,
                reply_text: returned.reply_text.to_string(),
                routing_key: routing_key.to_owned(),
            }
            .into_core())
        }
        Confirmation::Nack(None) => Err(RabbitMqError::Nacked {
            queue: routing_key.to_owned(),
        }
        .into_core()),
        Confirmation::NotRequested => Err(RabbitMqError::ConfirmsNotEnabled {
            queue: routing_key.to_owned(),
        }
        .into_core()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_hold_kind_reads_its_own_granularity() {
        let options = RabbitMqOptions::default()
            .retry_granularity(Duration::from_secs(10))
            .deferred_granularity(Duration::from_millis(250));
        assert_eq!(Hold::Retry.granularity(&options), Duration::from_secs(10));
        assert_eq!(
            Hold::Deferral.granularity(&options),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn a_plain_ack_is_success() {
        assert!(confirmation_to_result(Confirmation::Ack(None), "emails").is_ok());
    }

    #[test]
    fn a_nack_is_an_error_naming_the_queue() {
        let err = confirmation_to_result(Confirmation::Nack(None), "emails.deferred.1000")
            .expect_err("nack must not be success");
        let text = err.to_string();
        assert!(text.contains("nacked"), "{text}");
        assert!(text.contains("emails.deferred.1000"), "{text}");
    }

    #[test]
    fn not_requested_is_an_error_rather_than_a_silent_success() {
        let err = confirmation_to_result(Confirmation::NotRequested, "emails.dead")
            .expect_err("an unconfirmed publish must not be success");
        let text = err.to_string();
        assert!(
            text.contains("publisher confirms are not enabled"),
            "{text}"
        );
        assert!(text.contains("emails.dead"), "{text}");
    }

    #[test]
    fn every_confirmation_variant_is_classified() {
        // A reminder to revisit the mapping if `Confirmation` ever grows a
        // variant: the `Some(..)` arms are only reachable with a real broker.
        for confirmation in [
            Confirmation::Ack(None),
            Confirmation::Nack(None),
            Confirmation::NotRequested,
        ] {
            let expected_ok = confirmation == Confirmation::Ack(None);
            assert_eq!(
                confirmation_to_result(confirmation, "q").is_ok(),
                expected_ok
            );
        }
    }
}
