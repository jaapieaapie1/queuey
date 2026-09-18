//! The shared publishing channels: a small pool in confirm mode for publishes,
//! one plain channel for the on-demand hold queue declarations.

use std::{
    sync::{Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard, PoisonError},
    time::Duration,
};

use lapin::{
    BasicProperties, Channel, Confirmation, Connection,
    options::{BasicPublishOptions, ConfirmSelectOptions},
};
use queuey_core::{Envelope, Error, QueueConfig, Result};
use tokio::sync::{Mutex, MutexGuard, Semaphore, SemaphorePermit};
use tracing::{debug, warn};

use crate::{
    codec,
    connection::ConnectionHandle,
    error::{RabbitMqError, amqp, short_string},
    options::RabbitMqOptions,
    topology,
};

/// A pool of confirm-mode channels, each carrying at most one publish at a
/// time.
///
/// # Why one publish per channel
///
/// `lapin` does not key a `basic.return` to the delivery tag that caused it.
/// Returned messages go onto one queue per channel
/// (`ReturnedMessages::get_waiting_message` pops the front of a `VecDeque`) and
/// are attached to whichever pending tag the confirm handler resolves first.
/// For the `basic.ack(multiple = true)` RabbitMQ routinely sends, "first" means
/// `HashMap` iteration order over the tags the ack covers.
///
/// So with several publishes in flight on one channel, an unroutable publish's
/// return can be handed to a *different*, perfectly routable publish. That
/// publish then fails spuriously, which is safe — its caller does not ack
/// anything — but the unroutable one resolves as a bare `Confirmation::Ack` and
/// is reported as **success**. [`Delivery::dead_letter`](queuey_core::Delivery)
/// would then ack an original whose successor went nowhere: a job lost with
/// nothing logged. That is precisely what `Ok(())` from
/// [`publish_confirmed`](Publisher::publish_confirmed) promises cannot happen.
///
/// A channel with a single outstanding confirm cannot misattribute: there is
/// one pending tag, so a return can only belong to it. Concurrency is therefore
/// kept by having several such channels rather than by pipelining one, and the
/// pool size is the ceiling on publishes in flight
/// ([`RabbitMqOptions::publish_concurrency`]).
struct ConfirmChannels {
    connection: Arc<ConnectionHandle>,
    /// Channels nobody is publishing on. A `std` lock because it is only
    /// popped from and pushed to, never held across an `await`, and because a
    /// [`ChannelLease`] has to hand its channel back from `Drop`.
    idle: StdMutex<Vec<Channel>>,
    /// One permit per pooled channel. Holding a permit for the whole
    /// publish-and-confirm is what bounds both the number of channels that can
    /// exist and the number of confirms in flight.
    permits: Semaphore,
    /// The permit count, because a [`Semaphore`] cannot be asked for it and
    /// [`ConfirmChannels::close`] has to take all of them.
    size: u32,
}

/// A cheap, cloneable handle to the publishing channels.
///
/// The confirm channels are a small pool (see [`ConfirmChannels`]) shared by
/// the backend and by every in-flight
/// [`RabbitMqDelivery`](crate::RabbitMqDelivery), because a delivery must be
/// able to re-publish (retry / dead-letter) long after the call that produced
/// it returned. A publish holds one channel of the pool for its whole
/// publish-and-confirm; concurrent publishers use the other channels, and queue
/// once the pool is busy.
///
/// A channel exception (an unroutable `mandatory` publish is *not* one, but a
/// failed declaration or a broker-side error is) closes a channel for good, and
/// a dropped connection closes all of them. The connection is kept so a channel
/// can be reopened lazily on the next publish; see
/// [`ConfirmChannels::acquire`].
///
/// # Why declarations get their own channel
///
/// [`publish_held`](Publisher::publish_held) has to declare a hold queue before
/// it can publish into it, and a declaration is exactly the thing that can be
/// *refused*: a hold queue that already exists with different arguments is
/// answered with `PRECONDITION_FAILED`, which kills the channel it was issued
/// on. On a shared confirm channel that would take down every concurrent
/// publish with it (a dead-letter, an unrelated enqueue), and the declare's
/// round trip would have to be waited out while holding that channel.
///
/// So declarations run on a separate, non-confirm channel with its own mutex.
/// A refused declaration then costs exactly the one retry or deferral that
/// asked for it; the confirm pool never notices. Everything is reopened lazily
/// when the broker has closed it.
#[derive(Clone)]
pub(crate) struct Publisher {
    connection: Arc<ConnectionHandle>,
    channels: Arc<ConfirmChannels>,
    /// Channel used only for the on-demand hold queue declarations. Not in
    /// confirm mode: nothing is published on it, and a declaration is
    /// synchronous already.
    declare_channel: Arc<Mutex<Channel>>,
    options: Arc<RabbitMqOptions>,
}

impl std::fmt::Debug for Publisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Publisher").finish_non_exhaustive()
    }
}

impl ConfirmChannels {
    /// A pool of `size` channels, seeded with the one the backend opened while
    /// connecting.
    ///
    /// Seeding rather than opening `size` channels up front keeps a process
    /// that only ever enqueues from paying for concurrency it never uses: the
    /// rest are opened the first time that many publishes actually overlap.
    fn new(connection: Arc<ConnectionHandle>, seed: Channel, size: u32) -> Self {
        Self {
            connection,
            idle: StdMutex::new(vec![seed]),
            permits: Semaphore::new(size as usize),
            size,
        }
    }

    /// Lock the idle list, ignoring poisoning.
    ///
    /// It holds channel handles with no invariants to break, so a panic in
    /// another thread while it was locked is no reason to stop publishing.
    fn lock_idle(&self) -> StdMutexGuard<'_, Vec<Channel>> {
        self.idle.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Take a channel out of the pool for exactly one publish.
    ///
    /// Waits for a permit once the pool is fully busy, which is the bound this
    /// type exists to impose; see [`RabbitMqOptions::publish_concurrency`].
    ///
    /// A channel exception closes a channel permanently and a dropped
    /// connection closes all of them, so a dead channel is replaced here rather
    /// than handed out: one bad operation, or one outage, must not disable
    /// publishing for the process. When the *connection* is what died, the
    /// replacement is opened on a reconnected one, so a publish issued during
    /// an outage waits for the broker to come back instead of failing. That
    /// wait happens while holding a permit but no lock, so publishers queue
    /// behind the one single-flight reconnect in
    /// [`ConnectionHandle::ensure_connected`] rather than each demanding their
    /// own.
    async fn acquire(&self, context: &str) -> Result<ChannelLease<'_>> {
        let permit = self
            .permits
            .acquire()
            .await
            // Only reachable once `close` has taken the pool down for good, at
            // which point "the backend is closed" is exactly the right answer.
            .map_err(|_| RabbitMqError::Closed.into_core())?;

        // Bound in its own statement so the lock is released before the
        // `ensure_connected` below: an outage must not pile every publisher up
        // on the idle list.
        let pooled = self.lock_idle().pop();
        let channel = match pooled {
            Some(channel) if channel.status().connected() => channel,
            pooled => {
                if let Some(dead) = pooled {
                    warn!(
                        queue = context,
                        channel = dead.id(),
                        "publishing channel is closed; opening a replacement in confirm mode"
                    );
                }
                let connection = self.connection.ensure_connected().await?;
                Publisher::open_confirm_channel(&connection).await?
            }
        };

        Ok(ChannelLease {
            channels: self,
            channel: Some(channel),
            _permit: permit,
        })
    }

    /// Wait for every in-flight publish to finish, then close every channel.
    ///
    /// Taking all the permits is what makes this wait: a publish holds one for
    /// its whole publish-and-confirm, so `close` cannot pull a channel out from
    /// under a message that has been written but not yet confirmed. Closing the
    /// semaphore afterwards makes later publishes fail with
    /// [`RabbitMqError::Closed`] instead of quietly opening a fresh channel on
    /// a connection that is going away.
    ///
    /// Idempotent: a second call finds the semaphore closed, takes no permits,
    /// and has no channels left to close. Every channel is attempted even if
    /// one fails, so a stuck channel cannot leak the others; the first error is
    /// what is reported.
    async fn close(&self) -> Result<()> {
        let permits = self.permits.acquire_many(self.size).await.ok();
        self.permits.close();

        let channels = std::mem::take(&mut *self.lock_idle());
        let mut result = Ok(());
        for channel in &channels {
            let closed = close_channel(channel).await;
            if result.is_ok() {
                result = closed;
            }
        }
        drop(permits);
        result
    }
}

/// One pooled channel, exclusive to a single publish for as long as this lives.
///
/// Holding it across the confirmation is the whole point: a channel with one
/// outstanding confirm cannot be handed a `basic.return` that belongs to some
/// other publish. See [`ConfirmChannels`].
///
/// The channel goes back into the pool when the lease drops, dead or alive.
/// Diagnosing it here would duplicate what the next
/// [`acquire`](ConfirmChannels::acquire) does anyway, and returning it
/// unconditionally is what keeps a failed publish from shrinking the pool.
struct ChannelLease<'a> {
    channels: &'a ConfirmChannels,
    /// Always `Some` until `Drop` takes it back.
    channel: Option<Channel>,
    /// Released with the lease. This is the permit that bounds the pool, so it
    /// must outlive the confirmation, not just the publish.
    _permit: SemaphorePermit<'a>,
}

impl ChannelLease<'_> {
    fn channel(&self) -> &Channel {
        self.channel
            .as_ref()
            .expect("the channel is only taken out in Drop")
    }
}

impl Drop for ChannelLease<'_> {
    fn drop(&mut self) {
        if let Some(channel) = self.channel.take() {
            self.channels.lock_idle().push(channel);
        }
    }
}

impl Publisher {
    /// Wrap an already-opened confirm-mode `channel` plus a plain
    /// `declare_channel` for hold queue declarations.
    ///
    /// `channel` seeds the confirm pool, whose size comes from
    /// [`RabbitMqOptions::publish_concurrency`].
    pub(crate) fn new(
        connection: Arc<ConnectionHandle>,
        channel: Channel,
        declare_channel: Channel,
        options: Arc<RabbitMqOptions>,
    ) -> Self {
        let size = pool_size(&options);
        Self {
            channels: Arc::new(ConfirmChannels::new(Arc::clone(&connection), channel, size)),
            connection,
            declare_channel: Arc::new(Mutex::new(declare_channel)),
            options,
        }
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
    /// [`Error::UnknownQueue`]. The map itself lives on the
    /// [`ConnectionHandle`], which also replays it after a reconnect.
    fn config_for(&self, queue: &str) -> Option<QueueConfig> {
        self.connection.config_for(queue)
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

    /// Lock the declaration channel, reopening it first if it has died.
    ///
    /// Separate from the publishing channels on purpose: redeclaring a hold
    /// queue whose arguments differ from the existing one's is answered with
    /// `PRECONDITION_FAILED`, which closes the channel. Doing that on a confirm
    /// channel would fail whatever publish was in flight on it, and the
    /// declare's round trip would be waited out while holding it. Here it costs
    /// only the retry or deferral that asked for it, plus one reopened channel.
    async fn with_declare_channel(&self, context: &str) -> Result<MutexGuard<'_, Channel>> {
        let mut channel = self.declare_channel.lock().await;
        if !channel.status().connected() {
            warn!(
                queue = context,
                channel = channel.id(),
                "declaration channel is closed; opening a replacement"
            );
            let connection = self.connection.ensure_connected().await?;
            *channel = connection.create_channel().await.map_err(amqp)?;
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
    /// The channel is taken from the confirm pool and held for the whole
    /// publish *and* confirmation, so the broker's `basic.return` can only
    /// belong to this publish. Overlapping publishes go on other channels of
    /// the pool; the pool size is the ceiling on how many overlap at once. See
    /// [`ConfirmChannels`] for why pipelining one channel silently loses jobs.
    ///
    /// A channel that has died (a channel exception closes it permanently, and
    /// a dropped connection closes all of them) is replaced on checkout, so one
    /// bad operation does not disable publishing for the process.
    pub(crate) async fn publish_confirmed(
        &self,
        queue: &str,
        payload: &[u8],
        properties: BasicProperties,
    ) -> Result<()> {
        let routing_key = short_string(queue)?;

        let confirmation = {
            let lease = self.channels.acquire(queue).await?;
            let confirm = lease
                .channel()
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
                .map_err(amqp)?;
            // Awaited while the lease is still held. This channel has exactly
            // one publish outstanding, which is what lets a `basic.return` be
            // attributed to it and to nothing else.
            confirm.await.map_err(amqp)?
        };

        confirmation_to_result(confirmation, queue)?;
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

    /// Close every channel, whichever of them are still open.
    ///
    /// Does *not* reopen them first: closing a dead channel is already the
    /// state the caller wanted. The declaration channel is attempted even if
    /// the confirm pool failed, so a stuck publish cannot leak it; the first
    /// error is what is reported.
    pub(crate) async fn close(&self) -> Result<()> {
        let publishing = self.channels.close().await;
        let declaring = {
            let channel = self.declare_channel.lock().await;
            close_channel(&channel).await
        };
        publishing.and(declaring)
    }
}

/// How many confirm channels this configuration asks for, as a permit count.
///
/// Clamped rather than validated, like the granularities: this comes from a
/// builder, and library code does not panic on configuration. Zero would mean
/// "no publishing at all", which nobody can have meant.
fn pool_size(options: &RabbitMqOptions) -> u32 {
    u32::try_from(options.publish_concurrency)
        .unwrap_or(u32::MAX)
        .max(1)
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
    fn the_confirm_pool_never_shrinks_below_one_channel() {
        // A builder must not panic on a configuration value, and a pool of
        // zero would mean a backend that cannot publish at all.
        let options = RabbitMqOptions::default().publish_concurrency(0);
        assert_eq!(pool_size(&options), 1);
    }

    #[test]
    fn the_confirm_pool_follows_the_configured_concurrency() {
        let options = RabbitMqOptions::default().publish_concurrency(32);
        assert_eq!(pool_size(&options), 32);
    }

    #[test]
    fn an_absurd_confirm_pool_is_capped_rather_than_overflowing() {
        // `usize` is wider than the permit count a semaphore is asked for, and
        // saturating beats wrapping to something small and surprising.
        let options = RabbitMqOptions::default().publish_concurrency(usize::MAX);
        assert_eq!(pool_size(&options), u32::MAX);
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
