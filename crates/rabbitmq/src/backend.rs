//! The [`Backend`] implementation.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures::StreamExt;
use lapin::{
    Channel, Connection, Consumer,
    message::Delivery as LapinDelivery,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicQosOptions, BasicRejectOptions,
        QueueDeclareOptions,
    },
    types::{FieldTable, MAX_SHORT_STRING_LENGTH},
};
use queuey_core::{Backend, Delivery, DeliveryStream, Envelope, QueueConfig, Result};
use tracing::{debug, error, info, warn};

use crate::{
    codec,
    delivery::RabbitMqDelivery,
    error::{RabbitMqError, amqp, short_string},
    options::RabbitMqOptions,
    publisher::{Hold, Publisher},
    topology,
};

/// AMQP reply code for a normal, operator-initiated close.
const REPLY_SUCCESS: u16 = 200;

/// A [`Backend`] backed by a single RabbitMQ connection.
///
/// * One [`Connection`].
/// * One publishing [`Channel`] in confirm mode, shared behind a
///   [`tokio::sync::Mutex`]. Every publish (enqueue, retry, defer, dead-letter)
///   is `mandatory` and waits for the broker's confirmation. The channel is
///   reopened lazily if a channel exception closed it. Nothing is ever
///   *declared* on it.
/// * One long-lived [`Channel`] for the hold queue declarations every retry,
///   delayed publish and [`defer`](Backend::defer) makes on demand. A
///   declaration is the one thing the broker routinely refuses
///   (`PRECONDITION_FAILED` closes the channel it ran on), so it is kept away
///   from the publishes it would otherwise take down with it.
/// * One fresh [`Channel`] per [`consume`](Backend::consume) call, so each
///   consumer gets its own `basic_qos` prefetch window and a failure on one
///   consumer cannot take down the others.
/// * One short-lived channel per [`declare`](Backend::declare) call, so a
///   rejected declaration (e.g. re-declaring an existing queue with different
///   arguments, which RabbitMQ answers with `PRECONDITION_FAILED` and closes the
///   channel) cannot poison the other channels.
///
/// Reconnection is out of scope: when the connection is lost, consumer streams
/// end and subsequent operations fail.
///
/// See [`topology`] for the queues this creates.
#[derive(Debug)]
pub struct RabbitMqBackend {
    connection: Arc<Connection>,
    publisher: Publisher,
    options: Arc<RabbitMqOptions>,
}

impl RabbitMqBackend {
    /// Connect to `uri` with [`RabbitMqOptions::default`].
    ///
    /// ```no_run
    /// # async fn example() -> queuey_core::Result<()> {
    /// use queuey_rabbitmq::RabbitMqBackend;
    ///
    /// let backend = RabbitMqBackend::connect("amqp://guest:guest@localhost:5672/%2f").await?;
    /// # Ok(()) }
    /// ```
    pub async fn connect(uri: &str) -> Result<Self> {
        Self::with_options(uri, RabbitMqOptions::default()).await
    }

    /// Connect to `uri` with explicit options.
    pub async fn with_options(uri: &str, options: RabbitMqOptions) -> Result<Self> {
        let connection = Arc::new(
            Connection::connect(uri, options.connection_properties.clone())
                .await
                .map_err(amqp)?,
        );

        let channel = Publisher::open_confirm_channel(&connection).await?;
        // A second, non-confirm channel used only for the on-demand hold queue
        // declarations: a refused declaration closes the channel it ran on, and
        // that must never be the channel every publish shares.
        let declare_channel = connection.create_channel().await.map_err(amqp)?;

        let options = Arc::new(options);
        info!(
            channel = channel.id(),
            declare_channel = declare_channel.id(),
            "rabbitmq backend connected"
        );

        Ok(Self {
            publisher: Publisher::new(
                Arc::clone(&connection),
                channel,
                declare_channel,
                Arc::clone(&options),
            ),
            connection,
            options,
        })
    }

    /// The options this backend was built with.
    #[must_use]
    pub fn options(&self) -> &RabbitMqOptions {
        &self.options
    }

    /// The name of the dead-letter queue backing `queue`.
    #[must_use]
    pub fn dead_queue_name(&self, queue: &str) -> String {
        topology::dead_queue_name(queue, &self.options.dead_suffix)
    }

    /// The name of the hold queue that `ttl_ms`-long waits of `queue` happen in.
    ///
    /// There is one per distinct rounded delay, shared by retries and
    /// deferrals, and it is created on demand by whatever schedules the wait
    /// rather than by [`declare`](Backend::declare); see [`topology`].
    #[must_use]
    pub fn deferred_queue_name(&self, queue: &str, ttl_ms: u32) -> String {
        topology::deferred_queue_name(queue, &self.options.deferred_suffix, ttl_ms)
    }

    /// Declaration options for a queue that is never exclusive or auto-deleted.
    fn declare_options(durable: bool) -> QueueDeclareOptions {
        topology::declare_options(durable)
    }

    /// Declare `q` and (optionally) `q.dead` on `channel`.
    ///
    /// Hold queues are *not* declared here: their names depend on the delays
    /// jobs actually ask for, so they are created on demand by
    /// [`publish`](Backend::publish) with a delay, [`defer`](Backend::defer)
    /// and the delivery's `retry` / `defer`, and deleted again by the broker
    /// once idle.
    async fn declare_one(&self, channel: &Channel, config: &QueueConfig) -> Result<()> {
        channel
            .queue_declare(
                short_string(&config.name)?,
                Self::declare_options(config.durable),
                topology::queue_args(config),
            )
            .await
            .map_err(amqp)?;

        if self.options.declare_dead_letter_queues {
            let dead = self.dead_queue_name(&config.name);
            channel
                .queue_declare(
                    // Dead-lettered jobs outlive broker restarts by design:
                    // they are the record of what went wrong.
                    short_string(&dead)?,
                    Self::declare_options(true),
                    topology::dead_queue_args(config),
                )
                .await
                .map_err(amqp)?;
        }

        // Only after the declarations landed: a retry or deferral onto this
        // queue now knows whether its hold queue has to be durable, and with how
        // many priority levels the returning job will be ordered.
        self.publisher.remember(config);

        debug!(queue = %config.name, "topology declared");
        Ok(())
    }
}

#[async_trait]
impl Backend for RabbitMqBackend {
    async fn declare(&self, queues: &[QueueConfig]) -> Result<()> {
        if queues.is_empty() {
            return Ok(());
        }
        // Up front, before a single queue exists: a name that leaves no room for
        // its hold queues would otherwise declare fine and then fail one retry
        // at a time, in production, on the day someone picks a long delay.
        // Nothing is created when this fails.
        for config in queues {
            check_deferrable_name(&config.name, &self.options.deferred_suffix)?;
        }
        let channel = self.connection.create_channel().await.map_err(amqp)?;
        let result: Result<()> = async {
            for config in queues {
                self.declare_one(&channel, config).await?;
            }
            Ok(())
        }
        .await;

        // Best-effort cleanup: the declaration result is what matters.
        if channel.status().connected()
            && let Err(error) = channel.close(REPLY_SUCCESS, "OK".into()).await
        {
            debug!(%error, "closing the declaration channel failed");
        }
        result
    }

    /// Publish `envelope` to its queue, or with `delay` into the hold queue
    /// that releases it onto its queue afterwards.
    ///
    /// A delayed publish is held exactly like a retry: the delay is rounded up
    /// to [`retry_granularity`](RabbitMqOptions::retry_granularity), the job
    /// returns at the priority the envelope carries (`0` for a fresh envelope,
    /// so it joins the back of the queue), and the same two rules as for
    /// [`defer`](Backend::defer) apply: the queue must have been declared
    /// through this backend, and the delay must not exceed
    /// [`MAX_DEFERRAL_MS`](topology::MAX_DEFERRAL_MS). An undelayed publish
    /// has neither restriction.
    async fn publish(&self, envelope: &Envelope, delay: Option<Duration>) -> Result<()> {
        match delay {
            None => self.publisher.publish_envelope(envelope).await,
            Some(delay) => {
                self.publisher
                    .publish_held(envelope, delay, Hold::Retry)
                    .await
            }
        }
    }

    /// Hold `envelope` for `delay`, then put it back on its own queue.
    ///
    /// # The queue must have been declared through this backend
    ///
    /// Deferring onto a queue this backend instance never
    /// [`declare`](Backend::declare)d is [`Error::UnknownQueue`], not a
    /// best-effort publish. A hold queue has to know its main queue's durability
    /// and dead-letter it back by name, and neither can be guessed: a transient
    /// hold queue in front of a durable queue loses jobs on a restart, and a TTL
    /// expiry into a queue that does not exist is dropped by the broker in
    /// silence. Unlike a `mandatory` publish, nothing is returned and nothing
    /// is reported.
    ///
    /// `Producer::new` and `WorkerBuilder::build` declare the whole queue set,
    /// so anything built through them can defer. `Producer::new_undeclared`
    /// deliberately does not, so a producer built that way can enqueue but
    /// cannot defer, or enqueue with a delay, until something in the process
    /// declares the queue.
    ///
    /// # The delay has a ceiling
    ///
    /// Delays are rounded up to
    /// [`deferred_granularity`](RabbitMqOptions::deferred_granularity) and
    /// capped at [`MAX_DEFERRAL_MS`](topology::MAX_DEFERRAL_MS) (~24.8 days);
    /// a longer one is an error rather than a shorter wait.
    ///
    /// [`Error::UnknownQueue`]: queuey_core::Error::UnknownQueue
    async fn defer(&self, envelope: &Envelope, delay: Duration) -> Result<()> {
        self.publisher
            .publish_held(envelope, delay, Hold::Deferral)
            .await
    }

    async fn consume(&self, queue: &QueueConfig) -> Result<DeliveryStream> {
        let channel = self.connection.create_channel().await.map_err(amqp)?;
        channel
            .basic_qos(queue.prefetch, BasicQosOptions { global: false })
            .await
            .map_err(amqp)?;

        let tag = consumer_tag(&queue.name);
        let consumer = channel
            .basic_consume(
                short_string(&queue.name)?,
                short_string(&tag)?,
                BasicConsumeOptions {
                    no_local: false,
                    no_ack: false,
                    exclusive: false,
                    nowait: false,
                },
                FieldTable::default(),
            )
            .await
            .map_err(amqp)?;

        info!(queue = %queue.name, prefetch = queue.prefetch, %tag, "consuming");

        let stream: DeliveryStream = Box::pin(delivery_stream(ConsumeState {
            consumer,
            // Held purely to keep the consumer's channel open for as long as
            // the stream lives.
            _channel: channel,
            publisher: self.publisher.clone(),
            queue: queue.name.clone(),
        }));
        Ok(stream)
    }

    async fn close(&self) -> Result<()> {
        if let Err(error) = self.publisher.close().await {
            debug!(%error, "closing the publishing channel failed");
        }
        if self.connection.status().connected() {
            match self.connection.close(REPLY_SUCCESS, "OK".into()).await {
                Ok(()) => {}
                Err(error) if is_benign_close_error(&error) => {
                    debug!(%error, "connection already closing; treating close as successful");
                }
                Err(error) => return Err(amqp(error)),
            }
        }
        info!("rabbitmq backend closed");
        Ok(())
    }
}

/// Refuse a queue name whose *longest* hold queue name would not fit in an AMQP
/// short string.
///
/// A queue name is valid at up to 255 bytes, but a hold queue appends the suffix
/// and up to ten digits of TTL, so a perfectly legal `q` can have illegal hold
/// queues. Checking the worst case,
/// [`MAX_DEFERRAL_MS`](topology::MAX_DEFERRAL_MS), the longest TTL this backend
/// will ever produce, means the answer does not depend on which delays happen
/// to be used, so a name that passes `declare` can always be deferred on.
fn check_deferrable_name(queue: &str, deferred_suffix: &str) -> Result<()> {
    let hold = topology::deferred_queue_name(queue, deferred_suffix, topology::MAX_DEFERRAL_MS);
    if hold.len() <= MAX_SHORT_STRING_LENGTH {
        return Ok(());
    }
    Err(RabbitMqError::DeferredNameTooLong {
        queue: queue.to_owned(),
        length: hold.len(),
        hold,
        limit: MAX_SHORT_STRING_LENGTH,
    }
    .into_core())
}

/// Whether an error from `Connection::close` only says "already closing".
///
/// lapin flips every channel to `Closing` as soon as a connection close starts.
/// A consumer channel dropped just before (a finished [`DeliveryStream`]) still
/// has its own deferred `channel.close` queued; that command then fails the
/// channel state check and lapin reports it through the connection-close
/// promise, even though the connection does shut down normally. Nothing is
/// lost and nothing is left open, so this is not an error worth surfacing.
pub(crate) fn is_benign_close_error(error: &lapin::Error) -> bool {
    use lapin::{ChannelState, ConnectionState, ErrorKind};
    matches!(
        error.kind(),
        ErrorKind::InvalidChannelState(ChannelState::Closing | ChannelState::Closed, _)
            | ErrorKind::InvalidConnectionState(ConnectionState::Closing | ConnectionState::Closed)
    )
}

/// Everything the consumer stream needs to keep alive between polls.
struct ConsumeState {
    consumer: Consumer,
    _channel: Channel,
    publisher: Publisher,
    queue: String,
}

/// Turn a lapin [`Consumer`] into a core [`DeliveryStream`].
///
/// Bodies that do not decode as an [`Envelope`] are disposed of in place (see
/// [`discard_malformed`]) and never surface as stream items: a poison message
/// must not stall or kill a worker.
fn delivery_stream(
    state: ConsumeState,
) -> impl futures::Stream<Item = Result<Box<dyn Delivery>>> + Send {
    futures::stream::unfold(state, |mut state| async move {
        loop {
            let next = state.consumer.next().await?;
            let delivery = match next {
                Ok(delivery) => delivery,
                // lapin always follows an error with end-of-stream, so the
                // consumer terminates on the next poll.
                Err(error) => return Some((Err(amqp(error)), state)),
            };

            match Envelope::from_bytes(&delivery.data) {
                Ok(envelope) => {
                    let boxed: Box<dyn Delivery> = Box::new(RabbitMqDelivery::new(
                        envelope,
                        delivery.acker.clone(),
                        state.publisher.clone(),
                    ));
                    return Some((Ok(boxed), state));
                }
                Err(error) => {
                    warn!(
                        queue = %state.queue,
                        delivery_tag = delivery.delivery_tag,
                        bytes = delivery.data.len(),
                        %error,
                        "dropping message whose body is not a valid envelope"
                    );
                    discard_malformed(&state.publisher, &state.queue, &delivery).await;
                }
            }
        }
    })
}

/// Get an undecodable message off the queue without failing the stream.
///
/// The message is always settled if the broker will let us settle it, in this
/// order:
///
/// 1. Copy the raw bytes to `q.dead` with an `x-death-reason` header and ack the
///    original, so the payload survives for inspection. Skipped when
///    [`RabbitMqOptions::declare_dead_letter_queues`] is off, because this
///    backend then does not own `q.dead` and the `mandatory` publish would only
///    come back unroutable.
/// 2. `basic_reject(requeue = false)`, which discards the message (or hands it
///    to the queue's own dead-letter exchange) rather than letting it be
///    redelivered forever. This also runs when step 1 failed *after* the publish
///    landed but the ack did not.
/// 3. If even the reject fails, log at `ERROR` and move on. The message then
///    stays unacknowledged and keeps one prefetch slot until the consumer
///    channel closes, at which point the broker requeues it. Nothing better is
///    available: settling it needs the very channel that just refused.
async fn discard_malformed(publisher: &Publisher, queue: &str, delivery: &LapinDelivery) {
    if publisher.options().declare_dead_letter_queues {
        match publisher
            .publish_malformed(queue, &delivery.data, codec::REASON_MALFORMED)
            .await
        {
            Ok(()) => match delivery.acker.ack(BasicAckOptions::default()).await {
                Ok(true) => return,
                Ok(false) => {
                    warn!(
                        queue,
                        delivery_tag = delivery.delivery_tag,
                        "a malformed message was already settled; nothing left to ack"
                    );
                    return;
                }
                Err(error) => {
                    warn!(%error, queue, delivery_tag = delivery.delivery_tag,
                        "acking a malformed message failed; falling back to reject");
                }
            },
            Err(error) => {
                warn!(%error, queue, "forwarding a malformed message to the dead-letter queue failed");
            }
        }
    }

    match delivery
        .acker
        .reject(BasicRejectOptions { requeue: false })
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            warn!(
                queue,
                delivery_tag = delivery.delivery_tag,
                "a malformed message was already settled; nothing left to reject"
            );
        }
        Err(error) => {
            error!(
                %error, queue, delivery_tag = delivery.delivery_tag,
                "rejecting a malformed message failed; it stays unacknowledged and holds a \
                 prefetch slot until the consumer channel closes"
            );
        }
    }
}

/// A consumer tag that is unique within this process and short enough for AMQP.
///
/// RabbitMQ only requires uniqueness per channel, and this backend opens a fresh
/// channel per consumer, but a readable, globally distinct tag makes the
/// management UI far easier to reason about.
fn consumer_tag(queue: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or_default();
    // Leave ample room for the fixed parts inside the 255-byte AMQP limit.
    let queue = codec::truncate_at_boundary(queue, 160);
    format!("queuey.{queue}.{nanos:x}.{sequence}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closing_state_errors_are_benign_on_close() {
        use lapin::{ChannelState, ConnectionState, ErrorKind};
        for kind in [
            ErrorKind::InvalidChannelState(ChannelState::Closing, "channel.close"),
            ErrorKind::InvalidChannelState(ChannelState::Closed, "channel.close"),
            ErrorKind::InvalidConnectionState(ConnectionState::Closing),
            ErrorKind::InvalidConnectionState(ConnectionState::Closed),
        ] {
            assert!(is_benign_close_error(&lapin::Error::from(kind)));
        }
    }

    #[test]
    fn other_state_errors_are_not_benign_on_close() {
        use lapin::{ChannelState, ConnectionState, ErrorKind};
        for kind in [
            ErrorKind::InvalidChannelState(ChannelState::Initial, "channel.close"),
            ErrorKind::InvalidChannelState(ChannelState::Error, "channel.close"),
            ErrorKind::InvalidConnectionState(ConnectionState::Error),
            ErrorKind::InvalidChannel(7),
        ] {
            assert!(!is_benign_close_error(&lapin::Error::from(kind)));
        }
    }

    #[test]
    fn consumer_tags_are_unique_and_name_the_queue() {
        let first = consumer_tag("myapp.emails");
        let second = consumer_tag("myapp.emails");
        assert_ne!(first, second);
        assert!(first.starts_with("queuey.myapp.emails."));
        assert!(second.starts_with("queuey.myapp.emails."));
    }

    #[test]
    fn consumer_tags_fit_in_a_short_string() {
        let tag = consumer_tag(&"q".repeat(1000));
        assert!(
            tag.len() <= MAX_SHORT_STRING_LENGTH,
            "len was {}",
            tag.len()
        );
        assert!(short_string(&tag).is_ok());
    }

    #[test]
    fn a_queue_name_with_room_for_its_hold_queues_is_accepted() {
        assert!(check_deferrable_name("myapp.emails", topology::DEFAULT_DEFERRED_SUFFIX).is_ok());
        // The longest name that still fits: 255 - len(".deferred.") - 10 digits.
        let longest = "q".repeat(MAX_SHORT_STRING_LENGTH - ".deferred.".len() - 10);
        assert_eq!(longest.len(), 235);
        assert!(check_deferrable_name(&longest, topology::DEFAULT_DEFERRED_SUFFIX).is_ok());
    }

    #[test]
    fn a_queue_name_that_leaves_no_room_for_hold_queues_is_refused_at_declare() {
        // 250 bytes is a perfectly legal queue name (`short_string` takes it),
        // but `{q}.deferred.2147483647` is 270 bytes, so every deferral on it
        // would fail. Better to say so once, at declare time.
        let name = "q".repeat(250);
        assert!(
            short_string(&name).is_ok(),
            "the queue name itself is legal"
        );

        let error = check_deferrable_name(&name, topology::DEFAULT_DEFERRED_SUFFIX)
            .expect_err("a name with no room for hold queues must be refused");
        let text = error.to_string();
        assert!(
            text.contains("leaves no room for its hold queues"),
            "{text}"
        );
        assert!(text.contains("270 bytes"), "{text}");
        assert!(text.contains("255-byte"), "{text}");
    }

    #[test]
    fn the_hold_queue_name_check_uses_the_configured_suffix() {
        let name = "q".repeat(240);
        // 240 + 10 (".deferred.") + 10 digits = 260: refused.
        assert!(check_deferrable_name(&name, topology::DEFAULT_DEFERRED_SUFFIX).is_err());
        // 240 + 2 ("-h") + 1 (".") + 10 digits = 253: accepted.
        assert!(check_deferrable_name(&name, "-h").is_ok());
    }

    #[test]
    fn declare_options_never_auto_delete() {
        let durable = RabbitMqBackend::declare_options(true);
        assert!(durable.durable);
        assert!(!durable.auto_delete);
        assert!(!durable.exclusive);
        assert!(!durable.passive);
        assert!(!durable.nowait);

        let transient = RabbitMqBackend::declare_options(false);
        assert!(!transient.durable);
        assert!(!transient.auto_delete);
    }
}
