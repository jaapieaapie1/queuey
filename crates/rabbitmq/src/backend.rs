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
    options::{BasicAckOptions, BasicConsumeOptions, BasicQosOptions, BasicRejectOptions},
    types::{FieldTable, MAX_SHORT_STRING_LENGTH},
};
use queuey_core::{Backend, Delivery, DeliveryStream, Envelope, QueueConfig, Result};
use tracing::{debug, error, info, warn};

use crate::{
    codec,
    connection::{ConnectionHandle, REPLY_SUCCESS, declare_topology},
    delivery::RabbitMqDelivery,
    error::{RabbitMqError, amqp, short_string},
    options::RabbitMqOptions,
    publisher::{Hold, Publisher},
    reconnect::{Attempt, Rebuilding},
    topology,
};

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
/// Those are the channels the backend owns. An application whose own queues sit
/// on the same vhost as its jobs can work them without a second connection:
/// [`create_channel`](Self::create_channel) hands out a channel on this one.
/// Such a channel is the caller's to use, close and, because it does not come
/// back after a reconnect, reopen; see
/// [`connection_generation`](Self::connection_generation).
///
/// # Reconnection
///
/// The connection above is not one socket but a slot. When it drops, the first
/// operation to notice dials a replacement, the others queue behind it, every
/// queue this backend declared is re-declared on it, and the consumer streams
/// resubscribe and carry on yielding. Nothing above the backend sees an error:
/// a publish issued during the outage waits, and
/// [`Worker::run`](queuey_core::Worker::run) keeps running.
///
/// What does *not* survive is anything already in flight. The broker requeues
/// every unacknowledged delivery when a connection drops, so a job whose handler
/// was mid-run is delivered again on the new connection, and when the first run
/// finishes and tries to settle, that settle fails (the worker counts it in
/// [`WorkerHandle::settle_failures`](queuey_core::WorkerHandle::settle_failures)).
/// Handlers were already required to tolerate this — the contract is
/// at-least-once — but an outage is when it stops being theoretical.
///
/// See [`RabbitMqOptions::reconnect`] to bound the attempts or turn it off, and
/// [`topology`] for the queues this creates.
#[derive(Debug)]
pub struct RabbitMqBackend {
    connection: Arc<ConnectionHandle>,
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
    ///
    /// Only the *first* connection is made here, and it is not retried: a
    /// process that cannot reach its broker at startup should fail loudly rather
    /// than block its caller in a backoff loop. Every connection after this one
    /// is [`RabbitMqOptions::reconnect`]'s business.
    pub async fn with_options(uri: &str, options: RabbitMqOptions) -> Result<Self> {
        let options = Arc::new(options);
        let connection = ConnectionHandle::connect(uri, Arc::clone(&options)).await?;
        let live = connection.ensure_connected().await?;

        let channel = Publisher::open_confirm_channel(&live).await?;
        // A second, non-confirm channel used only for the on-demand hold queue
        // declarations: a refused declaration closes the channel it ran on, and
        // that must never be the channel every publish shares.
        let declare_channel = live.create_channel().await.map_err(amqp)?;

        info!(
            channel = channel.id(),
            declare_channel = declare_channel.id(),
            reconnects = connection.reconnects(),
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

    /// Whether the backend currently holds a live connection.
    ///
    /// A `false` does not mean the backend is broken: with reconnection on (the
    /// default) the next operation waits for a replacement. It is here for
    /// health endpoints and dashboards that want to report the gap rather than
    /// cause one, and it never itself triggers a reconnect.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.connection.is_connected()
    }

    /// Open a fresh [`Channel`] on the connection this backend is using,
    /// waiting for a reconnect first if that connection is currently down.
    ///
    /// # It reaches one vhost: this backend's
    ///
    /// A [`Connection`] is bound to the vhost in the URI it was dialled with, so
    /// this channel can only ever see queues on the backend's own vhost. Keeping
    /// application messaging on a *separate* vhost is a good default and the one
    /// this library assumes, because it walls job traffic, the dead-letter and
    /// hold queues it creates, and the permissions they need off from everything
    /// else. An application that does that still needs a connection of its own
    /// for its queues. It should open one; this method cannot reach them.
    ///
    /// What this is for is the other arrangement: job queues and application
    /// queues deliberately sharing one vhost. There a second connection to the
    /// same broker and the same vhost buys nothing but another socket, another
    /// set of credentials, another reconnect loop and another thing to remember
    /// to close, while this channel rides the connection the backend already
    /// keeps alive.
    ///
    /// # The channel is yours, and it does not outlive its connection
    ///
    /// What comes back is a plain [`lapin::Channel`] that the backend does not
    /// track, reopen or close. Closing it (or dropping it) is the caller's
    /// business, and so is its fate after an outage: unlike the backend's own
    /// channels, consumers and queues, which are rebuilt on the replacement
    /// connection, this channel dies with the connection it was opened on and
    /// nothing brings it back. A caller that means to outlive a broker restart
    /// has to notice and open a new one, which is what
    /// [`connection_generation`](Self::connection_generation) is for: remember
    /// the number this channel was opened on, and a different number later means
    /// the channel is gone and its consumers with it.
    ///
    /// # Do not collide with the library's topology
    ///
    /// Declare only queues that are yours. Re-declaring `q`, `q.dead` or
    /// `q.deferred.{ttl_ms}` (see [`topology`]) with arguments that differ by so
    /// much as one value is answered with `PRECONDITION_FAILED`, which closes
    /// the channel it ran on and, for a hold queue, keeps failing every retry
    /// and deferral until somebody deletes the queue. The backend's own queue
    /// names are available from [`dead_queue_name`](Self::dead_queue_name) and
    /// [`deferred_queue_name`](Self::deferred_queue_name) if you need to be sure
    /// you are steering clear of them.
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use futures::StreamExt;
    /// use lapin::{
    ///     options::{BasicAckOptions, BasicConsumeOptions, QueueDeclareOptions},
    ///     types::FieldTable,
    /// };
    /// use queuey_rabbitmq::RabbitMqBackend;
    ///
    /// let backend = RabbitMqBackend::connect("amqp://guest:guest@localhost:5672/%2f").await?;
    /// // ... declare the job queues and run a worker on `backend` as usual ...
    ///
    /// // An ingress queue another system publishes to, on the same connection.
    /// let opened_on = backend.connection_generation();
    /// let channel = backend.create_channel().await?;
    /// channel
    ///     .queue_declare(
    ///         "orders.ingress".into(),
    ///         QueueDeclareOptions {
    ///             durable: true,
    ///             ..Default::default()
    ///         },
    ///         FieldTable::default(),
    ///     )
    ///     .await?;
    /// let mut consumer = channel
    ///     .basic_consume(
    ///         "orders.ingress".into(),
    ///         "orders-ingress".into(),
    ///         BasicConsumeOptions::default(),
    ///         FieldTable::default(),
    ///     )
    ///     .await?;
    ///
    /// while let Some(delivery) = consumer.next().await {
    ///     let delivery = delivery?;
    ///     // ... hand the body to the application ...
    ///     delivery.acker.ack(BasicAckOptions::default()).await?;
    /// }
    ///
    /// // The consumer ended. If the connection was replaced underneath it, this
    /// // channel is gone for good; the backend recovered, this did not.
    /// if backend.connection_generation() != opened_on {
    ///     let _channel = backend.create_channel().await?;
    ///     // ... and resubscribe on it.
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn create_channel(&self) -> Result<Channel> {
        let live = self.connection.ensure_connected().await?;
        live.create_channel().await.map_err(amqp)
    }

    /// How many connections this backend has had, the first one included.
    ///
    /// It starts at `1` and goes up by one every time a dropped connection is
    /// replaced, so it is the answer to "is this still the connection I was
    /// given?", the question every user of
    /// [`create_channel`](Self::create_channel) eventually has. A channel opened
    /// there belongs to exactly one connection: the backend rebuilds *its*
    /// channels, consumers and queue declarations on the replacement, but not
    /// that one, which is closed by the broker along with everything else on the
    /// dead socket. Nothing tells its holder so, because a channel has no such
    /// signal; a stalled consumer and a number that has moved on is what it
    /// looks like.
    ///
    /// So a caller records this number when it opens a channel and compares it
    /// later, periodically or whenever its own consumer ends or a publish
    /// fails. A different number means: that channel is dead, open another with
    /// `create_channel` and redo the declarations and subscriptions that were on
    /// it. An unchanged number means the channel is as good as it ever was.
    ///
    /// This never itself triggers a reconnect, so it is safe to poll.
    #[must_use]
    pub fn connection_generation(&self) -> u64 {
        self.connection.generation()
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

    /// Declare `q` and (optionally) `q.dead` on `channel`, and remember the
    /// config.
    ///
    /// Hold queues are *not* declared here: their names depend on the delays
    /// jobs actually ask for, so they are created on demand by
    /// [`publish`](Backend::publish) with a delay, [`defer`](Backend::defer)
    /// and the delivery's `retry` / `defer`, and deleted again by the broker
    /// once idle.
    ///
    /// The declaration itself is [`declare_topology`], shared with the replay a
    /// reconnect performs, so what comes back after an outage is exactly what
    /// was created before it.
    async fn declare_one(&self, channel: &Channel, config: &QueueConfig) -> Result<()> {
        declare_topology(channel, config, &self.options).await?;

        // Only after the declarations landed: a retry or deferral onto this
        // queue now knows whether its hold queue has to be durable, and with how
        // many priority levels the returning job will be ordered. This is also
        // the record a reconnect replays.
        self.connection.remember(config);

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
        let channel = self
            .connection
            .ensure_connected()
            .await?
            .create_channel()
            .await
            .map_err(amqp)?;
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

    /// Subscribe to `queue` and stream its deliveries.
    ///
    /// The stream outlives the connection it started on: if the connection
    /// drops, it waits for a reconnect and resubscribes rather than ending, so
    /// [`Worker::run`](queuey_core::Worker::run) keeps going across a broker
    /// restart. It ends only when the backend is [`close`](Backend::close)d, or
    /// when reconnection is disabled or exhausted, which is what
    /// [`Error::ConsumerStopped`](queuey_core::Error::ConsumerStopped) is for.
    ///
    /// Deliveries the worker was still holding when the connection dropped are
    /// requeued by the broker and delivered again here; settling them on the old
    /// connection fails. See the type-level docs.
    async fn consume(&self, queue: &QueueConfig) -> Result<DeliveryStream> {
        let connection = self.connection.ensure_connected().await?;
        let generation = self.connection.generation();
        let (channel, consumer) = subscribe(&connection, queue).await?;

        let stream: DeliveryStream = Box::pin(delivery_stream(ConsumeState {
            consumer,
            // Held purely to keep the consumer's channel open for as long as
            // the stream lives, and replaced wholesale on a resubscribe.
            channel,
            connection: Arc::clone(&self.connection),
            publisher: self.publisher.clone(),
            config: queue.clone(),
            generation,
        }));
        Ok(stream)
    }

    /// Close the connection and every channel on it, for good.
    ///
    /// The handle is marked closing *first*, before a single channel goes down,
    /// so that the consumers watching for a dropped connection see a deliberate
    /// shutdown instead of an outage and end their streams rather than racing to
    /// reconnect. Nothing reopens the connection afterwards.
    async fn close(&self) -> Result<()> {
        self.connection.mark_closing();
        if let Err(error) = self.publisher.close().await {
            debug!(%error, "closing the publishing channel failed");
        }
        self.connection.close().await?;
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

/// Open a channel on `connection` and start consuming `config`'s queue on it.
///
/// One channel per consumer: the `basic_qos` prefetch window is per channel, and
/// a failure on one consumer must not take down the others. Shared by
/// [`consume`](Backend::consume) and by the resubscribe a dropped connection
/// triggers, so a recovered consumer is configured exactly like a fresh one.
async fn subscribe(connection: &Connection, config: &QueueConfig) -> Result<(Channel, Consumer)> {
    let channel = connection.create_channel().await.map_err(amqp)?;
    channel
        .basic_qos(config.prefetch, BasicQosOptions { global: false })
        .await
        .map_err(amqp)?;

    let tag = consumer_tag(&config.name);
    let consumer = channel
        .basic_consume(
            short_string(&config.name)?,
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

    info!(queue = %config.name, prefetch = config.prefetch, %tag, "consuming");
    Ok((channel, consumer))
}

/// Everything the consumer stream needs to keep alive between polls.
struct ConsumeState {
    consumer: Consumer,
    /// Kept alive so the consumer's channel outlives the call that opened it,
    /// and replaced on every resubscribe.
    channel: Channel,
    connection: Arc<ConnectionHandle>,
    publisher: Publisher,
    /// The full config, not just the name: a resubscribe needs the prefetch too.
    config: QueueConfig,
    /// Connection generation this subscription was made on, so a consumer can
    /// tell "my connection died" from "somebody already replaced it".
    generation: u64,
}

/// What a consumer should do after its subscription stopped producing.
enum Resubscribed {
    /// Back on a live connection; keep polling.
    Yes,
    /// The stream is over on purpose: the backend is closing, or reconnection is
    /// switched off.
    Stop,
}

/// Turn a lapin [`Consumer`] into a core [`DeliveryStream`].
///
/// Bodies that do not decode as an [`Envelope`] are disposed of in place (see
/// [`discard_malformed`]) and never surface as stream items: a poison message
/// must not stall or kill a worker.
///
/// A subscription that stops producing, whether it errors or simply ends, is
/// rebuilt rather than propagated (see [`recover`]), so a dropped connection is
/// a pause in this stream instead of the end of it. The stream still terminates
/// on a deliberate close, and still yields an error when reconnection is off or
/// has given up, which is what makes
/// [`Error::ConsumerStopped`](queuey_core::Error::ConsumerStopped) reachable.
fn delivery_stream(
    state: ConsumeState,
) -> impl futures::Stream<Item = Result<Box<dyn Delivery>>> + Send {
    futures::stream::unfold(state, |mut state| async move {
        loop {
            let next = match state.consumer.next().await {
                Some(Ok(delivery)) => Some(delivery),
                // lapin always follows an error with end-of-stream, so the
                // consumer is finished either way; both cases are the same
                // question, which is whether this consumer can be rebuilt.
                Some(Err(error)) => {
                    warn!(
                        queue = %state.config.name,
                        %error,
                        "consumer failed"
                    );
                    None
                }
                None => None,
            };

            let Some(delivery) = next else {
                match recover(&mut state).await {
                    Ok(Resubscribed::Yes) => continue,
                    Ok(Resubscribed::Stop) => return None,
                    Err(error) => return Some((Err(error), state)),
                }
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
                        queue = %state.config.name,
                        delivery_tag = delivery.delivery_tag,
                        bytes = delivery.data.len(),
                        %error,
                        "dropping message whose body is not a valid envelope"
                    );
                    discard_malformed(&state.publisher, &state.config.name, &delivery).await;
                }
            }
        }
    })
}

/// Put a stopped consumer back on a live connection.
///
/// Returns [`Resubscribed::Stop`] when the stream is meant to end (the backend
/// is closing, or reconnection is disabled), and an error when reconnection was
/// tried and gave up. Both end the stream; the worker distinguishes them,
/// because only one of the two is a failure.
///
/// The retry loop here is not the same one as
/// [`ConnectionHandle::ensure_connected`]'s, and it is needed as well as that
/// one: the connection can be perfectly healthy while `basic_consume` still
/// fails, most obviously when the queue itself is gone (deleted by an operator,
/// or lost to a broker restart and not restored because the redeclare after the
/// reconnect was refused). Without a delay of its own that case would spin as
/// fast as the broker can say no, so failures here are paced by the same
/// [`ReconnectPolicy`](crate::ReconnectPolicy) and counted against the same
/// attempt limit.
async fn recover(state: &mut ConsumeState) -> Result<Resubscribed> {
    if state.connection.is_closing() {
        debug!(queue = %state.config.name, "consumer stopped; the backend is closing");
        return Ok(Resubscribed::Stop);
    }
    let Some(policy) = state.connection.policy() else {
        info!(
            queue = %state.config.name,
            "consumer stopped and reconnection is disabled; ending the stream"
        );
        return Ok(Resubscribed::Stop);
    };

    // The old channel is usually dead already, but a broker-side `basic.cancel`
    // (a deleted queue) leaves it open with no consumer on it.
    close_consumer_channel(&state.channel).await;

    let mut failures: u32 = 0;
    loop {
        // `ensure_connected` enforces the same limit on the connection itself
        // and blocks here until the broker is back.
        let connection = state.connection.ensure_connected().await?;
        let generation = state.connection.generation();

        match subscribe(&connection, &state.config).await {
            Ok((channel, consumer)) => {
                info!(
                    queue = %state.config.name,
                    generation,
                    previous_generation = state.generation,
                    attempts = failures + 1,
                    "consumer resubscribed"
                );
                state.channel = channel;
                state.consumer = consumer;
                state.generation = generation;
                return Ok(Resubscribed::Yes);
            }
            Err(error) => {
                failures += 1;
                let Some(delay) =
                    policy.next_delay(Attempt::after(Rebuilding::Consumer, failures, &error))
                else {
                    error!(
                        queue = %state.config.name,
                        %error,
                        attempts = failures,
                        "giving up on resubscribing the consumer; the policy declined another \
                         attempt"
                    );
                    return Err(error);
                };
                warn!(
                    queue = %state.config.name,
                    %error,
                    failures,
                    ?delay,
                    "resubscribing the consumer failed; will try again"
                );
                state.connection.sleep_unless_closing(delay).await?;
            }
        }
    }
}

/// Close a consumer's channel before it is replaced, best-effort.
///
/// Only reachable for a channel the broker left open (a `basic.cancel` after its
/// queue was deleted); after a connection drop the channel is already gone and
/// this is a no-op. Leaking it would hold a channel per outage on a
/// long-running worker.
async fn close_consumer_channel(channel: &Channel) {
    if !channel.status().connected() {
        return;
    }
    if let Err(error) = channel.close(REPLY_SUCCESS, "OK".into()).await {
        debug!(%error, channel = channel.id(), "closing a replaced consumer channel failed");
    }
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
        let durable = topology::declare_options(true);
        assert!(durable.durable);
        assert!(!durable.auto_delete);
        assert!(!durable.exclusive);
        assert!(!durable.passive);
        assert!(!durable.nowait);

        let transient = topology::declare_options(false);
        assert!(!transient.durable);
        assert!(!transient.auto_delete);
    }
}
