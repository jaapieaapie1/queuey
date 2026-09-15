//! The reconnecting connection every channel in this backend is opened on.
//!
//! A [`lapin::Connection`] is dead once it drops: channels opened on it fail,
//! consumers on it end, and nothing recovers by itself. [`ConnectionHandle`]
//! wraps one behind a swap, so the rest of the backend can ask for "the
//! connection" and get a live one, reconnecting first if it has to.
//!
//! Three properties are worth stating up front, because the rest of the backend
//! relies on them:
//!
//! * **Reconnection is single-flight.** Every publisher, declarer and consumer
//!   that notices the outage calls [`ConnectionHandle::ensure_connected`], and
//!   exactly one of them does the work while the others wait for it. A fleet of
//!   consumers does not become a fleet of connections.
//! * **A [`generation`](ConnectionHandle::generation) counts successful
//!   connections.** A consumer that subscribed at generation 4 can tell "the
//!   connection I was using is the one that just died" from "somebody has
//!   already replaced it", which is what keeps every consumer on a backend from
//!   forcing its own reconnect in turn.
//! * **A closing handle never reconnects.** [`mark_closing`] is final:
//!   subsequent calls fail with [`RabbitMqError::Closed`] instead of quietly
//!   dialling the broker again, so shutting a backend down actually shuts it
//!   down.
//!
//! [`mark_closing`]: ConnectionHandle::mark_closing

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex as StdMutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use lapin::{Channel, Connection};
use queuey_core::{QueueConfig, Result};
use tokio::sync::{Mutex, Notify};
use tracing::{debug, error, info, warn};

use crate::{
    error::{RabbitMqError, amqp, short_string},
    options::RabbitMqOptions,
    reconnect::{Attempt, Rebuilding, ReconnectPolicy},
    topology,
};

/// AMQP reply code for a normal, operator-initiated close.
pub(crate) const REPLY_SUCCESS: u16 = 200;

/// A connection that replaces itself when the broker goes away.
///
/// Held behind an [`Arc`] by the backend, the publisher and every consumer
/// stream; each of them opens its channels through
/// [`ensure_connected`](Self::ensure_connected).
pub(crate) struct ConnectionHandle {
    /// Kept so a replacement can be dialled: a lapin `Connection` does not
    /// remember where it came from.
    uri: String,
    options: Arc<RabbitMqOptions>,
    /// The live connection. Behind a `std` lock because it is only ever read,
    /// cloned and swapped, never held across an `await`.
    current: RwLock<Arc<Connection>>,
    /// Number of connections this handle has successfully opened, the first
    /// included. Only ever increases.
    generation: AtomicU64,
    /// Held for the duration of a reconnect, so concurrent callers queue behind
    /// one attempt instead of opening a connection each.
    reconnecting: Mutex<()>,
    /// Set once, by [`mark_closing`](Self::mark_closing). From then on nothing
    /// reconnects and every request for a connection fails.
    closing: AtomicBool,
    /// Woken by [`mark_closing`](Self::mark_closing) so a reconnect that is
    /// sleeping out a backoff gives up at once instead of after its delay.
    closing_notify: Notify,
    /// Every [`QueueConfig`] this backend has declared, by queue name.
    ///
    /// Two things read it. A retry or deferral needs its main queue's
    /// durability and priority levels in order to declare a hold queue, and a
    /// reconnect replays the whole map onto the new connection, because a
    /// broker that restarted rather than merely dropped us has lost every queue
    /// that was not durable.
    configs: StdMutex<HashMap<String, QueueConfig>>,
}

impl std::fmt::Debug for ConnectionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionHandle")
            .field("generation", &self.generation.load(Ordering::Relaxed))
            .field("closing", &self.is_closing())
            .field("connected", &self.is_connected())
            .finish_non_exhaustive()
    }
}

impl ConnectionHandle {
    /// Dial `uri` once and wrap the result.
    ///
    /// The *first* connection is deliberately not retried: a worker that cannot
    /// reach its broker at startup should say so, rather than sit in a backoff
    /// loop while whoever called `connect().await` waits. Reconnection covers
    /// the connections after this one, which nobody is awaiting.
    pub(crate) async fn connect(uri: &str, options: Arc<RabbitMqOptions>) -> Result<Arc<Self>> {
        let connection = Connection::connect(uri, options.connection_properties.clone())
            .await
            .map_err(amqp)?;

        Ok(Arc::new(Self {
            uri: uri.to_owned(),
            options,
            current: RwLock::new(Arc::new(connection)),
            generation: AtomicU64::new(1),
            reconnecting: Mutex::new(()),
            closing: AtomicBool::new(false),
            closing_notify: Notify::new(),
            configs: StdMutex::new(HashMap::new()),
        }))
    }

    /// The current connection, live or not.
    ///
    /// Use [`ensure_connected`](Self::ensure_connected) to get one that is
    /// usable; this is for inspection and for the close path.
    fn peek(&self) -> Arc<Connection> {
        Arc::clone(
            &self
                .current
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// Whether the current connection is usable right now.
    ///
    /// A `false` is not fatal: the next
    /// [`ensure_connected`](Self::ensure_connected) reconnects. It is exposed so
    /// callers can report health without forcing a reconnect.
    pub(crate) fn is_connected(&self) -> bool {
        !self.is_closing() && self.peek().status().connected()
    }

    /// Whether the handle has been told to stop reconnecting.
    pub(crate) fn is_closing(&self) -> bool {
        self.closing.load(Ordering::Acquire)
    }

    /// How many connections this handle has opened, the first included.
    ///
    /// A consumer records this when it subscribes and compares it later: an
    /// unchanged generation means the connection it was using is the one that
    /// just died, a changed one means somebody has already replaced it.
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Whether this backend is configured to reconnect at all.
    pub(crate) fn reconnects(&self) -> bool {
        self.options.reconnect.is_some()
    }

    /// The reconnection policy, or [`None`] if reconnection is disabled.
    ///
    /// Exposed so the consumer streams can pace their *resubscribe* attempts
    /// through the same policy: a live connection whose queue has gone missing
    /// would otherwise be retried in a tight loop. They pass
    /// [`Rebuilding::Consumer`], so a policy can answer differently there.
    pub(crate) fn policy(&self) -> Option<Arc<dyn ReconnectPolicy>> {
        self.options.reconnect.clone()
    }

    /// Remember what `config` was declared as, for hold queue declarations and
    /// for the replay after a reconnect. Re-declaring overwrites.
    pub(crate) fn remember(&self, config: &QueueConfig) {
        self.lock_configs()
            .insert(config.name.clone(), config.clone());
    }

    /// The config `queue` was declared with, or [`None`] if this backend never
    /// declared it.
    pub(crate) fn config_for(&self, queue: &str) -> Option<QueueConfig> {
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

    /// A live connection, reconnecting first if the current one has died.
    ///
    /// Returns the connection rather than a channel because the callers want
    /// different things on it: a confirm-mode publishing channel, a plain
    /// declaration channel, one channel per consumer.
    ///
    /// Fails without dialling anything once the handle is
    /// [`closing`](Self::mark_closing), and fails immediately rather than
    /// looping when [`RabbitMqOptions::reconnect`] is [`None`].
    pub(crate) async fn ensure_connected(&self) -> Result<Arc<Connection>> {
        if let Some(connection) = self.live()? {
            return Ok(connection);
        }

        let _guard = self.reconnecting.lock().await;
        // Whoever held this lock may have fixed the connection while we queued
        // behind them, which is the whole point of taking it.
        if let Some(connection) = self.live()? {
            return Ok(connection);
        }

        let Some(policy) = self.options.reconnect.clone() else {
            return Err(RabbitMqError::ConnectionLost.into_core());
        };

        let mut failures: u32 = 0;
        let mut last: Option<lapin::Error> = None;
        loop {
            // Asked before every attempt, the first included, so a policy
            // controls whether to start at all and how long to wait first. The
            // built-in one answers `ZERO` for the first: a failover often
            // completes in the time it took to notice, and waiting out a backoff
            // before even trying would add that delay to the common case.
            let attempt = match last.as_ref() {
                Some(error) => Attempt::after(Rebuilding::Connection, failures, error),
                None => Attempt::first(Rebuilding::Connection),
            };
            let Some(delay) = policy.next_delay(attempt) else {
                error!(
                    attempts = failures,
                    "giving up on reconnecting to rabbitmq; the policy declined another attempt"
                );
                return Err(RabbitMqError::ReconnectExhausted {
                    attempts: failures,
                    source: last.map(Box::new),
                }
                .into_core());
            };

            if !delay.is_zero() {
                debug!(
                    ?delay,
                    failures, "waiting before the next reconnect attempt"
                );
                self.sleep_unless_closing(delay).await?;
            }
            self.check_open()?;

            match Connection::connect(&self.uri, self.options.connection_properties.clone()).await {
                Ok(connection) => {
                    let connection = Arc::new(connection);
                    // The handle may have been closed while we were dialling.
                    // Installing now would leave this connection open with
                    // nobody left to close it.
                    if self.is_closing() {
                        close_quietly(&connection).await;
                        return Err(RabbitMqError::Closed.into_core());
                    }

                    let generation = self.install(Arc::clone(&connection));
                    info!(
                        generation,
                        attempts = failures + 1,
                        "reconnected to rabbitmq"
                    );
                    self.redeclare(&connection).await;
                    return Ok(connection);
                }
                Err(error) => {
                    failures += 1;
                    warn!(%error, failures, "reconnecting to rabbitmq failed");
                    last = Some(error);
                }
            }
        }
    }

    /// The current connection if it is usable, [`None`] if it needs replacing,
    /// or an error once this handle is closing.
    fn live(&self) -> Result<Option<Arc<Connection>>> {
        self.check_open()?;
        let connection = self.peek();
        Ok(connection.status().connected().then_some(connection))
    }

    /// Fail if [`mark_closing`](Self::mark_closing) has been called.
    fn check_open(&self) -> Result<()> {
        if self.is_closing() {
            return Err(RabbitMqError::Closed.into_core());
        }
        Ok(())
    }

    /// Publish `connection` as the current one and return its generation.
    fn install(&self, connection: Arc<Connection>) -> u64 {
        *self
            .current
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = connection;
        // Bumped after the swap, so anyone who reads a new generation is
        // guaranteed to read the connection that goes with it.
        self.generation.fetch_add(1, Ordering::Release) + 1
    }

    /// Sleep for `delay`, cutting it short once the handle starts closing.
    ///
    /// The waiter is armed *before* the closing flag is re-checked, and that
    /// order matters: `notify_waiters` only wakes waiters that already exist, so
    /// arming afterwards would let a close that lands in between go unnoticed
    /// and leave this sleeping out a full backoff. A caller blocked here holds
    /// the publishing lock, so that delay would be a shutdown that hangs for up
    /// to the backoff cap.
    pub(crate) async fn sleep_unless_closing(&self, delay: Duration) -> Result<()> {
        let closing = self.closing_notify.notified();
        tokio::pin!(closing);
        closing.as_mut().enable();
        self.check_open()?;

        tokio::select! {
            biased;
            () = closing => Err(RabbitMqError::Closed.into_core()),
            () = tokio::time::sleep(delay) => self.check_open(),
        }
    }

    /// Re-declare every queue this backend had declared, on the new connection.
    ///
    /// A connection that dropped because the *broker* restarted comes back
    /// without any queue that was not durable, and without the `q.dead` queues
    /// that dead-letters publish to. Replaying the declarations is what makes a
    /// reconnect useful rather than merely successful.
    ///
    /// Best-effort on purpose. A queue whose arguments an operator changed while
    /// we were away answers the redeclare with `PRECONDITION_FAILED`, and
    /// failing the reconnect over that would leave the process with no
    /// connection at all, which is strictly worse: every other queue is fine,
    /// and the operations that actually touch the changed queue report the
    /// problem themselves. So it is logged at `ERROR` and the connection kept.
    ///
    /// Runs on its own short-lived channel, since a refused declaration closes
    /// the channel it ran on.
    async fn redeclare(&self, connection: &Connection) {
        let configs: Vec<QueueConfig> = self.lock_configs().values().cloned().collect();
        if configs.is_empty() {
            return;
        }

        let channel = match connection.create_channel().await {
            Ok(channel) => channel,
            Err(error) => {
                error!(
                    %error,
                    queues = configs.len(),
                    "could not open a channel to re-declare the topology after reconnecting; \
                     queues lost to a broker restart stay missing until something declares them"
                );
                return;
            }
        };

        let mut declared = 0usize;
        for config in &configs {
            if let Err(error) = declare_topology(&channel, config, &self.options).await {
                error!(
                    queue = %config.name,
                    %error,
                    declared,
                    remaining = configs.len() - declared,
                    "re-declaring a queue after reconnecting failed; operations on it will fail \
                     until it is declared successfully"
                );
                // A refused declaration closes the channel it ran on, so the
                // rest of the replay would fail on the dead channel rather than
                // on its own merits. Stop and let the next reconnect, or an
                // explicit `declare`, try again.
                break;
            }
            declared += 1;
        }

        if declared == configs.len() {
            debug!(queues = declared, "topology re-declared after reconnect");
        }

        if channel.status().connected()
            && let Err(error) = channel.close(REPLY_SUCCESS, "OK".into()).await
        {
            debug!(%error, "closing the re-declaration channel failed");
        }
    }

    /// Stop reconnecting, from now on and for good.
    ///
    /// Separate from [`close`](Self::close) and called *first* by it, because
    /// the backend has channels to close before the connection goes down and
    /// every one of those closes is a drop that a consumer would otherwise race
    /// to repair. Idempotent.
    pub(crate) fn mark_closing(&self) {
        if !self.closing.swap(true, Ordering::AcqRel) {
            // Wake a reconnect that is sleeping out a backoff.
            self.closing_notify.notify_waiters();
        }
    }

    /// Mark the handle closing and close the connection.
    ///
    /// Idempotent: a connection that is already down is already what the caller
    /// asked for.
    pub(crate) async fn close(&self) -> Result<()> {
        self.mark_closing();

        let connection = self.peek();
        if !connection.status().connected() {
            return Ok(());
        }
        match connection.close(REPLY_SUCCESS, "OK".into()).await {
            Ok(()) => Ok(()),
            Err(error) if crate::backend::is_benign_close_error(&error) => {
                debug!(%error, "connection already closing; treating close as successful");
                Ok(())
            }
            Err(error) => Err(amqp(error)),
        }
    }
}

/// Close `connection`, reporting failure only to the log.
///
/// Used for a connection nobody ever got to see: one opened by a reconnect that
/// lost the race with a close. There is no caller left to return an error to,
/// and leaking the socket is the worse outcome.
async fn close_quietly(connection: &Connection) {
    if !connection.status().connected() {
        return;
    }
    if let Err(error) = connection.close(REPLY_SUCCESS, "OK".into()).await {
        debug!(%error, "closing a connection opened by a cancelled reconnect failed");
    }
}

/// Declare `config`'s queue and, when enabled, its dead-letter queue on `channel`.
///
/// Hold queues are deliberately *not* declared: their names depend on the delays
/// jobs actually ask for, so they are created on demand right before each held
/// publish and deleted again by the broker once idle. See [`topology`].
///
/// Shared by [`RabbitMqBackend::declare`](crate::RabbitMqBackend) and by the
/// replay a reconnect does, so what a reconnect restores cannot drift from what
/// a declare creates.
pub(crate) async fn declare_topology(
    channel: &Channel,
    config: &QueueConfig,
    options: &RabbitMqOptions,
) -> Result<()> {
    channel
        .queue_declare(
            short_string(&config.name)?,
            topology::declare_options(config.durable),
            topology::queue_args(config),
        )
        .await
        .map_err(amqp)?;

    if options.declare_dead_letter_queues {
        let dead = topology::dead_queue_name(&config.name, &options.dead_suffix);
        channel
            .queue_declare(
                // Dead-lettered jobs outlive broker restarts by design: they
                // are the record of what went wrong.
                short_string(&dead)?,
                topology::declare_options(true),
                topology::dead_queue_args(config),
            )
            .await
            .map_err(amqp)?;
    }

    Ok(())
}
