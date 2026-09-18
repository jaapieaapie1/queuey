//! Tunables for [`RabbitMqBackend`](crate::RabbitMqBackend).

use std::{sync::Arc, time::Duration};

use lapin::ConnectionProperties;

use crate::{
    reconnect::{ReconnectPolicy, default_policy},
    topology::{DEFAULT_DEAD_SUFFIX, DEFAULT_DEFERRED_SUFFIX},
};

/// Default for [`RabbitMqOptions::retry_granularity`] and
/// [`RabbitMqOptions::deferred_granularity`].
const DEFAULT_GRANULARITY: Duration = Duration::from_secs(1);

/// Default for [`RabbitMqOptions::publish_concurrency`].
///
/// One confirm-mode channel per concurrent publish, so this is both the number
/// of channels the backend may open for publishing and the number of publisher
/// confirms it may have outstanding. See the field's docs for why it is a
/// ceiling and when to move it.
const DEFAULT_PUBLISH_CONCURRENCY: usize = 8;

/// Configuration for [`RabbitMqBackend::with_options`](crate::RabbitMqBackend::with_options).
///
/// ```
/// use queuey_rabbitmq::RabbitMqOptions;
///
/// let options = RabbitMqOptions::default()
///     .dead_suffix("-dlq")
///     .declare_dead_letter_queues(false);
/// assert_eq!(options.dead_suffix, "-dlq");
/// ```
///
/// Non-exhaustive, and deliberately so: this is where a new tunable lands, and
/// the alternative is that the first bound worth exposing (a publisher-confirm
/// timeout, a dial timeout) costs a major version. Start from
/// [`RabbitMqOptions::default`] and chain the builders below; the fields stay
/// public, so reading one needs nothing extra.
///
/// # No raw `lapin` handshake properties
///
/// Earlier versions had a `connection_properties: lapin::ConnectionProperties`
/// field here. It is gone, and nothing takes its place wholesale: a `lapin` type
/// in a public signature pins this crate's entire 1.x line to one `lapin` major,
/// so a `lapin` 5.0 would have forced a `queuey` 2.0 for a field almost nobody
/// set. The one thing people actually reached for it — naming the connection so
/// an operator can tell it apart in the management UI — is
/// [`connection_name`](Self::connection_name), a plain [`String`] this crate
/// owns.
///
/// What is deliberately *not* configurable, rather than merely unimplemented:
///
/// * **Arbitrary `client_properties`.** Beyond the connection name they are
///   decoration the broker only ever shows back to you, and exposing a
///   key/value bag typed in `lapin`'s `ShortString`/`LongString` would re-create
///   the pin this removal exists to break.
/// * **The AMQP `locale`.** RabbitMQ advertises exactly one, `en_US`, which is
///   `lapin`'s default; a knob whose only valid value is the default is a
///   support question waiting to happen.
/// * **A custom executor, reactor or auth provider.** Each is a `lapin` trait
///   object, so exposing one would pin the major version outright, and this
///   backend is built for the `tokio` runtime the rest of `queuey` already
///   requires.
/// * **`lapin`'s own `enable_auto_recover`.** Reconnection is this crate's job
///   and is governed by [`reconnect`](Self::reconnect); two recovery mechanisms
///   racing on one connection is worse than either alone.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RabbitMqOptions {
    /// Name reported to the broker for this backend's connection, or [`None`]
    /// for none.
    ///
    /// Defaults to [`None`]. Set it and RabbitMQ shows it in the management UI's
    /// connection list and in `rabbitmqctl list_connections client_properties`,
    /// which is the difference between an operator reading forty rows of
    /// `10.0.3.17:52344` and reading `orders-worker (eu-west-1, v1.4.2)`. It is
    /// sent once, during the handshake, and re-sent on every reconnect, so the
    /// name survives an outage.
    ///
    /// Purely descriptive: the broker never routes, authorises or deduplicates
    /// on it, and nothing stops two processes using the same one. Include
    /// whatever *you* would want to see at 3am — the role, the region, the
    /// version, the pod.
    ///
    /// ```
    /// use queuey_rabbitmq::RabbitMqOptions;
    ///
    /// let options = RabbitMqOptions::default().connection_name("orders-worker");
    /// assert_eq!(options.connection_name.as_deref(), Some("orders-worker"));
    /// ```
    pub connection_name: Option<String>,

    /// Suffix appended to a queue name to name its dead-letter queue.
    ///
    /// Defaults to [`DEFAULT_DEAD_SUFFIX`] (`".dead"`).
    pub dead_suffix: String,

    /// Whether [`declare`](queuey_core::Backend::declare) also declares
    /// the `q.dead` queues.
    ///
    /// Defaults to `true`. Set to `false` when dead-letter queues are managed
    /// out of band (policies, an operator-owned topology, a different broker
    /// vhost).
    ///
    /// This flag also selects *how* a message is dead-lettered, because this
    /// backend never publishes to a queue it does not own:
    ///
    /// * `true`: [`Delivery::dead_letter`](queuey_core::Delivery::dead_letter)
    ///   publishes the envelope to `q.dead` with the `x-death-*` headers and
    ///   then acks the original.
    /// * `false`: nothing is published. The original is rejected with
    ///   `requeue = false`, so the broker applies whatever
    ///   `x-dead-letter-exchange` policy the operator put on `q`, and drops the
    ///   message if there is none. The reason is logged at `WARN`, since it is
    ///   not recorded anywhere else.
    ///
    /// The same choice governs a message whose body is not a valid envelope.
    pub declare_dead_letter_queues: bool,

    /// Infix between a queue name and a hold queue's TTL.
    ///
    /// Defaults to [`DEFAULT_DEFERRED_SUFFIX`] (`".deferred"`), so a 30-second
    /// wait on `myapp.emails` happens in `myapp.emails.deferred.30000`. Retries,
    /// delayed enqueues and deferrals all wait in these hold queues; see
    /// [`crate::topology`] for why the TTL is part of the name.
    pub deferred_suffix: String,

    /// Step that retry backoffs and
    /// [`Producer::enqueue_after`](queuey_core::Producer::enqueue_after) delays
    /// are rounded **up** to.
    ///
    /// Defaults to one second. Every distinct rounded delay gets its own hold
    /// queue, so this is the knob that trades backoff precision for the number
    /// of queues on the broker. It matters most for exponential backoff with
    /// jitter, which produces a different delay for every retry: with the
    /// default, a policy capped at five minutes can create at most 300 hold
    /// queues per work queue, and a granularity of ten seconds brings that down
    /// to 30. Idle hold queues delete themselves, so this bounds the number that
    /// exist at once, not a total.
    ///
    /// Separate from [`deferred_granularity`](Self::deferred_granularity) on
    /// purpose: a backoff is a heuristic that tolerates coarse rounding, a
    /// `Retry-After` is a contract that may not.
    ///
    /// A retry is never released *early*: rounding is always up, a delay
    /// shorter than the granularity still waits one full step, and a delay that
    /// rounds up past
    /// [`MAX_DEFERRAL_MS`](crate::topology::MAX_DEFERRAL_MS) (~24.8 days) is
    /// refused instead of being shortened. A zero (or sub-millisecond) value is
    /// clamped to one millisecond rather than rejected, exactly as for
    /// [`deferred_granularity`](Self::deferred_granularity).
    pub retry_granularity: Duration,

    /// Step that deferral delays are rounded **up** to.
    ///
    /// Defaults to one second. Every distinct rounded delay gets its own hold
    /// queue, so this is the knob that trades precision for the number of queues
    /// on the broker: with the default, `Retry-After: 30` and a computed `29.2s`
    /// delay share `q.deferred.30000`, and no deferral can create more than
    /// `MAX_TTL_MS / 1000` queues per work queue.
    ///
    /// A deferral is never released *early*: rounding is always up, a delay
    /// shorter than the granularity still waits one full step, and a delay that
    /// rounds up past
    /// [`MAX_DEFERRAL_MS`](crate::topology::MAX_DEFERRAL_MS) (~24.8 days) is
    /// refused instead of being shortened.
    ///
    /// A zero (or sub-millisecond) value is clamped to one millisecond by
    /// [`deferred_ttl_ms`](crate::topology::deferred_ttl_ms) rather than
    /// rejected, because a backend constructor must not panic on a config value, but
    /// one millisecond of granularity means up to one hold queue per distinct
    /// millisecond, which is almost never what you want.
    ///
    /// Note what is *not* here: nothing tunes a hold queue's `x-expires`. Its
    /// arguments are a pure function of its name (`x-expires = 2 * ttl`), so two
    /// processes configured differently still agree on `q.deferred.30000`
    /// instead of locking each other out with `PRECONDITION_FAILED`. Both
    /// granularities are safe to tune because they only change *which* hold
    /// queue a delay lands in, never that queue's arguments.
    pub deferred_granularity: Duration,

    /// How many publishes may be in flight at once, and therefore how many
    /// confirm-mode channels the backend keeps.
    ///
    /// Defaults to `8`. Every publish — an enqueue, a retry, a deferral, a
    /// dead-letter — takes one channel of a pool this size and holds it until
    /// the broker confirms, so this is the ceiling on publisher confirms
    /// outstanding at any moment. A ninth concurrent publish waits for one of
    /// the eight to finish.
    ///
    /// # Why it is a ceiling at all
    ///
    /// Because a channel with more than one publish outstanding cannot tell
    /// whose `basic.return` is whose. `lapin` queues returned messages per
    /// channel and hands one to whichever pending delivery tag resolves first,
    /// which for the `basic.ack(multiple = true)` RabbitMQ routinely sends is
    /// arbitrary. Pipelining a single channel therefore lets an *unroutable*
    /// publish (a `q.dead` an operator deleted, a hold queue the broker
    /// expired) be reported as success, after which the delivery that was
    /// waiting on it acks an original whose successor went nowhere. One publish
    /// per channel makes that impossible, and this is how the concurrency
    /// pipelining used to provide is bought back.
    ///
    /// # Picking a number
    ///
    /// Eight is chosen to cover the common shape — a worker settling a handful
    /// of jobs while a producer enqueues — without turning one backend into a
    /// channel hog. Raise it when the *confirm round trip* rather than the
    /// handler is the bottleneck, which is the case for a publish-heavy process
    /// against a distant or `fsync`-bound broker: throughput is roughly
    /// `publish_concurrency / round_trip`, so a 10 ms round trip caps eight
    /// channels at ~800 publishes a second. Lower it only to spend fewer
    /// broker-side channels; below that the setting buys nothing.
    ///
    /// `0` is clamped to `1` rather than rejected, because a builder must not
    /// panic on a configuration value and "no publishing at all" is not a thing
    /// anyone meant.
    pub publish_concurrency: usize,

    /// How a lost connection is recovered, or [`None`] to fail instead.
    ///
    /// Defaults to [`BackoffPolicy::default`](crate::BackoffPolicy): retry
    /// forever with a jittered exponential backoff. Any
    /// [`ReconnectPolicy`] implementation can go here, so pacing that a backoff
    /// curve cannot express (a circuit breaker, a schedule, a different answer
    /// for an authentication failure than for a refused connection) is a matter
    /// of writing one. A dropped connection is then invisible to job code
    /// and to [`Worker::run`](queuey_core::Worker::run), which keeps running:
    /// publishes wait for the connection to come back, and consumers
    /// resubscribe on it.
    ///
    /// Two consequences worth knowing before relying on it:
    ///
    /// * **A broker that never comes back looks like a stall, not an error.**
    ///   With the default unlimited policy nothing ever returns
    ///   `Err`; the reconnect attempts are logged at `WARN`. Set
    ///   [`BackoffPolicy::max_attempts`](crate::BackoffPolicy::max_attempts) if
    ///   the worker should exit instead.
    /// * **Jobs in flight across an outage are redelivered.** The broker
    ///   requeues everything that was unacknowledged when the connection went
    ///   down, so a job whose handler was still running is run again on the new
    ///   connection, and the settle its first run eventually attempts fails
    ///   (counted by
    ///   [`WorkerHandle::settle_failures`](queuey_core::WorkerHandle::settle_failures)).
    ///   That is the at-least-once contract this backend already has, but an
    ///   outage is when it actually bites.
    ///
    /// Set to [`None`] for the original behaviour: the connection is not
    /// rebuilt, consumer streams end, and `Worker::run` returns an error.
    pub reconnect: Option<Arc<dyn ReconnectPolicy>>,
}

impl Default for RabbitMqOptions {
    fn default() -> Self {
        Self {
            connection_name: None,
            dead_suffix: DEFAULT_DEAD_SUFFIX.to_owned(),
            declare_dead_letter_queues: true,
            deferred_suffix: DEFAULT_DEFERRED_SUFFIX.to_owned(),
            retry_granularity: DEFAULT_GRANULARITY,
            deferred_granularity: DEFAULT_GRANULARITY,
            publish_concurrency: DEFAULT_PUBLISH_CONCURRENCY,
            reconnect: Some(default_policy()),
        }
    }
}

impl RabbitMqOptions {
    /// Name this backend's connection for the broker's benefit.
    ///
    /// See [`connection_name`](Self::connection_name) for what it buys and what
    /// it does not.
    #[must_use]
    pub fn connection_name(mut self, name: impl Into<String>) -> Self {
        self.connection_name = Some(name.into());
        self
    }

    /// The handshake properties this configuration asks `lapin` for.
    ///
    /// Crate-private: it returns a `lapin` type, and keeping the translation on
    /// this side of the wall is the whole point of owning
    /// [`connection_name`](Self::connection_name) as a [`String`]. Called on
    /// every dial, the first one and every reconnect, so a named connection
    /// keeps its name across an outage.
    pub(crate) fn handshake_properties(&self) -> ConnectionProperties {
        let properties = ConnectionProperties::default();
        match &self.connection_name {
            Some(name) => properties.with_connection_name(name.as_str().into()),
            None => properties,
        }
    }

    /// Replace the dead-letter queue suffix.
    #[must_use]
    pub fn dead_suffix(mut self, suffix: impl Into<String>) -> Self {
        self.dead_suffix = suffix.into();
        self
    }

    /// Enable or disable declaring `q.dead` queues.
    #[must_use]
    pub fn declare_dead_letter_queues(mut self, declare: bool) -> Self {
        self.declare_dead_letter_queues = declare;
        self
    }

    /// Replace the hold queue infix.
    #[must_use]
    pub fn deferred_suffix(mut self, suffix: impl Into<String>) -> Self {
        self.deferred_suffix = suffix.into();
        self
    }

    /// Replace the step retry backoffs and delayed enqueues are rounded up to.
    ///
    /// A zero or sub-millisecond value is *clamped* to one millisecond when the
    /// TTL is computed, not rejected here: this is a builder, and library code
    /// does not panic on configuration.
    #[must_use]
    pub fn retry_granularity(mut self, granularity: Duration) -> Self {
        self.retry_granularity = granularity;
        self
    }

    /// Replace the step deferral delays are rounded up to.
    ///
    /// A zero or sub-millisecond value is *clamped* to one millisecond when the
    /// TTL is computed, not rejected here: this is a builder, and library code
    /// does not panic on configuration.
    #[must_use]
    pub fn deferred_granularity(mut self, granularity: Duration) -> Self {
        self.deferred_granularity = granularity;
        self
    }

    /// Replace the number of publishes that may be in flight at once.
    ///
    /// One confirm-mode channel is kept per unit, so this trades broker-side
    /// channels for publish throughput. See
    /// [`publish_concurrency`](Self::publish_concurrency) for why it is
    /// bounded at all and how to choose. `0` is clamped to `1`.
    ///
    /// ```
    /// use queuey_rabbitmq::RabbitMqOptions;
    ///
    /// let wide = RabbitMqOptions::default().publish_concurrency(32);
    /// assert_eq!(wide.publish_concurrency, 32);
    /// ```
    #[must_use]
    pub fn publish_concurrency(mut self, publishes: usize) -> Self {
        self.publish_concurrency = publishes;
        self
    }

    /// Replace the reconnection policy, or pass [`None`] to disable
    /// reconnection entirely.
    ///
    /// Takes anything that is already an [`Arc<dyn ReconnectPolicy>`]; use
    /// [`reconnect_with`](Self::reconnect_with) to pass a policy by value.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use queuey_rabbitmq::{BackoffPolicy, RabbitMqOptions, ReconnectPolicy};
    ///
    /// let policy: Arc<dyn ReconnectPolicy> =
    ///     Arc::new(BackoffPolicy::default().max_attempts(Some(5)));
    /// let bounded = RabbitMqOptions::default().reconnect(Some(policy));
    /// assert!(bounded.reconnect.is_some());
    ///
    /// // Or fail fast, as this backend did before reconnection existed.
    /// let never = RabbitMqOptions::default().reconnect(None);
    /// assert!(never.reconnect.is_none());
    /// ```
    ///
    /// [`Arc<dyn ReconnectPolicy>`]: ReconnectPolicy
    #[must_use]
    pub fn reconnect(mut self, policy: Option<Arc<dyn ReconnectPolicy>>) -> Self {
        self.reconnect = policy;
        self
    }

    /// Reconnect according to `policy`, wrapping it for you.
    ///
    /// The common case: the backend stores policies behind an [`Arc`] because
    /// every consumer and publisher consults the same one, but a caller building
    /// options should not have to say so.
    ///
    /// ```
    /// use std::time::Duration;
    /// use queuey_rabbitmq::{Attempt, BackoffPolicy, RabbitMqOptions, ReconnectPolicy};
    ///
    /// // The built-in policy, tuned.
    /// let bounded = RabbitMqOptions::default()
    ///     .reconnect_with(BackoffPolicy::default().max_attempts(Some(5)));
    ///
    /// // Or one of your own.
    /// #[derive(Debug)]
    /// struct EverySecond;
    /// impl ReconnectPolicy for EverySecond {
    ///     fn next_delay(&self, _: Attempt<'_>) -> Option<Duration> {
    ///         Some(Duration::from_secs(1))
    ///     }
    /// }
    /// let steady = RabbitMqOptions::default().reconnect_with(EverySecond);
    /// assert!(steady.reconnect.is_some());
    /// ```
    #[must_use]
    pub fn reconnect_with(mut self, policy: impl ReconnectPolicy + 'static) -> Self {
        self.reconnect = Some(Arc::new(policy));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_documented_topology() {
        let options = RabbitMqOptions::default();
        assert_eq!(options.dead_suffix, ".dead");
        assert!(options.declare_dead_letter_queues);
        assert_eq!(options.deferred_suffix, ".deferred");
        assert_eq!(options.retry_granularity, Duration::from_secs(1));
        assert_eq!(options.deferred_granularity, Duration::from_secs(1));
        assert_eq!(
            options.publish_concurrency, 8,
            "publishes pipeline eight deep by default, one confirm channel each"
        );
        assert!(
            options.reconnect.is_some(),
            "a dropped connection is recovered by default"
        );
    }

    #[test]
    fn reconnection_can_be_bounded_or_turned_off() {
        use crate::reconnect::{Attempt, BackoffPolicy, Rebuilding};

        let bounded = RabbitMqOptions::default()
            .reconnect_with(BackoffPolicy::default().max_attempts(Some(1)));
        let policy = bounded.reconnect.expect("a policy");
        assert!(
            policy
                .next_delay(Attempt::first(Rebuilding::Connection))
                .is_some(),
            "one attempt is allowed"
        );

        let never = RabbitMqOptions::default().reconnect(None);
        assert!(never.reconnect.is_none());
        // Turning it off must not disturb the rest of the configuration.
        assert_eq!(never.dead_suffix, ".dead");
        assert_eq!(never.retry_granularity, Duration::from_secs(1));
    }

    #[test]
    fn a_custom_policy_can_replace_the_built_in_one() {
        use crate::reconnect::Attempt;

        #[derive(Debug)]
        struct Never;
        impl ReconnectPolicy for Never {
            fn next_delay(&self, _: Attempt<'_>) -> Option<Duration> {
                None
            }
        }

        let options = RabbitMqOptions::default().reconnect_with(Never);
        let policy = options.reconnect.expect("a policy");
        assert_eq!(
            policy.next_delay(Attempt::first(crate::reconnect::Rebuilding::Connection)),
            None
        );
        // `Debug` survives into the options, so configuration stays printable.
        assert!(format!("{policy:?}").contains("Never"));
    }

    #[test]
    fn deferral_tunables_can_be_overridden() {
        let options = RabbitMqOptions::default()
            .deferred_suffix("-hold")
            .deferred_granularity(Duration::from_millis(250));
        assert_eq!(options.deferred_suffix, "-hold");
        assert_eq!(options.deferred_granularity, Duration::from_millis(250));
        // And they are independent of the retry / dead-letter tunables.
        assert_eq!(options.retry_granularity, Duration::from_secs(1));
        assert_eq!(options.dead_suffix, ".dead");
    }

    #[test]
    fn retry_granularity_is_independent_of_the_deferral_granularity() {
        // Coarsening backoff rounding must not touch `Retry-After` precision.
        let options = RabbitMqOptions::default().retry_granularity(Duration::from_secs(10));
        assert_eq!(options.retry_granularity, Duration::from_secs(10));
        assert_eq!(options.deferred_granularity, Duration::from_secs(1));
    }

    #[test]
    fn a_zero_granularity_is_accepted_and_clamped_later_not_panicked_on() {
        let options = RabbitMqOptions::default().deferred_granularity(Duration::ZERO);
        assert_eq!(options.deferred_granularity, Duration::ZERO);
        // The clamp lives in `deferred_ttl_ms`, so nothing here can panic.
        assert_eq!(
            crate::topology::deferred_ttl_ms(
                Duration::from_millis(7),
                options.deferred_granularity
            ),
            Some(7)
        );
    }

    #[test]
    fn suffixes_can_be_overridden() {
        let options = RabbitMqOptions::default()
            .dead_suffix("-dlq")
            .deferred_suffix("-hold");
        assert_eq!(options.dead_suffix, "-dlq");
        assert_eq!(options.deferred_suffix, "-hold");
        assert!(options.declare_dead_letter_queues);
    }

    #[test]
    fn dead_letter_declaration_can_be_disabled() {
        let options = RabbitMqOptions::default().declare_dead_letter_queues(false);
        assert!(!options.declare_dead_letter_queues);
        // Turning declaration off must not change the names.
        assert_eq!(options.dead_suffix, ".dead");
    }

    #[test]
    fn the_connection_is_anonymous_until_it_is_named() {
        // No name is the default, and then nothing is added to the handshake:
        // an unset option must not put an empty `connection_name` in front of an
        // operator reading the management UI.
        let options = RabbitMqOptions::default();
        assert_eq!(options.connection_name, None);
        assert_eq!(
            format!("{:?}", options.handshake_properties()),
            format!("{:?}", lapin::ConnectionProperties::default()),
            "an unnamed connection dials with lapin's own defaults"
        );
    }

    #[test]
    fn a_connection_name_reaches_the_handshake_properties() {
        let options = RabbitMqOptions::default().connection_name("orders-worker");
        assert_eq!(options.connection_name.as_deref(), Some("orders-worker"));

        // The point of the option is what the broker is told, not what the
        // struct holds, so assert on the translated properties. `lapin` carries
        // the name as the `connection_name` client property, which is what
        // RabbitMQ shows in the management UI; comparing against the same thing
        // built by hand pins the translation without depending on how a
        // `LongString` happens to render.
        assert_eq!(
            format!("{:?}", options.handshake_properties()),
            format!(
                "{:?}",
                ConnectionProperties::default().with_connection_name("orders-worker".into())
            ),
        );
        assert!(
            format!("{:?}", options.handshake_properties()).contains("connection_name"),
            "the name rides along as the `connection_name` client property"
        );

        // Naming the connection must not disturb anything else.
        assert_eq!(options.dead_suffix, ".dead");
        assert!(options.reconnect.is_some());
    }
}
