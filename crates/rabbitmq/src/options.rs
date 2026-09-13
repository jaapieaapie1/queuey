//! Tunables for [`RabbitMqBackend`](crate::RabbitMqBackend).

use std::time::Duration;

use lapin::ConnectionProperties;

use crate::topology::{DEFAULT_DEAD_SUFFIX, DEFAULT_DEFERRED_SUFFIX};

/// Default for [`RabbitMqOptions::retry_granularity`] and
/// [`RabbitMqOptions::deferred_granularity`].
const DEFAULT_GRANULARITY: Duration = Duration::from_secs(1);

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
#[derive(Clone, Debug)]
pub struct RabbitMqOptions {
    /// Handshake properties passed to `lapin::Connection::connect`.
    pub connection_properties: ConnectionProperties,

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
}

impl Default for RabbitMqOptions {
    fn default() -> Self {
        Self {
            connection_properties: ConnectionProperties::default(),
            dead_suffix: DEFAULT_DEAD_SUFFIX.to_owned(),
            declare_dead_letter_queues: true,
            deferred_suffix: DEFAULT_DEFERRED_SUFFIX.to_owned(),
            retry_granularity: DEFAULT_GRANULARITY,
            deferred_granularity: DEFAULT_GRANULARITY,
        }
    }
}

impl RabbitMqOptions {
    /// Replace the connection handshake properties.
    #[must_use]
    pub fn connection_properties(mut self, properties: ConnectionProperties) -> Self {
        self.connection_properties = properties;
        self
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
    fn connection_properties_can_be_replaced() {
        let options = RabbitMqOptions::default()
            .connection_properties(ConnectionProperties::default().with_locale("nl_NL".into()));
        assert!(format!("{:?}", options.connection_properties).contains("nl_NL"));
    }
}
