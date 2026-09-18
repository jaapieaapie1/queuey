//! When and how hard this backend tries to get its connection back.
//!
//! [`ReconnectPolicy`] is the decision, [`BackoffPolicy`] the built-in answer to
//! it. Everything in this module is about *pacing*: whether to try again, and
//! how long to wait first. What actually gets rebuilt is
//! [`ConnectionHandle`](crate::connection::ConnectionHandle)'s business.

use std::{fmt::Debug, sync::Arc, time::Duration};

use queuey_core::Backoff;

/// Base delay of [`BackoffPolicy::default`]'s backoff.
const DEFAULT_BASE: Duration = Duration::from_millis(500);

/// Ceiling of [`BackoffPolicy::default`]'s backoff.
///
/// Half a minute is long enough that a broker that is down for an hour is
/// retried a hundred-odd times rather than a hundred thousand, and short enough
/// that a worker is back within half a minute of the broker returning.
const DEFAULT_MAX: Duration = Duration::from_secs(30);

/// What this backend is trying to rebuild.
///
/// Both share one policy, but they fail for different reasons and a policy is
/// allowed to treat them differently. A connection fails because the broker is
/// unreachable, and waiting is usually the only option. A resubscribe fails on a
/// *live* connection, most often because the queue is not there, and no amount
/// of waiting fixes a queue an operator deleted: that is the case worth
/// giving up on.
///
/// Non-exhaustive: the backend will learn to rebuild more than these two (a
/// channel, a publisher confirm), and a policy that already matches on this must
/// keep compiling when it does. Match with a `_` arm, or compare with `==`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Rebuilding {
    /// The AMQP connection itself.
    Connection,
    /// A consumer's subscription, on a connection that is already live.
    Consumer,
}

/// The state a [`ReconnectPolicy`] decides on.
///
/// Non-exhaustive: it gains fields as the backend learns to report more, and an
/// existing policy keeps compiling.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct Attempt<'a> {
    /// Consecutive failures so far, resetting on every success.
    ///
    /// `0` on the first attempt after a drop, when nothing has failed yet: a
    /// policy that returns [`Duration::ZERO`] for `0` retries immediately, which
    /// is what [`BackoffPolicy`] does, because a failover is often complete by
    /// the time the client notices.
    pub failures: u32,

    /// What failed last, or [`None`] when `failures` is `0`.
    ///
    /// Deliberately a plain [`std::error::Error`] rather than a concrete type.
    /// The two [`Rebuilding`] cases fail differently:
    /// [`Rebuilding::Consumer`] carries a [`queuey_core::Error`], which a policy
    /// may `downcast_ref` and match on, while [`Rebuilding::Connection`] carries
    /// the error the underlying AMQP client returned. That one's concrete type
    /// is an implementation detail and **not** part of this crate's 1.x
    /// contract, precisely so the client can be upgraded without a major
    /// version here; read it through [`Display`](std::fmt::Display) or
    /// [`source`](std::error::Error::source). Most policies only read
    /// `failures`.
    pub error: Option<&'a (dyn std::error::Error + 'static)>,

    /// Which of the two things is being rebuilt.
    pub rebuilding: Rebuilding,
}

impl<'a> Attempt<'a> {
    /// An attempt that has not failed yet.
    ///
    /// The backend builds these; they are public so you can unit-test a
    /// [`ReconnectPolicy`] of your own. This type is `#[non_exhaustive]`, so
    /// these constructors are the only way to make one.
    #[must_use]
    pub fn first(rebuilding: Rebuilding) -> Self {
        Self {
            failures: 0,
            error: None,
            rebuilding,
        }
    }

    /// An attempt after `failures` consecutive failures, the last being `error`.
    #[must_use]
    pub fn after(
        rebuilding: Rebuilding,
        failures: u32,
        error: &'a (dyn std::error::Error + 'static),
    ) -> Self {
        Self {
            failures,
            error: Some(error),
            rebuilding,
        }
    }
}

/// How a [`RabbitMqBackend`](crate::RabbitMqBackend) paces its recovery of a
/// lost connection.
///
/// One method, asked before *every* attempt including the first: return how long
/// to wait, or [`None`] to give up. Giving up surfaces as
/// [`Error::Backend`](queuey_core::Error::Backend) on whatever operation asked
/// for the connection, and ends consumer streams, so
/// [`Worker::run`](queuey_core::Worker::run) returns as it did before
/// reconnection existed.
///
/// [`BackoffPolicy`] covers the usual cases (a backoff curve and an optional
/// attempt limit) and is the default. Implement this directly when the decision
/// needs something a curve cannot express: a circuit breaker, a schedule, a
/// budget shared with the rest of the process, or a different answer for an
/// authentication failure than for a refused connection.
///
/// ```
/// use std::time::Duration;
/// use queuey_rabbitmq::{Attempt, Rebuilding, ReconnectPolicy};
///
/// /// Waits a flat second, but never retries a consumer more than twice:
/// /// a subscription that fails on a live connection is usually a deleted
/// /// queue, and waiting does not bring one back.
/// #[derive(Debug)]
/// struct Impatient;
///
/// impl ReconnectPolicy for Impatient {
///     fn next_delay(&self, attempt: Attempt<'_>) -> Option<Duration> {
///         if attempt.rebuilding == Rebuilding::Consumer && attempt.failures >= 2 {
///             return None;
///         }
///         Some(Duration::from_secs(1))
///     }
/// }
///
/// assert_eq!(
///     Impatient.next_delay(Attempt::first(Rebuilding::Connection)),
///     Some(Duration::from_secs(1)),
/// );
/// ```
///
/// Implementations are shared across tasks and consulted from several at once,
/// hence `Send + Sync`. `Debug` is required because
/// [`RabbitMqOptions`](crate::RabbitMqOptions) is `Debug`, and a policy that
/// prints as nothing would make that output a lie.
pub trait ReconnectPolicy: Send + Sync + Debug {
    /// How long to wait before making `attempt`, or [`None`] to stop trying.
    ///
    /// Called before every attempt, `attempt.failures == 0` included, so a
    /// policy controls the first try as well as the retries: returning
    /// [`Duration::ZERO`] there attempts immediately, and returning [`None`]
    /// there refuses to reconnect at all.
    ///
    /// Must not block: it is called from the task that is holding up every other
    /// publisher waiting on the connection.
    fn next_delay(&self, attempt: Attempt<'_>) -> Option<Duration>;
}

/// The built-in [`ReconnectPolicy`]: a [`Backoff`] curve plus an optional cap on
/// consecutive attempts.
///
/// A dropped connection is the normal case, not the exceptional one: brokers are
/// restarted for upgrades, failed over, and partitioned from their clients by
/// the network in between. The default therefore retries **forever**, on the
/// theory that a worker process which outlives its broker's restart is worth
/// more than one which exits and waits for a supervisor.
///
/// ```
/// use queuey_rabbitmq::BackoffPolicy;
///
/// // Give up after ten tries instead of retrying forever.
/// let policy = BackoffPolicy::default().max_attempts(Some(10));
/// assert_eq!(policy.max_attempts, Some(10));
/// ```
///
/// Set [`RabbitMqOptions::reconnect`](crate::RabbitMqOptions::reconnect) to
/// [`None`] to turn reconnection off entirely and get the original fail-fast
/// behaviour back.
///
/// Non-exhaustive: build one from [`BackoffPolicy::default`] and the
/// [`max_attempts`](Self::max_attempts) / [`backoff`](Self::backoff) builders,
/// so a knob added in 1.x (a per-[`Rebuilding`] limit, a deadline) stays a minor
/// release. The fields stay public, so reading them needs nothing extra.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BackoffPolicy {
    /// How many consecutive attempts to make before giving up, or [`None`] to
    /// keep trying indefinitely.
    ///
    /// Defaults to [`None`]. The count is of *consecutive* failures: it resets
    /// the moment a connection succeeds, so a process that reconnects once an
    /// hour for a year never exhausts a limit of three. `Some(0)` never
    /// reconnects at all, which is [`RabbitMqOptions::reconnect`]`(None)` the
    /// long way round.
    ///
    /// [`RabbitMqOptions::reconnect`]: crate::RabbitMqOptions::reconnect
    pub max_attempts: Option<u32>,

    /// Delay between attempts, as a function of how many have failed.
    ///
    /// Defaults to exponential with full jitter: 500ms base, doubling, capped at
    /// 30 seconds. Jitter matters more here than in a job backoff: every worker
    /// in a fleet loses its connection at the same instant when a broker goes
    /// down, and an unjittered backoff would have all of them knock on the door
    /// in lockstep for as long as the outage lasts.
    pub backoff: Backoff,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self {
            max_attempts: None,
            backoff: Backoff::exponential_with(DEFAULT_BASE, 2.0, DEFAULT_MAX, true),
        }
    }
}

impl BackoffPolicy {
    /// Limit the number of consecutive attempts, or [`None`] for no limit.
    #[must_use]
    pub fn max_attempts(mut self, attempts: Option<u32>) -> Self {
        self.max_attempts = attempts;
        self
    }

    /// Replace the delay schedule between attempts.
    #[must_use]
    pub fn backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }
}

impl ReconnectPolicy for BackoffPolicy {
    /// The first attempt after a drop is immediate; the rest follow the curve.
    ///
    /// A failover is often complete in the time it took the client to notice, so
    /// waiting out a backoff before even trying would add that delay to the
    /// common case. [`Backoff::delay_for`] takes the 1-based attempt that just
    /// failed, which is exactly the failure count.
    fn next_delay(&self, attempt: Attempt<'_>) -> Option<Duration> {
        if self.max_attempts.is_some_and(|max| attempt.failures >= max) {
            return None;
        }
        if attempt.failures == 0 {
            return Some(Duration::ZERO);
        }
        Some(self.backoff.delay_for(attempt.failures))
    }
}

/// The default policy, as [`RabbitMqOptions`](crate::RabbitMqOptions) stores it.
pub(crate) fn default_policy() -> Arc<dyn ReconnectPolicy> {
    Arc::new(BackoffPolicy::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The delay a policy gives for the attempt after `failures` failures.
    fn delay(policy: &dyn ReconnectPolicy, failures: u32) -> Option<Duration> {
        let error = std::io::Error::other("broker went away");
        let attempt = if failures == 0 {
            Attempt::first(Rebuilding::Connection)
        } else {
            Attempt::after(Rebuilding::Connection, failures, &error)
        };
        policy.next_delay(attempt)
    }

    #[test]
    fn the_default_retries_forever_with_a_jittered_exponential_backoff() {
        let policy = BackoffPolicy::default();
        assert_eq!(policy.max_attempts, None);
        let Backoff::Exponential {
            base,
            factor,
            max,
            jitter,
            ..
        } = policy.backoff
        else {
            panic!("the default must be exponential");
        };
        assert_eq!(base, DEFAULT_BASE);
        assert_eq!(factor, 2.0);
        assert_eq!(max, DEFAULT_MAX);
        assert!(jitter, "a fleet must not reconnect in lockstep");
    }

    #[test]
    fn the_first_attempt_after_a_drop_is_immediate() {
        assert_eq!(delay(&BackoffPolicy::default(), 0), Some(Duration::ZERO));
    }

    #[test]
    fn an_unlimited_policy_always_offers_another_attempt() {
        let policy = BackoffPolicy::default();
        for failures in [0, 1, 100, u32::MAX] {
            assert!(delay(&policy, failures).is_some(), "failures = {failures}");
        }
    }

    #[test]
    fn a_bounded_policy_stops_at_the_limit() {
        let policy = BackoffPolicy::default().max_attempts(Some(3));
        // Three attempts are made: after 0, 1 and 2 failures.
        assert!(delay(&policy, 0).is_some());
        assert!(delay(&policy, 1).is_some());
        assert!(delay(&policy, 2).is_some());
        assert_eq!(delay(&policy, 3), None, "the third failure is the last");
        assert_eq!(delay(&policy, 4), None);
    }

    #[test]
    fn a_zero_attempt_policy_never_reconnects() {
        let policy = BackoffPolicy::default().max_attempts(Some(0));
        assert_eq!(delay(&policy, 0), None);
    }

    #[test]
    fn the_delay_grows_and_is_capped() {
        // Without jitter the schedule is exact, so it can be asserted on.
        let policy = BackoffPolicy::default().backoff(Backoff::exponential_with(
            Duration::from_millis(500),
            2.0,
            Duration::from_secs(30),
            false,
        ));
        assert_eq!(delay(&policy, 1), Some(Duration::from_millis(500)));
        assert_eq!(delay(&policy, 2), Some(Duration::from_secs(1)));
        assert_eq!(delay(&policy, 3), Some(Duration::from_secs(2)));
        assert_eq!(delay(&policy, 20), Some(Duration::from_secs(30)), "capped");
    }

    #[test]
    fn a_jittered_delay_never_exceeds_the_cap() {
        let policy = BackoffPolicy::default();
        for failures in 1..40 {
            assert!(delay(&policy, failures).expect("unlimited") <= DEFAULT_MAX);
        }
    }

    /// A policy that could not be expressed by the built-in one: it reads the
    /// error and what is being rebuilt, not just the failure count.
    #[derive(Debug)]
    struct Picky;

    impl ReconnectPolicy for Picky {
        fn next_delay(&self, attempt: Attempt<'_>) -> Option<Duration> {
            // A subscription failing on a live connection is usually a queue
            // that is gone; waiting does not bring it back.
            if attempt.rebuilding == Rebuilding::Consumer && attempt.failures >= 1 {
                return None;
            }
            let fatal = attempt
                .error
                .is_some_and(|error| error.to_string().contains("ACCESS_REFUSED"));
            if fatal {
                return None;
            }
            Some(Duration::from_secs(1))
        }
    }

    #[test]
    fn a_custom_policy_can_decide_on_the_error_and_the_target() {
        let refused = std::io::Error::other("ACCESS_REFUSED - login was refused");
        assert_eq!(
            Picky.next_delay(Attempt::after(Rebuilding::Connection, 1, &refused)),
            None,
            "credentials will not fix themselves"
        );

        let flaky = std::io::Error::other("connection reset by peer");
        assert_eq!(
            Picky.next_delay(Attempt::after(Rebuilding::Connection, 9, &flaky)),
            Some(Duration::from_secs(1)),
            "a network blip is retried regardless of the count"
        );
        assert_eq!(
            Picky.next_delay(Attempt::after(Rebuilding::Consumer, 1, &flaky)),
            None,
            "but a consumer is given up on"
        );
    }

    #[test]
    fn a_policy_is_usable_behind_the_arc_the_options_store_it_in() {
        let policy: Arc<dyn ReconnectPolicy> = Arc::new(Picky);
        assert_eq!(
            policy.next_delay(Attempt::first(Rebuilding::Connection)),
            Some(Duration::from_secs(1))
        );
        // And `Debug` survives erasure, which is what keeps `RabbitMqOptions`
        // honest when it prints itself.
        assert!(format!("{policy:?}").contains("Picky"));
    }
}
