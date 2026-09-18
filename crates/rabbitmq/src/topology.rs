//! Pure helpers describing the RabbitMQ topology used by this backend.
//!
//! For a logical queue `q` the backend maintains two long-lived broker queues
//! plus one short-lived *hold* queue per distinct delay:
//!
//! | queue | role | arguments |
//! |---|---|---|
//! | `q` | main work queue | `x-message-ttl` when [`QueueConfig::message_ttl`] is set, `x-max-priority` when [`QueueConfig::max_priority`] is `Some` |
//! | `q.dead` | dead-letter queue | none |
//! | `q.deferred.{ttl_ms}` | hold queue for one delay | `x-message-ttl = ttl_ms`, `x-dead-letter-exchange = ""`, `x-dead-letter-routing-key = q`, `x-expires = 2 * ttl_ms` |
//!
//! Every wait, whether a retry backoff, a delayed enqueue or a deferral, goes
//! through a hold queue. There is no shared wait queue with per-message
//! expirations, and this is why:
//!
//! # Why hold queues instead of per-message `expiration`
//!
//! A classic queue only ever expires the message at its *head*. Messages behind
//! it are not examined until it has gone, so in a shared wait queue a message
//! with a five-minute expiration at the head holds back every one-second
//! expiration behind it (head-of-line blocking). Exponential backoff produces
//! exactly that mix, so retries were the worst-hit case.
//!
//! A hold queue never sets a per-message `expiration`. Instead the delay is
//! baked into the *name* of the queue the message waits in (`q.deferred.30000`
//! holds every 30-second wait for `q`), and the wait is the queue-wide
//! `x-message-ttl`. Every message in one hold queue therefore has the same TTL,
//! so they expire in exactly the order they were published and the head is
//! always the message that is due next. A short wait can never be stuck behind a
//! long one, because the two live in different queues. The price is one queue
//! per distinct delay, which is why delays are rounded up to a granularity:
//! [`RabbitMqOptions::retry_granularity`](crate::RabbitMqOptions::retry_granularity)
//! for retries and delayed enqueues,
//! [`RabbitMqOptions::deferred_granularity`](crate::RabbitMqOptions::deferred_granularity)
//! for deferrals (both default to one second), so `29.2s` and `30s` share
//! `q.deferred.30000`.
//!
//! Retries and deferrals share the hold queues: the arguments depend only on the
//! delay, and what differs between the two, the message priority, travels on the
//! message and only matters once it is back on `q`. A retry returns at priority
//! `0` and joins the back of the queue; a deferral returns at the queue's top
//! priority and overtakes the backlog.
//!
//! # Why a hold queue's arguments depend only on its name
//!
//! The TTL is in the name, and every other argument is derived from it or from
//! the main queue, so two processes running different builds of an application
//! compute the *same* arguments for `q.deferred.30000`. That matters because
//! RabbitMQ refuses a declaration whose arguments differ from the existing
//! queue's (`PRECONDITION_FAILED`, which closes the declaring channel): a
//! tunable in `x-expires` would deadlock the two processes against each other
//! for ever, one of them unable to schedule a single wait. Hence `x-expires = 2 * ttl_ms`
//! and nothing else: an idle hold queue deletes itself one TTL after the last
//! publish to it, and every declare resets that timer, which is why the
//! queue is redeclared before every publish.
//!
//! `x-expires` must also be strictly greater than `x-message-ttl`, or the broker
//! could delete a queue that still owes a message. `2 * ttl_ms` satisfies that
//! for every TTL up to [`MAX_DEFERRAL_MS`], which is why a longer delay is
//! refused rather than silently clamped.
//!
//! Everything in this module is pure: it never touches a connection, so it can
//! be unit-tested without a broker.
//!
//! # What is public here, and why only that
//!
//! The queue *names* ([`dead_queue_name`], [`deferred_queue_name`],
//! [`deferred_ttl_ms`]) and the argument/header key constants are public: they
//! are wire format an operator already sees in the management UI, and tooling
//! that monitors `q.dead` depth or cleans up stale hold queues needs to compute
//! them exactly as this backend does.
//!
//! The functions that build the declaration *arguments* are crate-private,
//! because they return `lapin` `FieldTable`s. Exporting a `lapin` type in a
//! signature would pin this crate's 1.x line to one `lapin` major version, and
//! the arguments are not a knob in any case: they are a pure function of the
//! queue name (see above), so there is nothing downstream could usefully do
//! with them that would not risk a `PRECONDITION_FAILED`.

use std::time::Duration;

use lapin::{
    options::QueueDeclareOptions,
    types::{AMQPValue, FieldTable, LongString},
};
use queuey_core::QueueConfig;

/// Default suffix appended to a queue name to build its dead-letter queue.
pub const DEFAULT_DEAD_SUFFIX: &str = ".dead";

/// Default infix between a queue name and a hold queue's TTL.
///
/// A hold queue is named `{q}{suffix}.{ttl_ms}`, so with the default a 30-second
/// wait (retry or deferral) of `myapp.emails` happens in
/// `myapp.emails.deferred.30000`. The name says "deferred" because deferrals
/// were the first user; retries and delayed enqueues share the very same queues.
pub const DEFAULT_DEFERRED_SUFFIX: &str = ".deferred";

/// Header carrying the dead-lettering reason on messages routed to `q.dead`.
pub const HEADER_DEATH_REASON: &str = "x-death-reason";

/// Header carrying the originating queue name on messages routed to `q.dead`.
pub const HEADER_ORIGINAL_QUEUE: &str = "x-original-queue";

/// Header carrying the number of attempts made before dead-lettering.
pub const HEADER_ATTEMPTS: &str = "x-attempts";

/// Header mirroring [`queuey_core::Envelope::attempt`] on every publish.
pub const HEADER_ATTEMPT: &str = "x-attempt";

/// Header mirroring [`queuey_core::Envelope::deferrals`] on every publish.
///
/// Purely informational, like [`HEADER_ATTEMPT`]: the body is the source of
/// truth. In particular the `x-death` header RabbitMQ stamps on a message it
/// expired out of a hold queue is ignored.
pub const HEADER_DEFERRALS: &str = "x-deferrals";

/// Queue argument naming the exchange used for dead-lettering.
pub const ARG_DEAD_LETTER_EXCHANGE: &str = "x-dead-letter-exchange";

/// Queue argument naming the routing key used for dead-lettering.
pub const ARG_DEAD_LETTER_ROUTING_KEY: &str = "x-dead-letter-routing-key";

/// Queue argument setting a queue-wide message time-to-live in milliseconds.
pub const ARG_MESSAGE_TTL: &str = "x-message-ttl";

/// Queue argument declaring how many priority levels a queue supports.
///
/// Set on the main queue `q` from [`QueueConfig::max_priority`]. Never set on
/// `q.dead` or a hold queue: those are strictly FIFO holding pens and a priority
/// queue costs the broker an index per level.
pub const ARG_MAX_PRIORITY: &str = "x-max-priority";

/// Queue argument making the broker delete a queue after it has been unused for
/// that many milliseconds.
///
/// Only used for hold queues, so a delay that is never used again does not leave
/// an empty queue behind for ever.
pub const ARG_EXPIRES: &str = "x-expires";

/// Largest time-to-live RabbitMQ accepts, in milliseconds (~49.7 days).
///
/// `x-message-ttl` and `x-expires` are parsed as unsigned 32-bit millisecond
/// counts. A larger value is refused with `PRECONDITION_FAILED`, which closes
/// the channel, so this crate clamps a queue TTL rather than letting a huge
/// [`Duration`] take the channel down.
pub const MAX_TTL_MS: u32 = u32::MAX;

/// Longest delay this backend will hold, in milliseconds (~24.8 days).
///
/// Applies to every wait that goes through a hold queue: retry backoffs, delayed
/// enqueues and deferrals alike.
///
/// A hold queue is declared with `x-expires = 2 * x-message-ttl` (see the module
/// docs for why the factor is fixed rather than configurable), and `x-expires`
/// has the same 32-bit millisecond domain as `x-message-ttl`. Half of
/// [`MAX_TTL_MS`] is therefore the largest TTL for which the doubled expiry
/// still fits and still stays strictly above the TTL, so the broker can never
/// delete a hold queue that still owes a message.
///
/// A longer delay is **refused**, not clamped: clamping would release the job
/// early, and "never early" is the one timing guarantee a hold makes. See
/// [`deferred_ttl_ms`].
pub const MAX_DEFERRAL_MS: u32 = MAX_TTL_MS / 2;

/// Name of the dead-letter queue for `queue`.
///
/// ```
/// use queuey_rabbitmq::topology::{dead_queue_name, DEFAULT_DEAD_SUFFIX};
/// assert_eq!(dead_queue_name("myapp.emails", DEFAULT_DEAD_SUFFIX), "myapp.emails.dead");
/// ```
#[must_use]
pub fn dead_queue_name(queue: &str, suffix: &str) -> String {
    format!("{queue}{suffix}")
}

/// Name of the hold queue holding `ttl_ms`-long waits of `queue`.
///
/// The TTL is part of the name on purpose: one queue per distinct delay is what
/// makes a hold queue drain strictly in order (see the module docs). Retries and
/// deferrals with the same rounded delay share the queue.
///
/// ```
/// use queuey_rabbitmq::topology::{deferred_queue_name, DEFAULT_DEFERRED_SUFFIX};
/// assert_eq!(
///     deferred_queue_name("myapp.emails", DEFAULT_DEFERRED_SUFFIX, 30_000),
///     "myapp.emails.deferred.30000"
/// );
/// ```
#[must_use]
pub fn deferred_queue_name(queue: &str, suffix: &str, ttl_ms: u32) -> String {
    format!("{queue}{suffix}.{ttl_ms}")
}

/// The hold queue TTL for `delay`: `delay` rounded **up** to a whole multiple of
/// `granularity`, at least one whole step, or [`None`] when that lands past
/// [`MAX_DEFERRAL_MS`].
///
/// Rounding up is what bounds the number of hold queues: with the default
/// one-second granularity every delay between `29.001s` and `30s` maps to
/// `30000`, so a burst of `Retry-After: 30` responses shares one queue, and a
/// jittered exponential backoff creates at most one queue per whole second of
/// its range. Rounding *up* rather than to nearest guarantees the job never
/// comes back early, and [`None`] rather than a clamp at the top end is the same
/// guarantee: a delay this backend cannot hold is refused, never shortened. The
/// rounding itself can push a delay over the cap, so a `delay` just under
/// [`MAX_DEFERRAL_MS`] may still be refused.
///
/// `granularity` is itself clamped to `[1ms, MAX_DEFERRAL_MS]`, so a zero or
/// absurd granularity is corrected rather than causing a division by zero or an
/// argument RabbitMQ would refuse. Library code must never panic on a value that
/// merely came out of a config file.
///
/// ```
/// use std::time::Duration;
/// use queuey_rabbitmq::topology::deferred_ttl_ms;
///
/// let second = Duration::from_secs(1);
/// assert_eq!(deferred_ttl_ms(Duration::from_secs(30), second), Some(30_000));
/// assert_eq!(deferred_ttl_ms(Duration::from_millis(29_200), second), Some(30_000));
/// assert_eq!(deferred_ttl_ms(Duration::from_millis(5), second), Some(1_000));
/// // 30 days is past what a hold queue can express.
/// assert_eq!(deferred_ttl_ms(Duration::from_secs(30 * 86_400), second), None);
/// ```
#[must_use]
pub fn deferred_ttl_ms(delay: Duration, granularity: Duration) -> Option<u32> {
    let max = u128::from(MAX_DEFERRAL_MS);
    let step = millis_ceil(granularity).clamp(1, max);
    // `step <= max`, so a delay of zero still waits one whole step.
    let ttl = millis_ceil(delay)
        .div_ceil(step)
        .saturating_mul(step)
        .max(step);
    (ttl <= max).then(|| u32::try_from(ttl).unwrap_or(MAX_DEFERRAL_MS))
}

/// Declaration arguments for the main queue `q`.
///
/// * `x-message-ttl` when [`QueueConfig::message_ttl`] is set (in whole
///   milliseconds, rounded up, at least 1).
/// * `x-max-priority` when [`QueueConfig::max_priority`] is `Some`, so deferred
///   jobs can come back ahead of the backlog.
///
/// Both are *declaration* arguments, which RabbitMQ refuses to change on an
/// existing queue (`PRECONDITION_FAILED`, which closes the declaring channel).
/// Adding `x-max-priority` to a queue that predates this feature therefore
/// requires deleting the queue or setting `max_priority = 0`.
#[must_use]
pub(crate) fn queue_args(config: &QueueConfig) -> FieldTable {
    let mut args = FieldTable::default();
    if let Some(ttl) = config.message_ttl {
        args.insert(
            ARG_MESSAGE_TTL.into(),
            AMQPValue::LongLongInt(ttl_millis(ttl)),
        );
    }
    if let Some(levels) = config.max_priority {
        // `x-max-priority` is validated by RabbitMQ as an integer of any width;
        // an unsigned byte is the exact domain of `QueueConfig::max_priority`.
        args.insert(ARG_MAX_PRIORITY.into(), AMQPValue::ShortShortUInt(levels));
    }
    args
}

/// Declaration arguments for the hold queue `q.deferred.{ttl_ms}`.
///
/// * `x-message-ttl = ttl_ms`: the delay itself, queue-wide, so the queue
///   drains in publish order.
/// * `x-dead-letter-exchange = ""` plus `x-dead-letter-routing-key = q`: an
///   expired message goes straight back onto the main queue.
/// * `x-expires = 2 * ttl_ms`: an idle hold queue deletes itself one TTL after
///   the last publish to it, so a delay that never recurs leaves nothing
///   behind. Every declare resets that timer, which is why the queue is
///   redeclared before every publish.
///
/// The arguments are a pure function of the hold queue's *name*: `ttl_ms` is in
/// the name, and the routing key and durability come from `config`, which is the
/// main queue the name is derived from. Nothing here is tunable, on purpose:
/// two processes with different settings would compute different arguments for
/// the same queue name and lock each other out with `PRECONDITION_FAILED`
/// for ever. See the module docs.
///
/// Never carries `x-max-priority`: a hold queue must stay FIFO, and the priority
/// only matters once the message is back on `q`.
#[must_use]
pub(crate) fn deferred_queue_args(config: &QueueConfig, ttl_ms: u32) -> FieldTable {
    let mut args = FieldTable::default();
    args.insert(
        ARG_MESSAGE_TTL.into(),
        AMQPValue::LongLongInt(ttl_ms.into()),
    );
    args.insert(
        ARG_DEAD_LETTER_EXCHANGE.into(),
        AMQPValue::LongString(LongString::from("")),
    );
    args.insert(
        ARG_DEAD_LETTER_ROUTING_KEY.into(),
        AMQPValue::LongString(LongString::from(config.name.as_str())),
    );
    // `ttl_ms` comes from `deferred_ttl_ms`, so it is at most `MAX_DEFERRAL_MS`
    // and the double fits. The clamp is for a hand-rolled call with a bigger
    // value: an `x-expires` past `u32::MAX` is a `PRECONDITION_FAILED`, not a
    // long timeout.
    let expires = u64::from(ttl_ms)
        .saturating_mul(2)
        .min(u64::from(MAX_TTL_MS));
    args.insert(
        ARG_EXPIRES.into(),
        AMQPValue::LongLongInt(i64::try_from(expires).unwrap_or(i64::from(MAX_TTL_MS))),
    );
    args
}

/// Declaration options for a queue that is never exclusive or auto-deleted.
///
/// Shared by the backend's own declarations and by the on-demand hold queue
/// declaration on the declaration channel, so the two can never disagree about
/// the flags and provoke a `PRECONDITION_FAILED`.
pub(crate) fn declare_options(durable: bool) -> QueueDeclareOptions {
    QueueDeclareOptions {
        passive: false,
        durable,
        exclusive: false,
        auto_delete: false,
        nowait: false,
    }
}

/// Declaration arguments for the dead-letter queue `q.dead`.
///
/// Deliberately empty: dead-lettered messages are terminal and must not expire
/// or be routed onwards without an operator looking at them.
#[must_use]
pub(crate) fn dead_queue_args(_config: &QueueConfig) -> FieldTable {
    FieldTable::default()
}

/// Whole milliseconds for `ttl`, rounded up, clamped to `[1, MAX_TTL_MS]`.
///
/// The upper bound is RabbitMQ's, not this crate's: a TTL past `u32::MAX` ms is
/// answered with `PRECONDITION_FAILED` and closes the declaring channel.
fn ttl_millis(ttl: Duration) -> i64 {
    let ms = ttl
        .as_nanos()
        .div_ceil(1_000_000)
        .clamp(1, u128::from(MAX_TTL_MS));
    i64::try_from(ms).unwrap_or(i64::from(MAX_TTL_MS))
}

/// Whole milliseconds in `duration`, rounded up. Unclamped, so callers can
/// decide what their own bounds are.
fn millis_ceil(duration: Duration) -> u128 {
    duration.as_nanos().div_ceil(1_000_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(name: &str) -> QueueConfig {
        QueueConfig::new(name)
    }

    #[test]
    fn dead_name_without_prefix() {
        assert_eq!(
            dead_queue_name("emails", DEFAULT_DEAD_SUFFIX),
            "emails.dead"
        );
    }

    #[test]
    fn dead_name_with_prefix() {
        assert_eq!(
            dead_queue_name("myapp.emails", DEFAULT_DEAD_SUFFIX),
            "myapp.emails.dead"
        );
    }

    #[test]
    fn custom_suffixes_are_honoured() {
        assert_eq!(dead_queue_name("q", "-dlq"), "q-dlq");
    }

    /// `QueueConfig::new` is a priority queue by default; most of the existing
    /// argument assertions predate that and only care about the TTL.
    fn plain(name: &str) -> QueueConfig {
        QueueConfig::new(name).max_priority(0)
    }

    #[test]
    fn queue_args_are_empty_without_ttl_or_priorities() {
        let args = queue_args(&plain("emails"));
        assert!(args.inner().is_empty(), "unexpected args: {args:?}");
        assert!(!args.contains_key(ARG_MESSAGE_TTL));
        assert!(!args.contains_key(ARG_MAX_PRIORITY));
    }

    #[test]
    fn queue_args_carry_message_ttl() {
        let cfg = plain("emails").message_ttl(Duration::from_secs(30));
        let args = queue_args(&cfg);
        assert_eq!(
            args.inner().get(ARG_MESSAGE_TTL),
            Some(&AMQPValue::LongLongInt(30_000))
        );
        assert_eq!(args.inner().len(), 1);
    }

    #[test]
    fn queue_args_carry_max_priority_when_the_queue_has_priorities() {
        // The `QueueConfig` default is 10 levels.
        let args = queue_args(&config("emails"));
        assert_eq!(
            args.inner().get(ARG_MAX_PRIORITY),
            Some(&AMQPValue::ShortShortUInt(10))
        );
        assert_eq!(args.inner().len(), 1);

        let args = queue_args(&config("emails").max_priority(255));
        assert_eq!(
            args.inner().get(ARG_MAX_PRIORITY),
            Some(&AMQPValue::ShortShortUInt(255))
        );
    }

    #[test]
    fn queue_args_omit_max_priority_when_priorities_are_off() {
        let cfg = config("emails").max_priority(0);
        assert_eq!(cfg.max_priority, None);
        assert!(!queue_args(&cfg).contains_key(ARG_MAX_PRIORITY));
    }

    #[test]
    fn queue_args_carry_ttl_and_priority_together() {
        let cfg = config("emails")
            .message_ttl(Duration::from_secs(5))
            .max_priority(3);
        let args = queue_args(&cfg);
        assert_eq!(
            args.inner().get(ARG_MESSAGE_TTL),
            Some(&AMQPValue::LongLongInt(5_000))
        );
        assert_eq!(
            args.inner().get(ARG_MAX_PRIORITY),
            Some(&AMQPValue::ShortShortUInt(3))
        );
        assert_eq!(args.inner().len(), 2);
    }

    #[test]
    fn only_the_main_queue_gets_priorities() {
        let cfg = config("emails");
        assert!(!dead_queue_args(&cfg).contains_key(ARG_MAX_PRIORITY));
        assert!(!deferred_queue_args(&cfg, 1_000).contains_key(ARG_MAX_PRIORITY));
    }

    #[test]
    fn sub_millisecond_ttl_rounds_up_to_one() {
        let cfg = config("emails").message_ttl(Duration::from_nanos(1));
        let args = queue_args(&cfg);
        assert_eq!(
            args.inner().get(ARG_MESSAGE_TTL),
            Some(&AMQPValue::LongLongInt(1))
        );
    }

    #[test]
    fn zero_ttl_is_clamped_to_one_millisecond() {
        let cfg = config("emails").message_ttl(Duration::ZERO);
        let args = queue_args(&cfg);
        assert_eq!(
            args.inner().get(ARG_MESSAGE_TTL),
            Some(&AMQPValue::LongLongInt(1))
        );
    }

    #[test]
    fn huge_ttl_is_clamped_to_what_rabbitmq_accepts() {
        // Not `i64::MAX`: RabbitMQ rejects anything past `u32::MAX` ms with
        // PRECONDITION_FAILED and closes the declaring channel.
        let cfg = config("emails").message_ttl(Duration::MAX);
        let args = queue_args(&cfg);
        assert_eq!(
            args.inner().get(ARG_MESSAGE_TTL),
            Some(&AMQPValue::LongLongInt(i64::from(MAX_TTL_MS)))
        );
    }

    #[test]
    fn ttl_exactly_at_the_limit_is_kept_verbatim() {
        let cfg = config("emails").message_ttl(Duration::from_millis(u64::from(MAX_TTL_MS)));
        let args = queue_args(&cfg);
        assert_eq!(
            args.inner().get(ARG_MESSAGE_TTL),
            Some(&AMQPValue::LongLongInt(i64::from(MAX_TTL_MS)))
        );
    }

    #[test]
    fn ttl_one_millisecond_past_the_limit_is_clamped() {
        let cfg = config("emails").message_ttl(Duration::from_millis(u64::from(MAX_TTL_MS) + 1));
        let args = queue_args(&cfg);
        assert_eq!(
            args.inner().get(ARG_MESSAGE_TTL),
            Some(&AMQPValue::LongLongInt(i64::from(MAX_TTL_MS)))
        );
    }

    #[test]
    fn dead_args_are_empty() {
        assert!(dead_queue_args(&config("emails")).inner().is_empty());
    }

    // -- deferral ----------------------------------------------------------

    const SECOND: Duration = Duration::from_secs(1);

    #[test]
    fn deferred_name_puts_the_ttl_in_the_queue_name() {
        assert_eq!(
            deferred_queue_name("emails", DEFAULT_DEFERRED_SUFFIX, 30_000),
            "emails.deferred.30000"
        );
        assert_eq!(
            deferred_queue_name("myapp.emails", DEFAULT_DEFERRED_SUFFIX, 1_000),
            "myapp.emails.deferred.1000"
        );
    }

    #[test]
    fn deferred_name_honours_a_custom_suffix() {
        assert_eq!(deferred_queue_name("q", "-hold", 250), "q-hold.250");
    }

    #[test]
    fn deferred_names_differ_per_ttl_which_is_the_whole_point() {
        let short = deferred_queue_name("q", DEFAULT_DEFERRED_SUFFIX, 1_000);
        let long = deferred_queue_name("q", DEFAULT_DEFERRED_SUFFIX, 60_000);
        assert_ne!(short, long);
    }

    #[test]
    fn an_exact_multiple_of_the_granularity_is_kept_verbatim() {
        assert_eq!(deferred_ttl_ms(SECOND, SECOND), Some(1_000));
        assert_eq!(
            deferred_ttl_ms(Duration::from_secs(30), SECOND),
            Some(30_000)
        );
        assert_eq!(
            deferred_ttl_ms(Duration::from_millis(750), Duration::from_millis(250)),
            Some(750)
        );
    }

    #[test]
    fn a_partial_step_rounds_up_so_a_job_never_returns_early() {
        assert_eq!(
            deferred_ttl_ms(Duration::from_millis(29_200), SECOND),
            Some(30_000)
        );
        assert_eq!(
            deferred_ttl_ms(Duration::from_millis(1_001), SECOND),
            Some(2_000)
        );
        assert_eq!(
            deferred_ttl_ms(Duration::from_millis(1_999), SECOND),
            Some(2_000)
        );
        // Sub-millisecond remainders count as a partial millisecond, then a
        // partial step.
        assert_eq!(
            deferred_ttl_ms(Duration::from_micros(1_000_001), SECOND),
            Some(2_000)
        );
    }

    #[test]
    fn a_delay_below_the_granularity_becomes_one_step() {
        assert_eq!(
            deferred_ttl_ms(Duration::from_millis(5), SECOND),
            Some(1_000)
        );
        assert_eq!(
            deferred_ttl_ms(Duration::from_nanos(1), SECOND),
            Some(1_000)
        );
        // Zero is not "publish it straight back": a deferral always waits.
        assert_eq!(deferred_ttl_ms(Duration::ZERO, SECOND), Some(1_000));
    }

    #[test]
    fn a_delay_past_the_cap_is_refused_rather_than_released_early() {
        // Clamping here is what the old code did, and it broke the one timing
        // guarantee a deferral makes.
        assert_eq!(deferred_ttl_ms(Duration::MAX, SECOND), None);
        assert_eq!(
            deferred_ttl_ms(Duration::from_secs(30 * 86_400), SECOND),
            None
        );
        assert_eq!(
            deferred_ttl_ms(
                Duration::from_millis(u64::from(MAX_DEFERRAL_MS) + 1),
                SECOND
            ),
            None
        );
    }

    #[test]
    fn the_cap_itself_is_accepted_at_a_matching_granularity() {
        assert_eq!(
            deferred_ttl_ms(
                Duration::from_millis(u64::from(MAX_DEFERRAL_MS)),
                Duration::from_millis(1)
            ),
            Some(MAX_DEFERRAL_MS)
        );
    }

    #[test]
    fn rounding_up_can_itself_cross_the_cap_and_is_then_refused() {
        // Just under the cap, but the next whole second is past it.
        let just_under = Duration::from_millis(u64::from(MAX_DEFERRAL_MS) - 1);
        assert_eq!(deferred_ttl_ms(just_under, SECOND), None);
    }

    #[test]
    fn a_zero_granularity_is_clamped_to_one_millisecond_rather_than_panicking() {
        assert_eq!(
            deferred_ttl_ms(Duration::from_millis(7), Duration::ZERO),
            Some(7)
        );
        assert_eq!(deferred_ttl_ms(Duration::ZERO, Duration::ZERO), Some(1));
        // Sub-millisecond granularities land in the same clamp.
        assert_eq!(
            deferred_ttl_ms(Duration::from_millis(7), Duration::from_nanos(1)),
            Some(7)
        );
    }

    #[test]
    fn an_absurd_granularity_is_clamped_to_the_cap_not_past_it() {
        assert_eq!(
            deferred_ttl_ms(SECOND, Duration::MAX),
            Some(MAX_DEFERRAL_MS)
        );
    }

    #[test]
    fn deferred_args_hold_for_the_ttl_then_dead_letter_onto_the_main_queue() {
        let args = deferred_queue_args(&config("myapp.emails"), 30_000);
        assert_eq!(
            args.inner().get(ARG_MESSAGE_TTL),
            Some(&AMQPValue::LongLongInt(30_000))
        );
        assert_eq!(
            args.inner().get(ARG_DEAD_LETTER_EXCHANGE),
            Some(&AMQPValue::LongString(LongString::from("")))
        );
        assert_eq!(
            args.inner().get(ARG_DEAD_LETTER_ROUTING_KEY),
            Some(&AMQPValue::LongString(LongString::from("myapp.emails")))
        );
        assert_eq!(
            args.inner().get(ARG_EXPIRES),
            Some(&AMQPValue::LongLongInt(60_000))
        );
        assert_eq!(args.inner().len(), 4);
    }

    #[test]
    fn deferred_args_depend_only_on_the_hold_queue_name() {
        // Two callers that disagree about *everything* except the main queue's
        // name and the TTL must still compute byte-identical arguments, or the
        // broker locks one of them out of `q.deferred.1000` for ever.
        let one = config("q").message_ttl(Duration::from_secs(99)).prefetch(1);
        let two = config("q").max_priority(0).prefetch(200);
        assert_eq!(
            deferred_queue_args(&one, 1_000),
            deferred_queue_args(&two, 1_000)
        );
    }

    #[test]
    fn expires_is_always_strictly_greater_than_the_ttl() {
        for ttl in [1_u32, 1_000, 30_000, MAX_DEFERRAL_MS] {
            let args = deferred_queue_args(&config("q"), ttl);
            let AMQPValue::LongLongInt(expires) = args.inner().get(ARG_EXPIRES).expect("x-expires")
            else {
                panic!("x-expires is not a long long int");
            };
            assert!(
                *expires > i64::from(ttl),
                "x-expires {expires} must outlive the {ttl}ms TTL"
            );
            assert!(
                *expires <= i64::from(MAX_TTL_MS),
                "x-expires {expires} is past what RabbitMQ accepts"
            );
        }
    }

    #[test]
    fn expires_is_clamped_when_doubling_the_ttl_would_overflow() {
        // Unreachable through `deferred_ttl_ms`, which caps at `MAX_DEFERRAL_MS`;
        // guarded anyway so a hand-rolled call cannot produce an argument
        // RabbitMQ answers with PRECONDITION_FAILED.
        let args = deferred_queue_args(&config("q"), MAX_TTL_MS);
        assert_eq!(
            args.inner().get(ARG_EXPIRES),
            Some(&AMQPValue::LongLongInt(i64::from(MAX_TTL_MS)))
        );
    }

    #[test]
    fn deferred_args_never_carry_a_queue_ttl_of_their_own_from_the_config() {
        // The hold queue's TTL is the delay, not the main queue's message TTL.
        let cfg = config("q").message_ttl(Duration::from_secs(99));
        let args = deferred_queue_args(&cfg, 2_000);
        assert_eq!(
            args.inner().get(ARG_MESSAGE_TTL),
            Some(&AMQPValue::LongLongInt(2_000))
        );
    }

    #[test]
    fn declare_options_are_shared_and_never_auto_delete() {
        let durable = declare_options(true);
        assert!(durable.durable);
        assert!(!durable.auto_delete);
        assert!(!durable.exclusive);
        assert!(!durable.passive);
        assert!(!durable.nowait);
        assert!(!declare_options(false).durable);
    }
}
