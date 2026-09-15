//! The reconnection API must be usable by a crate that depends on `queuey`
//! alone: no direct dependency on `queuey-rabbitmq` or `queuey-core`, and no
//! reaching through the `queuey::rabbitmq` module for half the types while the
//! other half sits at the top level.
//!
//! If any of these re-exports goes missing, this file stops compiling.

#![cfg(feature = "rabbitmq")]

use std::{sync::Arc, time::Duration};

use queuey::{
    Attempt, BackoffPolicy, RabbitMqOptions, Rebuilding, ReconnectPolicy, prelude::Backoff,
};

/// A policy of the kind `BackoffPolicy` cannot express: it reads the error and
/// what is being rebuilt, not just how many attempts have failed.
#[derive(Debug)]
struct HandWritten;

impl ReconnectPolicy for HandWritten {
    fn next_delay(&self, attempt: Attempt<'_>) -> Option<Duration> {
        // Credentials do not fix themselves; stop rather than hammer the broker.
        if attempt
            .error
            .is_some_and(|error| error.to_string().contains("ACCESS_REFUSED"))
        {
            return None;
        }
        match attempt.rebuilding {
            // A subscription failing on a live connection usually means the
            // queue is gone, and waiting does not bring one back.
            Rebuilding::Consumer if attempt.failures >= 3 => None,
            _ => Some(Duration::from_secs(1)),
        }
    }
}

#[test]
fn a_hand_written_policy_reaches_the_options_through_the_facade() {
    let options = RabbitMqOptions::default().reconnect_with(HandWritten);
    let policy = options.reconnect.expect("a policy");

    assert_eq!(
        policy.next_delay(Attempt::first(Rebuilding::Connection)),
        Some(Duration::from_secs(1))
    );

    let refused = std::io::Error::other("ACCESS_REFUSED - login was refused");
    assert_eq!(
        policy.next_delay(Attempt::after(Rebuilding::Connection, 1, &refused)),
        None
    );

    let reset = std::io::Error::other("connection reset by peer");
    assert_eq!(
        policy.next_delay(Attempt::after(Rebuilding::Consumer, 3, &reset)),
        None,
        "the consumer arm is reachable from outside the crate"
    );
}

#[test]
fn the_built_in_policy_is_tunable_through_the_facade() {
    // `Backoff` comes from the prelude, so tuning the built-in policy needs no
    // dependency on `queuey-core`.
    let tuned = BackoffPolicy::default()
        .max_attempts(Some(2))
        .backoff(Backoff::Fixed(Duration::from_millis(250)));

    assert_eq!(
        tuned.next_delay(Attempt::first(Rebuilding::Connection)),
        Some(Duration::ZERO),
        "the first attempt after a drop is immediate"
    );

    let error = std::io::Error::other("broker went away");
    assert_eq!(
        tuned.next_delay(Attempt::after(Rebuilding::Connection, 1, &error)),
        Some(Duration::from_millis(250))
    );
    assert_eq!(
        tuned.next_delay(Attempt::after(Rebuilding::Connection, 2, &error)),
        None,
        "two attempts, then the policy declines"
    );

    let options = RabbitMqOptions::default().reconnect_with(tuned);
    assert!(options.reconnect.is_some());
}

#[test]
fn a_policy_can_be_shared_as_an_arc_and_reconnection_can_be_switched_off() {
    // Both halves of the setter: pre-wrapped, and none at all.
    let shared: Arc<dyn ReconnectPolicy> = Arc::new(HandWritten);
    let options = RabbitMqOptions::default().reconnect(Some(Arc::clone(&shared)));
    assert!(options.reconnect.is_some());

    let never = RabbitMqOptions::default().reconnect(None);
    assert!(never.reconnect.is_none());

    // `Debug` survives erasure, so a printed config does not lie about which
    // policy is in force.
    assert!(format!("{shared:?}").contains("HandWritten"));
}
