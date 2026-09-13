//! Hand-written fixtures shared by the unit tests of this crate.
//!
//! The derive macros live in `queuey-macros`, which depends on this crate,
//! so the core tests spell out a [`QueueSet`] and a few [`Job`] impls by hand. They
//! double as executable documentation of what the macros must generate.

#![allow(dead_code)]

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{
    job::Job,
    queue::{QueueConfig, QueueSet},
    retry::{Backoff, RetryPolicy},
};

/// Queues with deliberately different retry policies, prefetch and priority setups.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum TestQueues {
    /// Prefetch 4, three attempts with a one second fixed backoff, 10 priority levels.
    Alpha,
    /// Prefetch 2, no retries (`max_attempts = 1`), 10 priority levels.
    Beta,
    /// Prefetch 1, no retries, and *not* a priority queue (`max_priority = 0`).
    Gamma,
}

impl TestQueues {
    /// Fixed backoff used by [`TestQueues::Alpha`].
    pub(crate) const ALPHA_DELAY: Duration = Duration::from_secs(1);
}

impl QueueSet for TestQueues {
    fn all() -> &'static [Self] {
        &[TestQueues::Alpha, TestQueues::Beta, TestQueues::Gamma]
    }

    fn name(&self) -> &'static str {
        match self {
            TestQueues::Alpha => "test.alpha",
            TestQueues::Beta => "test.beta",
            TestQueues::Gamma => "test.gamma",
        }
    }

    fn config(&self) -> QueueConfig {
        match self {
            TestQueues::Alpha => QueueConfig::new("test.alpha")
                .prefetch(4)
                .retry(RetryPolicy::new(3, Backoff::Fixed(Self::ALPHA_DELAY))),
            TestQueues::Beta => QueueConfig::new("test.beta")
                .prefetch(2)
                .retry(RetryPolicy::none()),
            TestQueues::Gamma => QueueConfig::new("test.gamma")
                .prefetch(1)
                .retry(RetryPolicy::none())
                .max_priority(0),
        }
    }
}

/// Plain job on [`TestQueues::Alpha`]; inherits the queue retry policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Greet {
    /// Who to greet.
    pub(crate) name: String,
}

impl Greet {
    /// Convenience constructor.
    pub(crate) fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
        }
    }
}

impl Job for Greet {
    type Queue = TestQueues;
    const NAME: &'static str = "test::Greet";
    const QUEUE: Self::Queue = TestQueues::Alpha;
}

/// Job on [`TestQueues::Beta`]; inherits that queue's "no retries" policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Ping {
    /// Sequence number, handy for FIFO assertions.
    pub(crate) seq: u32,
}

impl Job for Ping {
    type Queue = TestQueues;
    const NAME: &'static str = "test::Ping";
    const QUEUE: Self::Queue = TestQueues::Beta;
}

/// Job on [`TestQueues::Beta`] that overrides the queue policy with two attempts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Stubborn {
    /// Free-form identifier.
    pub(crate) id: u32,
}

impl Job for Stubborn {
    type Queue = TestQueues;
    const NAME: &'static str = "test::Stubborn";
    const QUEUE: Self::Queue = TestQueues::Beta;

    fn retry_policy() -> Option<RetryPolicy> {
        Some(RetryPolicy::fixed(2, Duration::from_secs(5)))
    }
}

/// Job on [`TestQueues::Gamma`], the queue that is not a priority queue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Nudge {
    /// Free-form identifier.
    pub(crate) id: u32,
}

impl Job for Nudge {
    type Queue = TestQueues;
    const NAME: &'static str = "test::Nudge";
    const QUEUE: Self::Queue = TestQueues::Gamma;
}

/// Job on [`TestQueues::Alpha`] for which no handler is ever registered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Orphan {
    /// Free-form identifier.
    pub(crate) id: u32,
}

impl Job for Orphan {
    type Queue = TestQueues;
    const NAME: &'static str = "test::Orphan";
    const QUEUE: Self::Queue = TestQueues::Alpha;
}
