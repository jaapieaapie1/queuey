//! Queue declarations: [`QueueSet`] and [`QueueConfig`].

use std::time::Duration;

use crate::retry::RetryPolicy;

/// Static configuration of a single queue.
#[derive(Debug, Clone, PartialEq)]
pub struct QueueConfig {
    /// Fully-qualified broker queue name (prefix already applied).
    pub name: String,
    /// Max unacknowledged messages per consumer.
    pub prefetch: u16,
    /// Default retry policy for jobs on this queue (a job may override).
    pub retry: RetryPolicy,
    /// Whether the queue survives broker restarts.
    pub durable: bool,
    /// Optional per-message TTL applied on publish.
    pub message_ttl: Option<Duration>,
    /// Number of priority levels the main queue supports (`x-max-priority` on RabbitMQ).
    ///
    /// `None` means the queue is not a priority queue and message priorities are
    /// ignored by the broker. Deferred jobs (see [`crate::JobError::Deferred`]) are
    /// republished with the highest level so they run ahead of the backlog.
    pub max_priority: Option<u8>,
}

/// Default for [`QueueConfig::max_priority`]: RabbitMQ recommends at most 10 levels.
pub const DEFAULT_MAX_PRIORITY: u8 = 10;

impl QueueConfig {
    /// Config for `name` with defaults: prefetch 16, durable, no retries, 10 priority levels.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            prefetch: 16,
            retry: RetryPolicy::default(),
            durable: true,
            message_ttl: None,
            max_priority: Some(DEFAULT_MAX_PRIORITY),
        }
    }
    /// Set the maximum number of unacknowledged messages per consumer.
    pub fn prefetch(mut self, prefetch: u16) -> Self {
        self.prefetch = prefetch;
        self
    }
    /// Set the default retry policy for this queue.
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }
    /// Set whether the queue survives broker restarts.
    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = durable;
        self
    }
    /// Set a per-message TTL applied on publish.
    pub fn message_ttl(mut self, ttl: Duration) -> Self {
        self.message_ttl = Some(ttl);
        self
    }
    /// Set the number of priority levels; `0` turns priorities off (`max_priority = None`).
    ///
    /// Changing this for a queue that already exists on the broker is refused by
    /// RabbitMQ (`PRECONDITION_FAILED`): delete the queue first.
    pub fn max_priority(mut self, levels: u8) -> Self {
        self.max_priority = (levels > 0).then_some(levels);
        self
    }
}

/// A closed set of queues, normally an enum with `#[derive(Queues)]` from the
/// macros crate; the hand-written equivalent is:
///
/// ```
/// use queuey_core::{QueueConfig, QueueSet, RetryPolicy};
///
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// enum AppQueues { Emails, Images }
///
/// impl QueueSet for AppQueues {
///     fn all() -> &'static [Self] { &[AppQueues::Emails, AppQueues::Images] }
///     fn name(&self) -> &'static str {
///         match self { AppQueues::Emails => "myapp.emails", AppQueues::Images => "myapp.img" }
///     }
///     fn config(&self) -> QueueConfig {
///         match self {
///             AppQueues::Emails => QueueConfig::new(self.name()).prefetch(10),
///             AppQueues::Images => QueueConfig::new(self.name()).retry(RetryPolicy::exponential(3)),
///         }
///     }
/// }
///
/// assert_eq!(AppQueues::from_name("myapp.img"), Some(AppQueues::Images));
/// assert_eq!(AppQueues::Emails.config().prefetch, 10);
/// ```
pub trait QueueSet:
    Copy + Clone + Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static
{
    /// Every variant of the set, in declaration order.
    fn all() -> &'static [Self];

    /// Fully-qualified broker queue name for this variant.
    fn name(&self) -> &'static str;

    /// Full configuration for this variant.
    fn config(&self) -> QueueConfig;

    /// Look up a variant by its broker name.
    fn from_name(name: &str) -> Option<Self> {
        Self::all().iter().copied().find(|q| q.name() == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_priority_defaults_to_ten_levels() {
        assert_eq!(DEFAULT_MAX_PRIORITY, 10);
        assert_eq!(
            QueueConfig::new("q").max_priority,
            Some(DEFAULT_MAX_PRIORITY)
        );
    }

    #[test]
    fn max_priority_zero_turns_priorities_off() {
        assert_eq!(QueueConfig::new("q").max_priority(0).max_priority, None);
    }

    #[test]
    fn max_priority_stores_the_number_of_levels() {
        assert_eq!(QueueConfig::new("q").max_priority(5).max_priority, Some(5));
        assert_eq!(
            QueueConfig::new("q").max_priority(255).max_priority,
            Some(255)
        );
        // Last call wins, including back to off.
        assert_eq!(
            QueueConfig::new("q").max_priority(5).max_priority(0),
            QueueConfig::new("q").max_priority(0)
        );
    }

    #[test]
    fn the_other_builders_leave_max_priority_alone() {
        let config = QueueConfig::new("q")
            .prefetch(3)
            .durable(false)
            .message_ttl(Duration::from_secs(1));
        assert_eq!(config.max_priority, Some(DEFAULT_MAX_PRIORITY));
    }
}
