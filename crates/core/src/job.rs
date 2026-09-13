//! The [`Job`] trait: a serializable payload bound to one queue.

use serde::{Serialize, de::DeserializeOwned};

use crate::{queue::QueueSet, retry::RetryPolicy};

/// A unit of work. Normally implemented via `#[derive(Job)]` from the macros
/// crate; the hand-written equivalent is:
///
/// ```
/// use queuey_core::{Job, QueueConfig, QueueSet, RetryPolicy};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// enum AppQueues { Emails }
///
/// impl QueueSet for AppQueues {
///     fn all() -> &'static [Self] { &[AppQueues::Emails] }
///     fn name(&self) -> &'static str { "emails" }
///     fn config(&self) -> QueueConfig { QueueConfig::new(self.name()) }
/// }
///
/// #[derive(Serialize, Deserialize)]
/// struct SendEmail { to: String }
///
/// impl Job for SendEmail {
///     type Queue = AppQueues;
///     const NAME: &'static str = "SendEmail";
///     const QUEUE: AppQueues = AppQueues::Emails;
///     fn retry_policy() -> Option<RetryPolicy> { Some(RetryPolicy::exponential(5)) }
/// }
///
/// assert_eq!(SendEmail::QUEUE.name(), "emails");
/// ```
pub trait Job: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// The queue set this job belongs to.
    type Queue: QueueSet;

    /// Unique, stable identifier for this job type. Used for routing to the right
    /// handler. Defaults to the fully-qualified type path via the derive macro.
    const NAME: &'static str;

    /// The queue (variant) this job is published to and consumed from.
    const QUEUE: Self::Queue;

    /// Per-job retry override. `None` means "use the queue's policy".
    fn retry_policy() -> Option<RetryPolicy> {
        None
    }
}
