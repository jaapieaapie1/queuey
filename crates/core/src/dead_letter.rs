//! Notification that a job will not run again: [`DeadLetterHook`], [`DeadLetter`],
//! [`DeadLetterCause`].
//!
//! Dead-lettering is the one outcome an application usually has to act on (mark an
//! endpoint dead, alert, refund, tell the system that enqueued the job), and it is the
//! one outcome no handler is in a position to observe: a job with no handler or an
//! undecodable payload never reaches user code, and a handler that returns
//! `Retryable` cannot tell whether the policy will retry or give up without
//! re-deriving the policy itself. The hook is the worker's report of what it decided.
//!
//! The hook runs *before* the delivery is settled, deliberately. If the process dies
//! in between, the broker redelivers the job and the hook runs again: at-least-once,
//! matching the rest of the system, rather than a notification that can be lost while
//! the job is definitively gone.

use async_trait::async_trait;

use crate::envelope::Envelope;

/// Why a job was dead-lettered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeadLetterCause {
    /// No handler was registered for the envelope's `job_type`. The payload was
    /// never decoded and no user code ran.
    NoHandler,
    /// The payload did not deserialize into the handler's job type. Nothing ran.
    Decode,
    /// The handler returned [`crate::JobError::Fatal`]: a permanent failure, so the
    /// retry policy was never consulted.
    Fatal,
    /// The handler returned [`crate::JobError::Retryable`] on its last allowed
    /// attempt and the policy gave up.
    Exhausted,
}

impl DeadLetterCause {
    /// Whether user code ran at all. `false` for [`NoHandler`](Self::NoHandler) and
    /// [`Decode`](Self::Decode), which are wiring faults rather than job failures.
    #[must_use]
    pub fn reached_handler(&self) -> bool {
        matches!(self, Self::Fatal | Self::Exhausted)
    }
}

/// A job that has been given up on, as handed to a [`DeadLetterHook`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DeadLetter {
    /// The envelope as it was delivered. `envelope.attempt` is the attempt that
    /// failed, and `envelope.job_id` is stable across every attempt this job made.
    pub envelope: Envelope,
    /// What the worker decided.
    pub cause: DeadLetterCause,
    /// The same human-readable text the worker passes to
    /// [`Delivery::dead_letter`](crate::Delivery::dead_letter), including the
    /// underlying error.
    pub reason: String,
    /// `max_attempts` of the effective retry policy, when there was one. `None` for
    /// [`DeadLetterCause::NoHandler`], where no policy could be resolved.
    pub max_attempts: Option<u32>,
}

impl DeadLetter {
    /// Attempts this job made, including the one that just failed.
    #[must_use]
    pub fn attempts(&self) -> u32 {
        self.envelope.attempt
    }
}

/// Called by the worker for every job it dead-letters.
///
/// Register with [`crate::WorkerBuilder::on_dead_letter`]. The hook is a
/// notification, not a veto: whatever it does, the delivery is dead-lettered
/// afterwards. It is awaited before the delivery is settled, so a hook that blocks
/// holds on to one prefetch slot; do the slow part elsewhere if that matters. A panic
/// is caught and logged, and dead-lettering proceeds.
///
/// ```
/// use queuey_core::{DeadLetter, DeadLetterHook, async_trait};
///
/// struct Alert;
///
/// #[async_trait]
/// impl DeadLetterHook for Alert {
///     async fn on_dead_letter(&self, dead: DeadLetter) {
///         eprintln!(
///             "{} gave up after {} attempts: {}",
///             dead.envelope.job_type,
///             dead.attempts(),
///             dead.reason
///         );
///     }
/// }
/// ```
#[async_trait]
pub trait DeadLetterHook: Send + Sync + 'static {
    /// Report one dead-lettered job.
    async fn on_dead_letter(&self, dead: DeadLetter);
}

/// Adapter so a plain async closure can be a hook:
/// `builder.on_dead_letter(FnDeadLetterHook::new(|dead| async move { ... }))`.
///
/// The same shape as [`FnHandler`](crate::FnHandler), and for the same reason: a
/// blanket impl over `Fn` would stop anyone else implementing the trait.
pub struct FnDeadLetterHook<F> {
    f: F,
}

impl<F> FnDeadLetterHook<F> {
    /// Wrap `f` as a [`DeadLetterHook`].
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

#[async_trait]
impl<F, Fut> DeadLetterHook for FnDeadLetterHook<F>
where
    F: Fn(DeadLetter) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    async fn on_dead_letter(&self, dead: DeadLetter) {
        (self.f)(dead).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_handler_causes_report_a_job_failure() {
        assert!(DeadLetterCause::Fatal.reached_handler());
        assert!(DeadLetterCause::Exhausted.reached_handler());
        assert!(!DeadLetterCause::NoHandler.reached_handler());
        assert!(!DeadLetterCause::Decode.reached_handler());
    }
}
