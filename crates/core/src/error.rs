//! Error types: [`enum@Error`] for infrastructure, [`JobError`] for handlers.

use thiserror::Error;

/// `Result` alias defaulting to this crate's [`enum@Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Infrastructure-level errors (serialization, transport, configuration).
#[derive(Debug, Error)]
pub enum Error {
    /// A job payload or envelope could not be (de)serialized.
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    /// The transport failed.
    #[error("backend error: {0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),

    /// A queue name that is not part of the queue set was used.
    #[error("unknown queue `{0}`")]
    UnknownQueue(String),

    /// No handler is registered for a job type.
    #[error("no handler registered for job type `{0}`")]
    NoHandler(String),

    /// Two handlers claimed the same `Job::NAME`.
    #[error("handler for job type `{0}` registered twice")]
    DuplicateHandler(String),

    /// The worker or backend is shut down.
    #[error("worker is shut down")]
    ShutDown,

    /// A consumer stream ended on its own, which means the backend went away.
    #[error("consumer for queue `{0}` stopped unexpectedly")]
    ConsumerStopped(String),
}

impl Error {
    /// Wrap a transport error.
    pub fn backend<E: std::error::Error + Send + Sync + 'static>(e: E) -> Self {
        Self::Backend(Box::new(e))
    }
}

/// Error returned by a [`crate::JobHandler`].
#[derive(Debug, Error)]
pub enum JobError {
    /// Transient failure; the retry policy decides whether to retry.
    #[error("job failed (retryable): {0}")]
    Retryable(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// Permanent failure; go straight to dead-letter regardless of policy.
    #[error("job failed (fatal): {0}")]
    Fatal(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// Not a failure: the job could not run *yet* and must be tried again in `delay`.
    ///
    /// The motivating case is an external API answering `429 Too Many Requests` with
    /// `Retry-After: 30`: nothing went wrong. The job has to wait exactly that
    /// long and then run *before* the backlog that piled up meanwhile.
    ///
    /// Unlike [`JobError::Retryable`]:
    ///
    /// * [`crate::Envelope::attempt`] is **unchanged**, so a deferral never burns down
    ///   the retry budget and a job may defer itself indefinitely;
    /// * the retry policy is **not consulted**: neither its backoff (the handler
    ///   states the delay) nor its `max_attempts` (nothing failed, so nothing is
    ///   dead-lettered);
    /// * [`crate::Envelope::deferrals`] is incremented and the envelope comes back with
    ///   the highest priority its queue supports, ahead of normally enqueued work;
    /// * the worker logs it at `INFO`, not `WARN`/`ERROR`.
    ///
    /// There is no built-in cap: a handler that wants one inspects
    /// [`crate::JobContext::deferrals`] and returns [`JobError::Fatal`] instead.
    #[error("job deferred for {delay:?}: {reason}")]
    Deferred {
        /// How long the job must wait before it is delivered again.
        delay: std::time::Duration,
        /// Why it was deferred, for logs. Not part of any control flow.
        reason: String,
    },
}

impl JobError {
    /// Wrap `e` as a transient failure.
    pub fn retryable<E: std::error::Error + Send + Sync + 'static>(e: E) -> Self {
        Self::Retryable(Box::new(e))
    }
    /// Wrap `e` as a permanent failure.
    pub fn fatal<E: std::error::Error + Send + Sync + 'static>(e: E) -> Self {
        Self::Fatal(Box::new(e))
    }
    /// Transient failure with a plain message.
    pub fn retryable_msg(msg: impl Into<String>) -> Self {
        Self::Retryable(msg.into().into())
    }
    /// Permanent failure with a plain message.
    pub fn fatal_msg(msg: impl Into<String>) -> Self {
        Self::Fatal(msg.into().into())
    }
    /// Defer the job by `delay` with the generic reason `"deferred"`.
    ///
    /// Not a failure: the attempt counter is untouched and the retry policy is not
    /// consulted. See [`JobError::Deferred`].
    pub fn deferred(delay: std::time::Duration) -> Self {
        Self::Deferred {
            delay,
            reason: "deferred".to_owned(),
        }
    }
    /// Defer the job by `delay`, recording why (`"rate limited: Retry-After 30s"`).
    ///
    /// Not a failure: the attempt counter is untouched and the retry policy is not
    /// consulted. See [`JobError::Deferred`].
    pub fn deferred_msg(delay: std::time::Duration, msg: impl Into<String>) -> Self {
        Self::Deferred {
            delay,
            reason: msg.into(),
        }
    }
}

/// Convenience: any `std::error::Error` becomes a retryable job error.
impl From<Box<dyn std::error::Error + Send + Sync + 'static>> for JobError {
    fn from(e: Box<dyn std::error::Error + Send + Sync + 'static>) -> Self {
        Self::Retryable(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn deferred_uses_a_generic_reason() {
        let err = JobError::deferred(Duration::from_secs(30));
        match &err {
            JobError::Deferred { delay, reason } => {
                assert_eq!(*delay, Duration::from_secs(30));
                assert_eq!(reason, "deferred");
            }
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(err.to_string(), "job deferred for 30s: deferred");
    }

    #[test]
    fn deferred_msg_keeps_the_message() {
        let err = JobError::deferred_msg(Duration::from_millis(1_500), "rate limited");
        match &err {
            JobError::Deferred { delay, reason } => {
                assert_eq!(*delay, Duration::from_millis(1_500));
                assert_eq!(reason, "rate limited");
            }
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(err.to_string(), "job deferred for 1.5s: rate limited");
    }

    #[test]
    fn a_deferral_carries_no_source_error() {
        use std::error::Error as _;
        assert!(
            JobError::deferred(Duration::from_secs(1))
                .source()
                .is_none()
        );
        assert!(JobError::retryable_msg("boom").source().is_some());
    }
}
