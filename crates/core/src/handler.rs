//! [`JobHandler`], the [`JobContext`] it receives and the [`FnHandler`] adapter.

use std::time::Duration;

use async_trait::async_trait;
use uuid::Uuid;

use crate::{error::JobError, job::Job};

/// Metadata about the current execution, available to handlers.
#[derive(Debug, Clone)]
pub struct JobContext {
    /// Stable id of the job, unchanged across retries.
    pub job_id: Uuid,
    /// `Job::NAME` of the running job.
    pub job_type: &'static str,
    /// Broker queue name the job was consumed from.
    pub queue: &'static str,
    /// 1-based.
    pub attempt: u32,
    /// Total attempts the effective retry policy allows.
    pub max_attempts: u32,
    /// How often this job was deferred (see [`crate::JobError::Deferred`]).
    ///
    /// Independent of `attempt`. There is no built-in cap: a handler that wants one
    /// checks this and returns [`crate::JobError::Fatal`] instead of deferring again.
    pub deferrals: u32,
    /// Broker message priority this delivery arrived with; `0` is normal work.
    pub priority: u8,
    /// Time since first enqueue.
    pub age: Duration,
}

impl JobContext {
    /// Whether a failure now means the job is dead-lettered.
    pub fn is_last_attempt(&self) -> bool {
        self.attempt >= self.max_attempts
    }
}

/// Processes jobs of one type. Register with [`crate::WorkerBuilder::handler`].
#[async_trait]
pub trait JobHandler: Send + Sync + 'static {
    /// The one job type this handler processes.
    type Job: Job;

    /// Process one job. Returning `Err` hands control to the retry policy.
    async fn handle(&self, job: Self::Job, ctx: JobContext) -> Result<(), JobError>;
}

/// Blanket adapter so plain async closures can be handlers:
/// `builder.handler(FnHandler::<SendEmail, _>::new(|job, ctx| async move { ... }))`.
pub struct FnHandler<J, F> {
    f: F,
    _job: std::marker::PhantomData<fn(J)>,
}

impl<J, F> FnHandler<J, F> {
    /// Wrap `f` as a [`JobHandler`] for job type `J`.
    pub fn new(f: F) -> Self {
        Self {
            f,
            _job: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<J, F, Fut> JobHandler for FnHandler<J, F>
where
    J: Job,
    F: Fn(J, JobContext) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), JobError>> + Send + 'static,
{
    type Job = J;

    async fn handle(&self, job: J, ctx: JobContext) -> Result<(), JobError> {
        (self.f)(job, ctx).await
    }
}
