//! [`JobHandler`], the [`JobContext`] it receives and the [`FnHandler`] adapter.

use std::time::Duration;

use async_trait::async_trait;
use uuid::Uuid;

use crate::{error::JobError, job::Job, retry::RetryPolicy};

/// Metadata about the current execution, available to handlers.
#[derive(Debug, Clone)]
#[non_exhaustive]
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
    /// The correlation id the job was enqueued with, if any. Set through
    /// [`EnqueueOptions::correlation_id`](crate::EnqueueOptions::correlation_id),
    /// carried across retries and deferrals, never interpreted by the library.
    pub correlation_id: Option<String>,
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

    /// Choose a retry policy for *this* job, looking at its decoded payload.
    ///
    /// Every other way of configuring retries answers "how do jobs of this kind
    /// behave"; this one answers "how does this particular job behave", which is the
    /// question a payload often decides. Webhook deliveries are the motivating case:
    /// the endpoint that has been flaky since it was onboarded earns ten attempts
    /// over an hour, a brand new one gets three, and both are the same job type on
    /// the same queue. The handler has `&self`, so the answer can come from whatever
    /// configuration or client the handler was built with.
    ///
    /// Returning `None` (the default) keeps the configured policy, resolved from
    /// [`WorkerBuilder::job_retry_override`](crate::WorkerBuilder::job_retry_override),
    /// [`WorkerBuilder::retry_override`](crate::WorkerBuilder::retry_override),
    /// `#[job(retry(...))]` and `#[queue(retry(...))]` in that order. A `Some` beats
    /// all four: it is the most specific statement available, and it is the one the
    /// consuming process makes about work it is holding in its hands.
    ///
    /// Called once per delivery, after the payload decodes and before
    /// [`handle`](Self::handle), so [`JobContext::max_attempts`] and
    /// [`JobContext::is_last_attempt`] already reflect what this returned. It runs on
    /// every attempt, and nothing pins it to the answer it gave last time: returning a
    /// policy with fewer attempts than the job has already made retires that job on
    /// its next failure, which is a reasonable way to stop a retry storm and a
    /// surprising way to lose work. Keep it cheap and side-effect free: it is not
    /// async, so it cannot do I/O.
    ///
    /// ```
    /// # use queuey_core::{Job, JobContext, JobError, JobHandler, RetryPolicy, async_trait};
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    /// # enum Q { Webhooks }
    /// # impl queuey_core::QueueSet for Q {
    /// #     fn all() -> &'static [Self] { &[Q::Webhooks] }
    /// #     fn name(&self) -> &'static str { "webhooks" }
    /// #     fn config(&self) -> queuey_core::QueueConfig { queuey_core::QueueConfig::new("webhooks") }
    /// # }
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Deliver { endpoint: String }
    /// # impl Job for Deliver {
    /// #     type Queue = Q;
    /// #     const NAME: &'static str = "Deliver";
    /// #     const QUEUE: Q = Q::Webhooks;
    /// # }
    /// struct Webhooks { lenient: Vec<String> }
    ///
    /// #[async_trait]
    /// impl JobHandler for Webhooks {
    ///     type Job = Deliver;
    ///
    ///     fn set_retry_policy(&self, job: &Deliver) -> Option<RetryPolicy> {
    ///         self.lenient
    ///             .contains(&job.endpoint)
    ///             .then(|| RetryPolicy::exponential(10))
    ///     }
    ///
    ///     async fn handle(&self, job: Deliver, ctx: JobContext) -> Result<(), JobError> {
    ///         # let _ = (job, ctx);
    ///         Ok(())
    ///     }
    /// }
    /// ```
    fn set_retry_policy(&self, job: &Self::Job) -> Option<RetryPolicy> {
        let _ = job;
        None
    }
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
