//! [`JobHandler`], the [`JobContext`] it receives and the [`FnHandler`] adapter.

use std::time::Duration;

use async_trait::async_trait;
use uuid::Uuid;

use crate::{error::JobError, job::Job, queue::QueueSet, retry::RetryPolicy};

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
    /// A context for `J` on its `attempt`-th of `max_attempts` tries.
    ///
    /// The worker builds these from the envelope it just decoded. They are
    /// public because this type is `#[non_exhaustive]`, and without a
    /// constructor the only way to get one is to stand up a [`MemoryBackend`]
    /// and a [`Worker`] and drive a real delivery through them — a lot of
    /// machinery for a test of a handler's own branching on `attempt`,
    /// `deferrals` or `age`.
    ///
    /// The four arguments are the ones with no defensible default; everything
    /// else starts neutral (a fresh `job_id`, zero deferrals, priority `0`, zero
    /// age, no correlation id) and is set by the builders below, so a test only
    /// spells out what it is actually asserting on. `job_type` and `queue` come
    /// from `J` rather than being passed, because the worker cannot get them
    /// wrong either.
    ///
    /// ```
    /// # use queuey_core::{Job, JobContext, QueueConfig, QueueSet};
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    /// # enum Q { Emails }
    /// # impl QueueSet for Q {
    /// #     fn all() -> &'static [Self] { &[Q::Emails] }
    /// #     fn name(&self) -> &'static str { "emails" }
    /// #     fn config(&self) -> QueueConfig { QueueConfig::new("emails") }
    /// # }
    /// # #[derive(Serialize, Deserialize)]
    /// # struct SendEmail;
    /// # impl Job for SendEmail {
    /// #     type Queue = Q;
    /// #     const NAME: &'static str = "SendEmail";
    /// #     const QUEUE: Q = Q::Emails;
    /// # }
    /// // The last attempt of three, having been deferred twice already.
    /// let ctx = JobContext::new::<SendEmail>(3, 3).with_deferrals(2);
    /// assert!(ctx.is_last_attempt());
    /// assert_eq!(ctx.job_type, "SendEmail");
    /// assert_eq!(ctx.queue, "emails");
    /// ```
    ///
    /// [`MemoryBackend`]: crate::MemoryBackend
    /// [`Worker`]: crate::Worker
    #[must_use]
    pub fn new<J: Job>(attempt: u32, max_attempts: u32) -> Self {
        Self {
            job_id: Uuid::new_v4(),
            job_type: J::NAME,
            queue: J::QUEUE.name(),
            attempt,
            max_attempts,
            deferrals: 0,
            priority: 0,
            age: Duration::ZERO,
            correlation_id: None,
        }
    }

    /// Set the job id, which a real context carries across every attempt.
    #[must_use]
    pub fn with_job_id(mut self, job_id: Uuid) -> Self {
        self.job_id = job_id;
        self
    }

    /// Set how often the job was deferred, for a handler that caps deferrals.
    #[must_use]
    pub fn with_deferrals(mut self, deferrals: u32) -> Self {
        self.deferrals = deferrals;
        self
    }

    /// Set the broker priority this delivery arrived with.
    #[must_use]
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }

    /// Set the time since first enqueue, for a handler that drops stale work.
    #[must_use]
    pub fn with_age(mut self, age: Duration) -> Self {
        self.age = age;
        self
    }

    /// Set the correlation id the job was enqueued with.
    #[must_use]
    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{Greet, TestQueues};

    /// The whole point of [`JobContext::new`]: a handler whose behaviour depends
    /// on the context is testable by calling it, with no backend, no worker and
    /// no delivery in sight.
    struct GiveUpWhenStale;

    #[async_trait]
    impl JobHandler for GiveUpWhenStale {
        type Job = Greet;

        async fn handle(&self, _job: Greet, ctx: JobContext) -> Result<(), JobError> {
            if ctx.deferrals >= 3 {
                return Err(JobError::fatal_msg("deferred too often"));
            }
            if ctx.age > Duration::from_secs(60) {
                return Err(JobError::fatal_msg("too old to be worth sending"));
            }
            Err(JobError::deferred(Duration::from_secs(30)))
        }
    }

    #[tokio::test]
    async fn a_handler_is_testable_without_a_backend() {
        let handler = GiveUpWhenStale;
        let job = Greet::new("world");

        let fresh = JobContext::new::<Greet>(1, 3);
        assert!(matches!(
            handler.handle(job.clone(), fresh).await,
            Err(JobError::Deferred { .. })
        ));

        let deferred_out = JobContext::new::<Greet>(1, 3).with_deferrals(3);
        assert!(matches!(
            handler.handle(job.clone(), deferred_out).await,
            Err(JobError::Fatal(_))
        ));

        let stale = JobContext::new::<Greet>(1, 3).with_age(Duration::from_secs(61));
        assert!(matches!(
            handler.handle(job, stale).await,
            Err(JobError::Fatal(_))
        ));
    }

    #[test]
    fn the_constructor_fills_job_type_and_queue_from_the_job() {
        let ctx = JobContext::new::<Greet>(2, 5);
        assert_eq!(ctx.job_type, Greet::NAME);
        assert_eq!(ctx.queue, TestQueues::Alpha.name());
        assert_eq!(ctx.attempt, 2);
        assert_eq!(ctx.max_attempts, 5);
        assert!(!ctx.is_last_attempt());
        // Everything optional starts neutral.
        assert_eq!(ctx.deferrals, 0);
        assert_eq!(ctx.priority, 0);
        assert_eq!(ctx.age, Duration::ZERO);
        assert_eq!(ctx.correlation_id, None);
    }

    #[test]
    fn the_builders_set_exactly_what_they_name() {
        let id = Uuid::new_v4();
        let ctx = JobContext::new::<Greet>(3, 3)
            .with_job_id(id)
            .with_deferrals(7)
            .with_priority(10)
            .with_age(Duration::from_secs(90))
            .with_correlation_id("req-42");
        assert_eq!(ctx.job_id, id);
        assert_eq!(ctx.deferrals, 7);
        assert_eq!(ctx.priority, 10);
        assert_eq!(ctx.age, Duration::from_secs(90));
        assert_eq!(ctx.correlation_id.as_deref(), Some("req-42"));
        assert!(ctx.is_last_attempt());
    }
}
