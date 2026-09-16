//! The [`Producer`]: type-safe publishing into a queue set.

use std::{sync::Arc, time::Duration};

use crate::{backend::Backend, envelope::Envelope, error::Result, job::Job, queue::QueueSet};

/// When and how a job is published: the options [`Producer::enqueue_with`] takes.
///
/// Default is "publish now, no correlation id", which is exactly
/// [`Producer::enqueue`]. The other constructors and setters compose:
///
/// ```
/// # use std::time::Duration;
/// # use queuey_core::EnqueueOptions;
/// // Hold for 30s, then release ahead of the backlog, tagged with a trace id.
/// let options = EnqueueOptions::deferred(Duration::from_secs(30)).correlation_id("trace-abc");
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct EnqueueOptions {
    correlation_id: Option<String>,
    schedule: Schedule,
}

/// Private because the three constructors below are the whole vocabulary; keeping it
/// unexported leaves room to add scheduling shapes without a breaking change.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
enum Schedule {
    /// Publish immediately.
    #[default]
    Now,
    /// Hold for this long, then join the back of the queue (priority `0`).
    After(Duration),
    /// Hold for this long, then return at the queue's top priority.
    Deferred(Duration),
}

impl EnqueueOptions {
    /// Publish as soon as the call reaches the broker.
    #[must_use]
    pub fn now() -> Self {
        Self::default()
    }

    /// Publish after `delay`, at normal priority. See [`Producer::enqueue_after`].
    #[must_use]
    pub fn after(delay: Duration) -> Self {
        Self {
            schedule: Schedule::After(delay),
            ..Self::default()
        }
    }

    /// Hold for `delay`, then release at the queue's top priority. See
    /// [`Producer::defer`].
    #[must_use]
    pub fn deferred(delay: Duration) -> Self {
        Self {
            schedule: Schedule::Deferred(delay),
            ..Self::default()
        }
    }

    /// Tag the envelope with a correlation id, carried through every retry and
    /// deferral and handed to the handler as [`crate::JobContext::correlation_id`].
    #[must_use]
    pub fn correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }
}

/// Type-safe publisher. `Q` pins the producer to one queue set so a job from a
/// different application cannot be enqueued by accident.
pub struct Producer<Q: QueueSet, B: Backend> {
    backend: Arc<B>,
    _q: std::marker::PhantomData<fn(Q)>,
}

impl<Q: QueueSet, B: Backend> Clone for Producer<Q, B> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            _q: std::marker::PhantomData,
        }
    }
}

impl<Q: QueueSet, B: Backend> Producer<Q, B> {
    /// Create a producer and declare all queues in `Q`.
    pub async fn new(backend: Arc<B>) -> Result<Self> {
        let configs: Vec<_> = Q::all().iter().map(|q| q.config()).collect();
        backend.declare(&configs).await?;
        Ok(Self::new_undeclared(backend))
    }

    /// Create a producer without declaring queues (they must already exist).
    pub fn new_undeclared(backend: Arc<B>) -> Self {
        Self {
            backend,
            _q: std::marker::PhantomData,
        }
    }

    /// Publish `job` to its statically-known queue. Returns the job id.
    pub async fn enqueue<J: Job<Queue = Q>>(&self, job: &J) -> Result<uuid::Uuid> {
        self.enqueue_with(job, EnqueueOptions::now()).await
    }

    /// Publish `job` with explicit [`EnqueueOptions`]: the form the other three
    /// methods are shorthands for, and the one that takes a correlation id.
    pub async fn enqueue_with<J: Job<Queue = Q>>(
        &self,
        job: &J,
        options: EnqueueOptions,
    ) -> Result<uuid::Uuid> {
        let mut env = Envelope::new(job)?;
        env.correlation_id = options.correlation_id;

        match options.schedule {
            Schedule::Now => self.backend.publish(&env, None).await?,
            Schedule::After(delay) => self.backend.publish(&env, Some(delay)).await?,
            Schedule::Deferred(delay) => {
                env.priority = J::QUEUE.config().max_priority.unwrap_or(0);
                self.backend.defer(&env, delay).await?;
            }
        }
        Ok(env.job_id)
    }

    /// Publish `job`, to become visible after `delay`.
    ///
    /// The plain delay: the job waits, then joins the back of the queue like any other
    /// message (priority `0`). On RabbitMQ it waits in the hold queue for its delay,
    /// exactly as a retry does, so different delays never block each other. Use
    /// [`Producer::defer`] when the job must come back *ahead* of the backlog.
    ///
    /// On RabbitMQ this needs the queue to have been declared through the same backend
    /// (which [`Producer::new`] does and [`Producer::new_undeclared`] does not), and the
    /// delay is capped at about 24.8 days; see the backend's docs.
    pub async fn enqueue_after<J: Job<Queue = Q>>(
        &self,
        job: &J,
        delay: Duration,
    ) -> Result<uuid::Uuid> {
        self.enqueue_with(job, EnqueueOptions::after(delay)).await
    }

    /// Publish `job` into a hold that releases it after `delay`, at the front of the
    /// queue. Returns the job id.
    ///
    /// The envelope is a first-attempt one (`attempt = 1`, `deferrals = 0`) carrying
    /// the highest priority its queue supports
    /// ([`crate::QueueConfig::max_priority`], `0` when the queue is not a priority
    /// queue), so when the delay is up it runs before everything that was enqueued
    /// normally in the meantime. The producer-side twin of a handler returning
    /// [`crate::JobError::Deferred`].
    ///
    /// Contrast with [`Producer::enqueue_after`]: that returns at priority `0`, behind
    /// the backlog; this one returns at the top. Both wait in a hold per delay, so
    /// equal delays drain strictly in order and different delays never block each
    /// other.
    pub async fn defer<J: Job<Queue = Q>>(&self, job: &J, delay: Duration) -> Result<uuid::Uuid> {
        self.enqueue_with(job, EnqueueOptions::deferred(delay))
            .await
    }

    /// The backend this producer publishes through.
    pub fn backend(&self) -> &Arc<B> {
        &self.backend
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        memory::MemoryBackend,
        test_support::{Greet, Nudge, TestQueues},
    };

    async fn producer() -> (Arc<MemoryBackend>, Producer<TestQueues, MemoryBackend>) {
        let backend = Arc::new(MemoryBackend::new());
        let producer = Producer::<TestQueues, _>::new(backend.clone())
            .await
            .unwrap();
        (backend, producer)
    }

    /// Read the one envelope waiting on `queue`, without settling it.
    async fn peek(backend: &MemoryBackend, queue: &TestQueues) -> Envelope {
        let mut stream = backend.consume(&queue.config()).await.unwrap();
        let delivery = futures::StreamExt::next(&mut stream)
            .await
            .unwrap()
            .unwrap();
        let envelope = delivery.envelope().clone();
        delivery.ack().await.unwrap();
        envelope
    }

    #[tokio::test(start_paused = true)]
    async fn enqueue_with_carries_the_correlation_id() {
        let (backend, producer) = producer().await;
        producer
            .enqueue_with(
                &Greet::new("traced"),
                EnqueueOptions::now().correlation_id("trace-abc"),
            )
            .await
            .unwrap();

        let envelope = peek(&backend, &TestQueues::Alpha).await;
        assert_eq!(envelope.correlation_id.as_deref(), Some("trace-abc"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_plain_enqueue_has_no_correlation_id() {
        let (backend, producer) = producer().await;
        producer.enqueue(&Greet::new("plain")).await.unwrap();

        let envelope = peek(&backend, &TestQueues::Alpha).await;
        assert_eq!(envelope.correlation_id, None);
    }

    #[tokio::test(start_paused = true)]
    async fn options_schedule_a_delay_and_a_deferral_like_the_shorthands() {
        let (backend, producer) = producer().await;
        producer
            .enqueue_with(
                &Greet::new("later"),
                EnqueueOptions::after(Duration::from_secs(10)).correlation_id("delayed"),
            )
            .await
            .unwrap();
        producer
            .enqueue_with(
                &Greet::new("ahead"),
                EnqueueOptions::deferred(Duration::from_secs(10)).correlation_id("deferred"),
            )
            .await
            .unwrap();

        assert_eq!(backend.pending("test.alpha"), 0, "both are held");
        // `deferred()` counts only the hold a deferral makes; a delayed publish waits
        // elsewhere and shows up in neither counter until it is released.
        assert_eq!(backend.deferred("test.alpha"), 1);

        tokio::time::sleep(Duration::from_secs(11)).await;
        assert_eq!(backend.pending("test.alpha"), 2);

        // The deferral comes back at the queue's top priority, so it is served first.
        let first = peek(&backend, &TestQueues::Alpha).await;
        assert_eq!(first.correlation_id.as_deref(), Some("deferred"));
        assert_eq!(first.priority, 10);

        let second = peek(&backend, &TestQueues::Alpha).await;
        assert_eq!(second.correlation_id.as_deref(), Some("delayed"));
        assert_eq!(second.priority, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn defer_holds_the_job_and_returns_it_at_the_queue_top_priority() {
        let (backend, producer) = producer().await;
        // Two normal jobs are already waiting.
        producer.enqueue(&Greet::new("backlog")).await.unwrap();

        let id = producer
            .defer(&Greet::new("held"), Duration::from_secs(30))
            .await
            .unwrap();

        assert_eq!(backend.deferred("test.alpha"), 1);
        assert_eq!(backend.pending("test.alpha"), 1, "only the backlog so far");

        tokio::time::sleep(Duration::from_secs(29)).await;
        assert_eq!(backend.pending("test.alpha"), 1);

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(backend.deferred("test.alpha"), 0);
        assert_eq!(backend.pending("test.alpha"), 2);

        let mut stream = backend.consume(&TestQueues::Alpha.config()).await.unwrap();
        let first = futures::StreamExt::next(&mut stream)
            .await
            .unwrap()
            .unwrap();
        let envelope = first.envelope().clone();
        assert_eq!(envelope.job_id, id, "the deferred job is served first");
        // Alpha keeps the default ten priority levels.
        assert_eq!(envelope.priority, 10);
        assert_eq!(envelope.deferrals, 0, "the producer never deferred it once");
        assert_eq!(envelope.attempt, 1);
        first.ack().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn defer_on_a_queue_without_priorities_uses_zero() {
        let (backend, producer) = producer().await;
        producer
            .defer(&Nudge { id: 1 }, Duration::from_secs(5))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(6)).await;
        let mut stream = backend.consume(&TestQueues::Gamma.config()).await.unwrap();
        let delivery = futures::StreamExt::next(&mut stream)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivery.envelope().priority, 0);
        assert_eq!(delivery.envelope().deferrals, 0);
        delivery.ack().await.unwrap();
    }

    #[tokio::test]
    async fn defer_on_a_closed_backend_fails() {
        let (backend, producer) = producer().await;
        backend.close().await.unwrap();
        assert!(matches!(
            producer
                .defer(&Greet::new("x"), Duration::from_secs(1))
                .await,
            Err(crate::error::Error::ShutDown)
        ));
    }
}
