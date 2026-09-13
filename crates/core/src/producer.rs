//! The [`Producer`]: type-safe publishing into a queue set.

use std::{sync::Arc, time::Duration};

use crate::{backend::Backend, envelope::Envelope, error::Result, job::Job, queue::QueueSet};

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
        self.enqueue_delayed(job, None).await
    }

    /// Publish `job`, to become visible after `delay`.
    ///
    /// The plain delay: the job waits, then joins the back of the queue like any other
    /// message (priority `0`). On RabbitMQ every delayed publish of a queue shares one
    /// wait queue, so a message with a long delay sitting at its head holds up shorter
    /// ones behind it. Use [`Producer::defer`] when the job must come back *ahead* of
    /// the backlog, or when many different delays are in play.
    pub async fn enqueue_after<J: Job<Queue = Q>>(
        &self,
        job: &J,
        delay: Duration,
    ) -> Result<uuid::Uuid> {
        self.enqueue_delayed(job, Some(delay)).await
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
    /// Contrast with [`Producer::enqueue_after`]: that shares one wait queue per queue
    /// (head-of-line blocking between different delays on RabbitMQ) and returns at
    /// priority `0`; this one is held per delay (equal delays drain strictly in order)
    /// and returns at the top.
    pub async fn defer<J: Job<Queue = Q>>(&self, job: &J, delay: Duration) -> Result<uuid::Uuid> {
        let mut env = Envelope::new(job)?;
        env.priority = J::QUEUE.config().max_priority.unwrap_or(0);
        self.backend.defer(&env, delay).await?;
        Ok(env.job_id)
    }

    async fn enqueue_delayed<J: Job<Queue = Q>>(
        &self,
        job: &J,
        delay: Option<Duration>,
    ) -> Result<uuid::Uuid> {
        let env = Envelope::new(job)?;
        self.backend.publish(&env, delay).await?;
        Ok(env.job_id)
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
