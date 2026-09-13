//! Worker runtime.
//!
//! Contract:
//! * `Worker::<Q, B>::builder(backend)` -> `WorkerBuilder`.
//! * `builder.handler(h)` registers a `JobHandler` whose `Job::Queue == Q`;
//!   duplicate `Job::NAME` -> `Error::DuplicateHandler` at `build()`.
//! * `builder.queues(&[Q::A, Q::B])` restricts consumption to a subset (default: all).
//! * `builder.concurrency(n)` caps in-flight jobs per worker process (default: sum of prefetch).
//! * `builder.close_backend_on_shutdown(bool)` (default `false`) closes the shared backend
//!   when `run()` returns.
//! * `builder.build().await?` declares queues and returns a `Worker`.
//! * `worker.run()` consumes until `WorkerHandle::shutdown()` is called; graceful:
//!   stops the consumers first, then processes everything already pulled from the broker,
//!   then waits for the in-flight jobs. The backend is left open unless
//!   `close_backend_on_shutdown(true)` was set.
//! * `worker.handle()` -> `WorkerHandle` (Clone) for shutdown from elsewhere, and for
//!   `WorkerHandle::settle_failures()`.
//!
//! Per-delivery algorithm:
//! 1. Look up handler by `envelope.job_type`; if none -> `dead_letter("no handler")`.
//! 2. Decode payload; on failure -> `dead_letter("decode error")`.
//! 3. Build `JobContext`, run handler with `tokio::time::timeout` if configured.
//! 4. Ok -> `ack`.
//! 5. `JobError::Fatal` -> `dead_letter`.
//! 6. `JobError::Deferred{delay, reason}` -> logged at `INFO`, then
//!    `defer(env.deferred(priority), delay)` where `priority` is the job queue's
//!    `max_priority.unwrap_or(0)`. Nothing failed: `attempt` is unchanged, the retry
//!    policy is never consulted, only `deferrals` grows.
//! 7. `JobError::Retryable` -> policy = job override or queue default;
//!    `policy.decide(attempt)`; `Retry{delay}` -> `retry(env.next_attempt(), delay)`,
//!    `GiveUp` -> `dead_letter("max attempts")`. `next_attempt` puts `priority` back to
//!    `0`, so a job that deferred earlier does not keep jumping the backlog on retries.
//! 8. Handler panics are caught (`catch_unwind` via spawned task JoinError) and treated as Retryable.
//!
//! Every step emits `tracing` events with job_id / job_type / attempt fields.

use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use futures::StreamExt;
use tokio::{
    sync::{Semaphore, mpsc, watch},
    task::JoinSet,
};
use tracing::{Instrument, debug, error, info, warn};

use crate::{
    backend::{Backend, Delivery, DeliveryStream},
    envelope::{Envelope, now_ms},
    error::{Error, JobError, Result},
    handler::{JobContext, JobHandler},
    job::Job,
    queue::{QueueConfig, QueueSet},
    retry::{RetryDecision, RetryPolicy},
};

/// What happened while running one job.
#[derive(Debug)]
enum JobOutcome {
    /// The handler returned `Ok(())`.
    Success,
    /// The payload could not be deserialized into the handler's job type.
    Decode(String),
    /// Transient failure (including panics and timeouts); consult the retry policy.
    Retryable(String),
    /// Permanent failure; dead-letter without consulting the retry policy.
    Fatal(String),
    /// Not a failure: hold the job for `delay` and run it again, attempt unchanged.
    Deferred {
        /// How long the job asked to wait.
        delay: Duration,
        /// Why it was deferred; for logs only.
        reason: String,
    },
}

/// Type-erased [`JobHandler`], so handlers for different job types can live in one map.
#[async_trait]
trait ErasedHandler: Send + Sync + 'static {
    /// Effective policy: the job override if there is one, else the queue default.
    fn policy(&self) -> RetryPolicy;

    /// Priority a deferred envelope of this job is republished with: the highest level
    /// its queue supports, or `0` when the queue is not a priority queue.
    fn defer_priority(&self) -> u8;

    /// Decode and run the job, never panicking and never returning an error type.
    async fn run(&self, envelope: &Envelope, timeout: Option<Duration>) -> JobOutcome;
}

/// Adapter from a concrete [`JobHandler`] to [`ErasedHandler`].
struct ErasedJobHandler<H: JobHandler> {
    handler: Arc<H>,
}

#[async_trait]
impl<H: JobHandler> ErasedHandler for ErasedJobHandler<H> {
    fn policy(&self) -> RetryPolicy {
        <H::Job as Job>::retry_policy().unwrap_or_else(|| <H::Job as Job>::QUEUE.config().retry)
    }

    fn defer_priority(&self) -> u8 {
        <H::Job as Job>::QUEUE.config().max_priority.unwrap_or(0)
    }

    async fn run(&self, envelope: &Envelope, timeout: Option<Duration>) -> JobOutcome {
        let job = match envelope.decode::<H::Job>() {
            Ok(job) => job,
            Err(e) => return JobOutcome::Decode(e.to_string()),
        };

        let ctx = JobContext {
            job_id: envelope.job_id,
            job_type: <H::Job as Job>::NAME,
            queue: <H::Job as Job>::QUEUE.name(),
            attempt: envelope.attempt,
            max_attempts: self.policy().max_attempts,
            deferrals: envelope.deferrals,
            priority: envelope.priority,
            age: Duration::from_millis(now_ms().saturating_sub(envelope.enqueued_at_ms)),
        };

        // Spawning turns a handler panic into a `JoinError` instead of unwinding the
        // worker, and gives us something to abort when the job times out.
        let handler = self.handler.clone();
        let mut task = tokio::spawn(async move { handler.handle(job, ctx).await });

        let joined = match timeout {
            Some(limit) => match tokio::time::timeout(limit, &mut task).await {
                Ok(joined) => joined,
                Err(_) => {
                    task.abort();
                    // Wait for the cancellation to land: the concurrency permit is
                    // released when this function returns, so the job must be gone by
                    // then or the cap could be exceeded.
                    let _ = task.await;
                    return JobOutcome::Retryable(format!("job timed out after {limit:?}"));
                }
            },
            None => (&mut task).await,
        };

        match joined {
            Ok(Ok(())) => JobOutcome::Success,
            Ok(Err(JobError::Retryable(e))) => JobOutcome::Retryable(e.to_string()),
            Ok(Err(JobError::Fatal(e))) => JobOutcome::Fatal(e.to_string()),
            Ok(Err(JobError::Deferred { delay, reason })) => JobOutcome::Deferred { delay, reason },
            Err(e) if e.is_panic() => JobOutcome::Retryable("handler panicked".to_owned()),
            Err(e) => JobOutcome::Retryable(format!("handler task failed: {e}")),
        }
    }
}

type HandlerMap = HashMap<&'static str, Arc<dyn ErasedHandler>>;

/// Consumes one or more queues of `Q` and dispatches jobs to registered handlers.
///
/// Build one with [`Worker::builder`]; run it with [`Worker::run`].
pub struct Worker<Q: QueueSet, B: Backend> {
    backend: Arc<B>,
    handlers: Arc<HandlerMap>,
    queues: Vec<QueueConfig>,
    concurrency: usize,
    job_timeout: Option<Duration>,
    close_backend: bool,
    handle: WorkerHandle,
    _q: PhantomData<fn(Q)>,
}

impl<Q: QueueSet, B: Backend> std::fmt::Debug for Worker<Q, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Worker")
            .field(
                "queues",
                &self.queues.iter().map(|c| &c.name).collect::<Vec<_>>(),
            )
            .field("handlers", &self.handlers.keys().collect::<Vec<_>>())
            .field("concurrency", &self.concurrency)
            .field("job_timeout", &self.job_timeout)
            .field("close_backend_on_shutdown", &self.close_backend)
            .finish()
    }
}

impl<Q: QueueSet, B: Backend> Worker<Q, B> {
    /// Start configuring a worker for queue set `Q` on `backend`.
    pub fn builder(backend: Arc<B>) -> WorkerBuilder<Q, B> {
        WorkerBuilder {
            backend,
            handlers: Vec::new(),
            queues: None,
            concurrency: None,
            job_timeout: None,
            close_backend: false,
        }
    }

    /// A clonable handle that can shut this worker down from anywhere.
    pub fn handle(&self) -> WorkerHandle {
        self.handle.clone()
    }

    /// The queues this worker consumes.
    pub fn queues(&self) -> &[QueueConfig] {
        &self.queues
    }

    /// Maximum number of jobs this worker runs at the same time.
    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    /// Consume and dispatch until [`WorkerHandle::shutdown`] is called.
    ///
    /// Shutdown is graceful, and in this order:
    ///
    /// 1. the consumers stop pulling new deliveries from the broker;
    /// 2. everything they already pulled is processed normally (a delivery that has
    ///    been handed over is never dropped unsettled);
    /// 3. the jobs already running are awaited.
    ///
    /// The backend is *not* closed unless
    /// [`WorkerBuilder::close_backend_on_shutdown`] was set: it is usually an
    /// `Arc` shared with a [`crate::Producer`] that outlives the worker.
    ///
    /// Returns [`Error::ConsumerStopped`] if a consumer stream ends on its own (which
    /// means the backend went away), or the first backend error seen on a stream.
    pub async fn run(self) -> Result<()> {
        let Worker {
            backend,
            handlers,
            queues,
            concurrency,
            job_timeout,
            close_backend,
            handle,
            ..
        } = self;

        info!(
            queues = queues.len(),
            concurrency,
            handlers = handlers.len(),
            "worker starting"
        );

        let mut outcome: Result<()> = Ok(());
        let (tx, mut rx) = mpsc::channel::<ConsumerEvent>(queues.len().max(1));
        // Tells the consumer tasks to stop pulling. Separate from the user-facing
        // shutdown signal because every exit from the dispatch loop stops them.
        let (stop_tx, stop_rx) = watch::channel(false);
        let mut consumers = JoinSet::new();
        for config in &queues {
            match backend.consume(config).await {
                Ok(stream) => {
                    consumers.spawn(consume_into(
                        stream,
                        tx.clone(),
                        config.name.clone(),
                        stop_rx.clone(),
                    ));
                }
                Err(e) => {
                    error!(queue = %config.name, error = %e, "failed to start consumer");
                    outcome = Err(e);
                    break;
                }
            }
        }
        drop(tx);
        drop(stop_rx);

        let permits = Arc::new(Semaphore::new(concurrency));
        let mut in_flight: JoinSet<()> = JoinSet::new();
        let mut shutdown = handle.subscribe();
        let settle_failures = handle.settle_failures.clone();

        while outcome.is_ok() {
            if *shutdown.borrow_and_update() {
                break;
            }

            // Take a concurrency slot before pulling, so a delivery is never held
            // while we wait for capacity.
            let permit = tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                permit = permits.clone().acquire_owned() => match permit {
                    Ok(permit) => permit,
                    Err(_) => break,
                },
            };

            let event = tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                event = rx.recv() => event,
            };

            match event {
                // Every consumer task is gone.
                None => break,
                Some(ConsumerEvent::Ended(queue)) => {
                    error!(%queue, "consumer stream ended unexpectedly");
                    outcome = Err(Error::ConsumerStopped(queue));
                }
                Some(ConsumerEvent::Failed(e)) => {
                    error!(error = %e, "consumer stream failed");
                    outcome = Err(e);
                }
                Some(ConsumerEvent::Delivery(delivery)) => {
                    // Reap finished jobs so the JoinSet does not grow without bound.
                    while in_flight.try_join_next().is_some() {}
                    in_flight.spawn(process(
                        delivery,
                        handlers.clone(),
                        job_timeout,
                        settle_failures.clone(),
                        permit,
                    ));
                }
            }
        }

        // Stop the consumers *before* the channel is drained, so nothing else can be
        // pulled from a stream, and drain only afterwards: a delivery that reached the
        // channel has already left the broker's hands and must still be settled.
        debug!("worker draining");
        stop_tx.send_replace(true);

        loop {
            let Ok(permit) = permits.clone().acquire_owned().await else {
                break;
            };
            match rx.recv().await {
                // Every consumer task has finished and the channel is empty.
                None => break,
                Some(ConsumerEvent::Delivery(delivery)) => {
                    while in_flight.try_join_next().is_some() {}
                    in_flight.spawn(process(
                        delivery,
                        handlers.clone(),
                        job_timeout,
                        settle_failures.clone(),
                        permit,
                    ));
                }
                Some(ConsumerEvent::Failed(e)) => {
                    error!(error = %e, "consumer stream failed while draining");
                    if outcome.is_ok() {
                        outcome = Err(e);
                    }
                }
                Some(ConsumerEvent::Ended(queue)) => {
                    debug!(%queue, "consumer stream ended while draining");
                }
            }
        }
        while consumers.join_next().await.is_some() {}
        while in_flight.join_next().await.is_some() {}

        if close_backend && let Err(e) = backend.close().await {
            warn!(error = %e, "backend close failed");
            if outcome.is_ok() {
                outcome = Err(e);
            }
        }
        info!(
            settle_failures = settle_failures.load(Ordering::Relaxed),
            "worker stopped"
        );
        outcome
    }
}

/// Forwards one queue's deliveries into the dispatch channel until told to stop.
///
/// Pulling and forwarding are inseparable: once an item has been taken off the
/// stream it is always sent, so no delivery is dropped unsettled at shutdown.
async fn consume_into(
    mut stream: DeliveryStream,
    tx: mpsc::Sender<ConsumerEvent>,
    queue: String,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        if *stop.borrow_and_update() {
            return;
        }
        let item = tokio::select! {
            biased;
            _ = stop.changed() => return,
            item = stream.next() => item,
        };
        let Some(item) = item else { break };
        let event = match item {
            Ok(delivery) => ConsumerEvent::Delivery(delivery),
            Err(e) => ConsumerEvent::Failed(e),
        };
        if tx.send(event).await.is_err() {
            return;
        }
    }
    let _ = tx.send(ConsumerEvent::Ended(queue)).await;
}

/// One message from a per-queue consumer task to the dispatch loop.
enum ConsumerEvent {
    Delivery(Box<dyn Delivery>),
    Failed(Error),
    Ended(String),
}

/// Runs one delivery inside a span carrying the job identity.
async fn process(
    delivery: Box<dyn Delivery>,
    handlers: Arc<HandlerMap>,
    timeout: Option<Duration>,
    settle_failures: Arc<AtomicU64>,
    _permit: tokio::sync::OwnedSemaphorePermit,
) {
    let envelope = delivery.envelope().clone();
    let span = tracing::info_span!(
        "job",
        job_id = %envelope.job_id,
        job_type = %envelope.job_type,
        queue = %envelope.queue,
        attempt = envelope.attempt,
    );
    dispatch(delivery, envelope, handlers, timeout, &settle_failures)
        .instrument(span)
        .await;
}

async fn dispatch(
    delivery: Box<dyn Delivery>,
    envelope: Envelope,
    handlers: Arc<HandlerMap>,
    timeout: Option<Duration>,
    failures: &AtomicU64,
) {
    let Some(handler) = handlers.get(envelope.job_type.as_str()).cloned() else {
        warn!("no handler registered; dead-lettering");
        settle(
            delivery
                .dead_letter(&format!("no handler for job type `{}`", envelope.job_type))
                .await,
            "dead_letter",
            failures,
        );
        return;
    };

    debug!("running job");
    match handler.run(&envelope, timeout).await {
        JobOutcome::Success => {
            info!("job succeeded");
            settle(delivery.ack().await, "ack", failures);
        }
        JobOutcome::Decode(e) => {
            error!(error = %e, "payload did not match the handler's job type");
            settle(
                delivery.dead_letter(&format!("decode error: {e}")).await,
                "dead_letter",
                failures,
            );
        }
        JobOutcome::Fatal(e) => {
            error!(error = %e, "job failed fatally");
            settle(
                delivery.dead_letter(&format!("fatal error: {e}")).await,
                "dead_letter",
                failures,
            );
        }
        JobOutcome::Deferred { delay, reason } => {
            // Nothing failed: the attempt counter stays put and the retry policy is
            // never consulted. Only `deferrals` grows, and the envelope comes back at
            // the front of its queue.
            let priority = handler.defer_priority();
            info!(?delay, deferrals = envelope.deferrals + 1, %reason, "job deferred");
            settle(
                delivery.defer(envelope.deferred(priority), delay).await,
                "defer",
                failures,
            );
        }
        JobOutcome::Retryable(e) => {
            let policy = handler.policy();
            match policy.decide(envelope.attempt) {
                RetryDecision::Retry { delay } => {
                    warn!(error = %e, ?delay, next_attempt = envelope.attempt + 1, "job failed; retrying");
                    settle(
                        delivery.retry(envelope.next_attempt(), delay).await,
                        "retry",
                        failures,
                    );
                }
                RetryDecision::GiveUp => {
                    error!(error = %e, max_attempts = policy.max_attempts, "job failed; giving up");
                    let reason = format!(
                        "max attempts ({}) exhausted after attempt {}: {e}",
                        policy.max_attempts, envelope.attempt
                    );
                    settle(delivery.dead_letter(&reason).await, "dead_letter", failures);
                }
            }
        }
    }
}

/// Log and count (but never propagate) a failure to settle a delivery.
///
/// A settle failure means the broker still owns the message: it will be redelivered
/// once this consumer's channel or connection goes away, so the count is a signal of
/// duplicate work ahead rather than of lost work.
fn settle(result: Result<()>, what: &'static str, failures: &AtomicU64) {
    if let Err(e) = result {
        failures.fetch_add(1, Ordering::Relaxed);
        error!(error = %e, operation = what, "failed to settle delivery");
    }
}

/// Configures a [`Worker`]. Created by [`Worker::builder`].
pub struct WorkerBuilder<Q: QueueSet, B: Backend> {
    backend: Arc<B>,
    handlers: Vec<(&'static str, Arc<dyn ErasedHandler>)>,
    queues: Option<Vec<Q>>,
    concurrency: Option<usize>,
    job_timeout: Option<Duration>,
    close_backend: bool,
}

impl<Q: QueueSet, B: Backend> WorkerBuilder<Q, B> {
    /// Register a handler.
    ///
    /// The `H::Job: Job<Queue = Q>` bound is what makes the worker type-safe: a handler
    /// for a job belonging to another queue set does not compile.
    pub fn handler<H>(mut self, handler: H) -> Self
    where
        H: JobHandler,
        H::Job: Job<Queue = Q>,
    {
        self.handlers.push((
            <H::Job as Job>::NAME,
            Arc::new(ErasedJobHandler {
                handler: Arc::new(handler),
            }),
        ));
        self
    }

    /// Consume only these queues instead of every queue in `Q`. Duplicates are ignored.
    pub fn queues(mut self, queues: &[Q]) -> Self {
        self.queues = Some(queues.to_vec());
        self
    }

    /// Cap the number of jobs running at the same time.
    ///
    /// Defaults to the sum of the prefetch of the consumed queues. `0` is treated as `1`.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = Some(concurrency);
        self
    }

    /// Abort a handler that runs longer than `timeout` and treat it as a retryable failure.
    pub fn job_timeout(mut self, timeout: Duration) -> Self {
        self.job_timeout = Some(timeout);
        self
    }

    /// Close the backend when [`Worker::run`] returns. Defaults to `false`.
    ///
    /// The backend is an `Arc` that is normally shared with a [`crate::Producer`] (and
    /// possibly other workers), so closing it is the caller's decision: a worker that
    /// closed it on its own would take those down with it. Turn this on for a process
    /// whose only job is to run this worker, or call `backend.close()` yourself once
    /// `run()` has returned.
    pub fn close_backend_on_shutdown(mut self, close: bool) -> Self {
        self.close_backend = close;
        self
    }

    /// Declare the queues and produce the [`Worker`].
    ///
    /// Fails with [`Error::DuplicateHandler`] if two handlers claim the same `Job::NAME`.
    pub async fn build(self) -> Result<Worker<Q, B>> {
        let mut handlers: HandlerMap = HashMap::with_capacity(self.handlers.len());
        for (name, handler) in self.handlers {
            if handlers.insert(name, handler).is_some() {
                return Err(Error::DuplicateHandler(name.to_owned()));
            }
        }

        let selected: Vec<Q> = self.queues.unwrap_or_else(|| Q::all().to_vec());
        let mut queues: Vec<QueueConfig> = Vec::with_capacity(selected.len());
        for queue in selected {
            let config = queue.config();
            if !queues.iter().any(|c| c.name == config.name) {
                queues.push(config);
            }
        }

        self.backend.declare(&queues).await?;

        let concurrency = self
            .concurrency
            .unwrap_or_else(|| queues.iter().map(|c| usize::from(c.prefetch)).sum())
            .max(1);

        Ok(Worker {
            backend: self.backend,
            handlers: Arc::new(handlers),
            queues,
            concurrency,
            job_timeout: self.job_timeout,
            close_backend: self.close_backend,
            handle: WorkerHandle::new(),
            _q: PhantomData,
        })
    }
}

/// Remote control for a running [`Worker`]. Cheap to clone and send across tasks.
#[derive(Clone)]
pub struct WorkerHandle {
    shutdown: Arc<watch::Sender<bool>>,
    settle_failures: Arc<AtomicU64>,
}

impl WorkerHandle {
    fn new() -> Self {
        Self {
            shutdown: Arc::new(watch::channel(false).0),
            settle_failures: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Ask the worker to stop. Returns immediately; [`Worker::run`] finishes the jobs
    /// that are already running before it returns.
    ///
    /// Calling this more than once, or before [`Worker::run`] starts, is fine.
    pub fn shutdown(&self) {
        self.shutdown.send_replace(true);
    }

    /// Whether [`WorkerHandle::shutdown`] has been called.
    pub fn is_shutdown(&self) -> bool {
        *self.shutdown.borrow()
    }

    /// How many times the worker failed to ack / retry / dead-letter a delivery.
    ///
    /// Each failure is also logged at `ERROR`. The job itself ran; only telling the
    /// broker about the outcome failed, so the message is still owned by the broker
    /// and will be redelivered. A non-zero (and especially a growing) count means
    /// duplicate processing, and usually a sick connection. Alert on it.
    pub fn settle_failures(&self) -> u64 {
        self.settle_failures.load(Ordering::Relaxed)
    }

    fn subscribe(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }
}

impl std::fmt::Debug for WorkerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerHandle")
            .field("shutdown", &self.is_shutdown())
            .field("settle_failures", &self.settle_failures())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        handler::FnHandler,
        memory::MemoryBackend,
        producer::Producer,
        test_support::{Greet, Nudge, Orphan, Ping, Stubborn, TestQueues},
    };
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::task::JoinHandle;

    const ALPHA: &str = "test.alpha";
    const BETA: &str = "test.beta";
    const GAMMA: &str = "test.gamma";

    /// Records what handlers saw, so assertions do not depend on logging.
    #[derive(Default)]
    struct Recorder {
        attempts: AtomicUsize,
        running: AtomicUsize,
        max_running: AtomicUsize,
    }

    impl Recorder {
        fn enter(&self) -> usize {
            let running = self.running.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_running.fetch_max(running, Ordering::SeqCst);
            self.attempts.fetch_add(1, Ordering::SeqCst) + 1
        }
        fn leave(&self) {
            self.running.fetch_sub(1, Ordering::SeqCst);
        }
        fn attempts(&self) -> usize {
            self.attempts.load(Ordering::SeqCst)
        }
        fn max_running(&self) -> usize {
            self.max_running.load(Ordering::SeqCst)
        }
    }

    fn backend() -> Arc<MemoryBackend> {
        Arc::new(MemoryBackend::new())
    }

    /// A [`MemoryBackend`] whose deliveries can never be settled, so the worker's
    /// settle-failure path can be exercised.
    struct Unsettleable(Arc<MemoryBackend>);

    #[async_trait]
    impl Backend for Unsettleable {
        async fn declare(&self, queues: &[QueueConfig]) -> Result<()> {
            self.0.declare(queues).await
        }

        async fn publish(&self, envelope: &Envelope, delay: Option<Duration>) -> Result<()> {
            self.0.publish(envelope, delay).await
        }

        async fn defer(&self, envelope: &Envelope, delay: Duration) -> Result<()> {
            self.0.defer(envelope, delay).await
        }

        async fn consume(&self, queue: &QueueConfig) -> Result<DeliveryStream> {
            let stream = self.0.consume(queue).await?;
            Ok(Box::pin(stream.map(|item| {
                item.map(|delivery| Box::new(NeverSettles(delivery)) as Box<dyn Delivery>)
            })))
        }

        async fn close(&self) -> Result<()> {
            self.0.close().await
        }
    }

    /// Every settle attempt fails; the inner delivery is dropped unsettled.
    struct NeverSettles(Box<dyn Delivery>);

    #[async_trait]
    impl Delivery for NeverSettles {
        fn envelope(&self) -> &Envelope {
            self.0.envelope()
        }

        async fn ack(self: Box<Self>) -> Result<()> {
            Err(Error::backend(std::io::Error::other("channel is gone")))
        }

        async fn dead_letter(self: Box<Self>, _reason: &str) -> Result<()> {
            Err(Error::backend(std::io::Error::other("channel is gone")))
        }

        async fn retry(self: Box<Self>, _next: Envelope, _delay: Duration) -> Result<()> {
            Err(Error::backend(std::io::Error::other("channel is gone")))
        }

        async fn defer(self: Box<Self>, _next: Envelope, _delay: Duration) -> Result<()> {
            Err(Error::backend(std::io::Error::other("channel is gone")))
        }
    }

    async fn producer(backend: &Arc<MemoryBackend>) -> Producer<TestQueues, MemoryBackend> {
        Producer::<TestQueues, _>::new(backend.clone())
            .await
            .unwrap()
    }

    fn start(worker: Worker<TestQueues, MemoryBackend>) -> (WorkerHandle, JoinHandle<Result<()>>) {
        let handle = worker.handle();
        (handle, tokio::spawn(worker.run()))
    }

    /// Polls `cond` while letting paused time (and therefore retry delays) advance.
    async fn wait_for(mut cond: impl FnMut() -> bool) {
        for _ in 0..1_000 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("condition was never met");
    }

    /// Polls `cond` without advancing the clock, for state that only needs other
    /// tasks to be given a turn (a consumer registering itself, say).
    async fn wait_for_tasks(mut cond: impl FnMut() -> bool) {
        for _ in 0..10_000 {
            if cond() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition was never met");
    }

    async fn stop(handle: WorkerHandle, task: JoinHandle<Result<()>>) -> Result<()> {
        handle.shutdown();
        task.await.expect("worker task panicked")
    }

    #[tokio::test(start_paused = true)]
    async fn successful_job_is_acked() {
        let backend = backend();
        let producer = producer(&backend).await;
        let seen = Arc::new(Recorder::default());

        let worker = {
            let seen = seen.clone();
            Worker::<TestQueues, _>::builder(backend.clone())
                .handler(FnHandler::<Greet, _>::new(
                    move |job: Greet, ctx: JobContext| {
                        let seen = seen.clone();
                        async move {
                            seen.enter();
                            seen.leave();
                            assert_eq!(job.name, "ada");
                            assert_eq!(ctx.attempt, 1);
                            assert_eq!(ctx.max_attempts, 3);
                            assert_eq!(ctx.job_type, Greet::NAME);
                            assert_eq!(ctx.queue, ALPHA);
                            Ok(())
                        }
                    },
                ))
                .build()
                .await
                .unwrap()
        };
        let (handle, task) = start(worker);

        let id = producer.enqueue(&Greet::new("ada")).await.unwrap();
        wait_for(|| backend.acked(ALPHA).len() == 1).await;

        assert_eq!(backend.acked(ALPHA)[0].job_id, id);
        assert!(backend.dead_letters(ALPHA).is_empty());
        assert_eq!(seen.attempts(), 1);
        assert_eq!(handle.settle_failures(), 0);
        stop(handle, task).await.unwrap();
        // The backend is shared with the producer, so the worker leaves it alone.
        assert!(!backend.is_closed());
    }

    #[tokio::test(start_paused = true)]
    async fn retryable_failure_is_retried_and_then_succeeds() {
        let backend = backend();
        let producer = producer(&backend).await;
        let seen = Arc::new(Recorder::default());

        let worker = {
            let seen = seen.clone();
            Worker::<TestQueues, _>::builder(backend.clone())
                .handler(FnHandler::<Greet, _>::new(
                    move |_job: Greet, ctx: JobContext| {
                        let seen = seen.clone();
                        async move {
                            let n = seen.enter();
                            seen.leave();
                            if n == 1 {
                                assert_eq!(ctx.attempt, 1);
                                assert!(!ctx.is_last_attempt());
                                Err(JobError::retryable_msg("flaky"))
                            } else {
                                assert_eq!(ctx.attempt, 2);
                                Ok(())
                            }
                        }
                    },
                ))
                .build()
                .await
                .unwrap()
        };
        let (handle, task) = start(worker);

        producer.enqueue(&Greet::new("ada")).await.unwrap();
        wait_for(|| seen.attempts() == 2).await;
        wait_for(|| backend.acked(ALPHA).len() == 2).await;

        // The first ack is the retried original, the second the successful attempt.
        let acked = backend.acked(ALPHA);
        assert_eq!(acked[0].attempt, 1);
        assert_eq!(acked[1].attempt, 2);
        assert_eq!(acked[0].job_id, acked[1].job_id);
        assert!(backend.dead_letters(ALPHA).is_empty());
        stop(handle, task).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn retryable_failure_dead_letters_once_attempts_are_exhausted() {
        let backend = backend();
        let producer = producer(&backend).await;
        let seen = Arc::new(Recorder::default());

        let worker = {
            let seen = seen.clone();
            Worker::<TestQueues, _>::builder(backend.clone())
                .handler(FnHandler::<Greet, _>::new(
                    move |_job: Greet, _ctx: JobContext| {
                        let seen = seen.clone();
                        async move {
                            seen.enter();
                            seen.leave();
                            Err(JobError::retryable_msg("always down"))
                        }
                    },
                ))
                .build()
                .await
                .unwrap()
        };
        let (handle, task) = start(worker);

        producer.enqueue(&Greet::new("ada")).await.unwrap();
        wait_for(|| !backend.dead_letters(ALPHA).is_empty()).await;

        // Queue policy is three attempts.
        assert_eq!(seen.attempts(), 3);
        let dead = backend.dead_letters(ALPHA);
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].0.attempt, 3);
        assert!(
            dead[0].1.contains("max attempts"),
            "reason was {:?}",
            dead[0].1
        );
        assert!(dead[0].1.contains("always down"));
        stop(handle, task).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn fatal_failure_dead_letters_immediately() {
        let backend = backend();
        let producer = producer(&backend).await;
        let seen = Arc::new(Recorder::default());

        let worker = {
            let seen = seen.clone();
            Worker::<TestQueues, _>::builder(backend.clone())
                .handler(FnHandler::<Greet, _>::new(
                    move |_job: Greet, _ctx: JobContext| {
                        let seen = seen.clone();
                        async move {
                            seen.enter();
                            seen.leave();
                            Err(JobError::fatal_msg("bad input"))
                        }
                    },
                ))
                .build()
                .await
                .unwrap()
        };
        let (handle, task) = start(worker);

        producer.enqueue(&Greet::new("ada")).await.unwrap();
        wait_for(|| !backend.dead_letters(ALPHA).is_empty()).await;

        // One attempt only, even though the queue allows three.
        assert_eq!(seen.attempts(), 1);
        let dead = backend.dead_letters(ALPHA);
        assert_eq!(dead[0].0.attempt, 1);
        assert!(dead[0].1.contains("fatal"), "reason was {:?}", dead[0].1);
        assert!(backend.acked(ALPHA).is_empty());
        stop(handle, task).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn job_retry_policy_overrides_the_queue_policy() {
        let backend = backend();
        let producer = producer(&backend).await;
        let seen = Arc::new(Recorder::default());

        let worker = {
            let seen = seen.clone();
            Worker::<TestQueues, _>::builder(backend.clone())
                .handler(FnHandler::<Stubborn, _>::new(
                    move |_job: Stubborn, ctx: JobContext| {
                        let seen = seen.clone();
                        async move {
                            seen.enter();
                            // The queue says one attempt, the job says two.
                            assert_eq!(ctx.max_attempts, 2);
                            seen.leave();
                            Err(JobError::retryable_msg("nope"))
                        }
                    },
                ))
                .build()
                .await
                .unwrap()
        };
        let (handle, task) = start(worker);

        producer.enqueue(&Stubborn { id: 1 }).await.unwrap();
        wait_for(|| !backend.dead_letters(BETA).is_empty()).await;

        assert_eq!(seen.attempts(), 2);
        assert!(backend.dead_letters(BETA)[0].1.contains("max attempts"));
        stop(handle, task).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn queue_policy_without_retries_dead_letters_on_first_failure() {
        let backend = backend();
        let producer = producer(&backend).await;
        let seen = Arc::new(Recorder::default());

        let worker = {
            let seen = seen.clone();
            Worker::<TestQueues, _>::builder(backend.clone())
                .handler(FnHandler::<Ping, _>::new(
                    move |_job: Ping, ctx: JobContext| {
                        let seen = seen.clone();
                        async move {
                            seen.enter();
                            assert_eq!(ctx.max_attempts, 1);
                            assert!(ctx.is_last_attempt());
                            seen.leave();
                            Err(JobError::retryable_msg("nope"))
                        }
                    },
                ))
                .build()
                .await
                .unwrap()
        };
        let (handle, task) = start(worker);

        producer.enqueue(&Ping { seq: 1 }).await.unwrap();
        wait_for(|| !backend.dead_letters(BETA).is_empty()).await;
        assert_eq!(seen.attempts(), 1);
        stop(handle, task).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn job_without_a_handler_is_dead_lettered() {
        let backend = backend();
        let producer = producer(&backend).await;

        let worker = Worker::<TestQueues, _>::builder(backend.clone())
            .handler(FnHandler::<Greet, _>::new(
                |_job: Greet, _ctx: JobContext| async move { Ok(()) },
            ))
            .build()
            .await
            .unwrap();
        let (handle, task) = start(worker);

        producer.enqueue(&Orphan { id: 9 }).await.unwrap();
        wait_for(|| !backend.dead_letters(ALPHA).is_empty()).await;

        let dead = backend.dead_letters(ALPHA);
        assert_eq!(dead[0].0.job_type, Orphan::NAME);
        assert!(
            dead[0].1.contains("no handler"),
            "reason was {:?}",
            dead[0].1
        );
        stop(handle, task).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn malformed_payload_is_dead_lettered() {
        let backend = backend();
        let seen = Arc::new(Recorder::default());

        let worker = {
            let seen = seen.clone();
            Worker::<TestQueues, _>::builder(backend.clone())
                .handler(FnHandler::<Greet, _>::new(
                    move |_job: Greet, _ctx: JobContext| {
                        let seen = seen.clone();
                        async move {
                            seen.enter();
                            seen.leave();
                            Ok(())
                        }
                    },
                ))
                .build()
                .await
                .unwrap()
        };
        let (handle, task) = start(worker);

        let mut envelope = Envelope::new(&Greet::new("ada")).unwrap();
        envelope.payload = serde_json::json!({ "not_a_name": 42 });
        backend.publish(&envelope, None).await.unwrap();

        wait_for(|| !backend.dead_letters(ALPHA).is_empty()).await;
        let dead = backend.dead_letters(ALPHA);
        assert!(
            dead[0].1.contains("decode error"),
            "reason was {:?}",
            dead[0].1
        );
        assert_eq!(
            seen.attempts(),
            0,
            "handler must not run on a decode failure"
        );
        stop(handle, task).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn panicking_handler_is_retried() {
        let backend = backend();
        let producer = producer(&backend).await;
        let seen = Arc::new(Recorder::default());

        let worker = {
            let seen = seen.clone();
            Worker::<TestQueues, _>::builder(backend.clone())
                .handler(FnHandler::<Greet, _>::new(
                    move |_job: Greet, _ctx: JobContext| {
                        let seen = seen.clone();
                        async move {
                            let n = seen.enter();
                            seen.leave();
                            if n == 1 {
                                panic!("handler exploded on purpose");
                            }
                            Ok(())
                        }
                    },
                ))
                .build()
                .await
                .unwrap()
        };
        let (handle, task) = start(worker);

        producer.enqueue(&Greet::new("ada")).await.unwrap();
        wait_for(|| seen.attempts() == 2).await;
        wait_for(|| backend.acked(ALPHA).len() == 2).await;

        assert!(backend.dead_letters(ALPHA).is_empty());
        // The worker itself survived the panic.
        assert!(!task.is_finished());
        stop(handle, task).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn job_timeout_is_a_retryable_failure() {
        let backend = backend();
        let producer = producer(&backend).await;
        let seen = Arc::new(Recorder::default());

        let worker = {
            let seen = seen.clone();
            Worker::<TestQueues, _>::builder(backend.clone())
                .job_timeout(Duration::from_secs(5))
                .handler(FnHandler::<Greet, _>::new(
                    move |_job: Greet, _ctx: JobContext| {
                        let seen = seen.clone();
                        async move {
                            let n = seen.enter();
                            if n == 1 {
                                tokio::time::sleep(Duration::from_secs(3600)).await;
                            }
                            seen.leave();
                            Ok(())
                        }
                    },
                ))
                .build()
                .await
                .unwrap()
        };
        let (handle, task) = start(worker);

        producer.enqueue(&Greet::new("ada")).await.unwrap();
        wait_for(|| backend.acked(ALPHA).len() == 2).await;

        assert_eq!(seen.attempts(), 2);
        assert!(backend.dead_letters(ALPHA).is_empty());
        stop(handle, task).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn deferred_job_runs_again_with_the_same_attempt_and_top_priority() {
        let backend = backend();
        let producer = producer(&backend).await;
        let seen = Arc::new(Recorder::default());
        let second_run = Arc::new(Mutex::new(None::<JobContext>));

        let worker = {
            let (seen, second_run) = (seen.clone(), second_run.clone());
            Worker::<TestQueues, _>::builder(backend.clone())
                .handler(FnHandler::<Greet, _>::new(
                    move |_j: Greet, ctx: JobContext| {
                        let (seen, second_run) = (seen.clone(), second_run.clone());
                        async move {
                            let n = seen.enter();
                            seen.leave();
                            if n == 1 {
                                assert_eq!(ctx.deferrals, 0);
                                assert_eq!(ctx.priority, 0);
                                Err(JobError::deferred(Duration::from_secs(1)))
                            } else {
                                *second_run.lock().unwrap() = Some(ctx);
                                Ok(())
                            }
                        }
                    },
                ))
                .build()
                .await
                .unwrap()
        };
        let (handle, task) = start(worker);

        let id = producer.enqueue(&Greet::new("limited")).await.unwrap();
        wait_for(|| seen.attempts() == 2).await;
        wait_for(|| backend.acked(ALPHA).len() == 2).await;

        let ctx = second_run.lock().unwrap().clone().expect("second run");
        assert_eq!(ctx.attempt, 1, "a deferral does not spend an attempt");
        assert_eq!(ctx.deferrals, 1);
        assert_eq!(ctx.priority, 10, "alpha's default ten levels");
        assert_eq!(ctx.job_id, id);

        // First ack is the deferred original, second the successful re-delivery.
        let acked = backend.acked(ALPHA);
        assert_eq!((acked[0].attempt, acked[0].deferrals), (1, 0));
        assert_eq!((acked[1].attempt, acked[1].deferrals), (1, 1));
        assert_eq!(acked[1].priority, 10);
        assert!(backend.dead_letters(ALPHA).is_empty());
        assert_eq!(handle.settle_failures(), 0, "the defer settled cleanly");
        assert_eq!(backend.deferred(ALPHA), 0);
        stop(handle, task).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_retry_after_a_deferral_drops_back_to_priority_zero() {
        let backend = backend();
        let producer = producer(&backend).await;
        let seen = Arc::new(Recorder::default());
        let third_run = Arc::new(Mutex::new(None::<JobContext>));

        // Run 1 defers, run 2 (back at the top of the queue) fails transiently, run 3
        // is the retry: it must have lost the deferral's priority.
        let worker = {
            let (seen, third_run) = (seen.clone(), third_run.clone());
            Worker::<TestQueues, _>::builder(backend.clone())
                .handler(FnHandler::<Greet, _>::new(
                    move |_j: Greet, ctx: JobContext| {
                        let (seen, third_run) = (seen.clone(), third_run.clone());
                        async move {
                            let n = seen.enter();
                            seen.leave();
                            match n {
                                1 => Err(JobError::deferred(Duration::from_secs(1))),
                                2 => {
                                    assert_eq!(ctx.priority, 10, "the deferral came back first");
                                    Err(JobError::retryable_msg("flaky"))
                                }
                                _ => {
                                    *third_run.lock().unwrap() = Some(ctx);
                                    Ok(())
                                }
                            }
                        }
                    },
                ))
                .build()
                .await
                .unwrap()
        };
        let (handle, task) = start(worker);

        producer.enqueue(&Greet::new("limited")).await.unwrap();
        wait_for(|| seen.attempts() == 3).await;
        wait_for(|| backend.acked(ALPHA).len() == 3).await;

        let ctx = third_run.lock().unwrap().clone().expect("third run");
        assert_eq!(ctx.attempt, 2, "the retry spent an attempt");
        assert_eq!(ctx.deferrals, 1, "the deferral history is carried forward");
        assert_eq!(ctx.priority, 0, "a retry must not keep jumping the backlog");

        // The envelope the backend saw agrees with what the handler was told.
        let acked = backend.acked(ALPHA);
        assert_eq!(
            (acked[2].attempt, acked[2].deferrals, acked[2].priority),
            (2, 1, 0)
        );
        assert!(backend.dead_letters(ALPHA).is_empty());
        assert_eq!(handle.settle_failures(), 0);
        stop(handle, task).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn deferral_on_a_queue_without_priorities_stays_at_zero() {
        let backend = backend();
        let producer = producer(&backend).await;
        let seen = Arc::new(Recorder::default());
        let second_run = Arc::new(Mutex::new(None::<JobContext>));

        let worker = {
            let (seen, second_run) = (seen.clone(), second_run.clone());
            Worker::<TestQueues, _>::builder(backend.clone())
                .handler(FnHandler::<Nudge, _>::new(
                    move |_j: Nudge, ctx: JobContext| {
                        let (seen, second_run) = (seen.clone(), second_run.clone());
                        async move {
                            let n = seen.enter();
                            seen.leave();
                            if n == 1 {
                                Err(JobError::deferred_msg(
                                    Duration::from_secs(2),
                                    "rate limited",
                                ))
                            } else {
                                *second_run.lock().unwrap() = Some(ctx);
                                Ok(())
                            }
                        }
                    },
                ))
                .build()
                .await
                .unwrap()
        };
        let (handle, task) = start(worker);

        producer.enqueue(&Nudge { id: 1 }).await.unwrap();
        wait_for(|| seen.attempts() == 2).await;

        let ctx = second_run.lock().unwrap().clone().expect("second run");
        assert_eq!(ctx.priority, 0, "gamma is not a priority queue");
        assert_eq!(ctx.deferrals, 1);
        assert_eq!(ctx.attempt, 1);
        assert!(backend.dead_letters(GAMMA).is_empty());
        assert_eq!(handle.settle_failures(), 0);
        stop(handle, task).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn deferral_never_consults_the_retry_policy() {
        let backend = backend();
        let producer = producer(&backend).await;
        let seen = Arc::new(Recorder::default());

        // Beta allows a single attempt: a *failure* here dead-letters at once. A
        // deferral must not, however often it happens.
        let worker = {
            let seen = seen.clone();
            Worker::<TestQueues, _>::builder(backend.clone())
                .handler(FnHandler::<Ping, _>::new(
                    move |_j: Ping, ctx: JobContext| {
                        let seen = seen.clone();
                        async move {
                            let n = seen.enter();
                            seen.leave();
                            assert_eq!(ctx.max_attempts, 1);
                            assert_eq!(ctx.attempt, 1);
                            assert!(ctx.is_last_attempt());
                            assert_eq!(ctx.deferrals as usize, n - 1);
                            if n < 4 {
                                Err(JobError::deferred(Duration::from_secs(1)))
                            } else {
                                Ok(())
                            }
                        }
                    },
                ))
                .build()
                .await
                .unwrap()
        };
        let (handle, task) = start(worker);

        producer.enqueue(&Ping { seq: 1 }).await.unwrap();
        wait_for(|| seen.attempts() == 4).await;
        wait_for(|| backend.acked(BETA).len() == 4).await;

        assert!(
            backend.dead_letters(BETA).is_empty(),
            "three deferrals on a one-attempt queue must not dead-letter"
        );
        assert!(backend.acked(BETA).iter().all(|e| e.attempt == 1));
        assert_eq!(
            backend.acked(BETA).last().unwrap().deferrals,
            3,
            "only the deferral counter moved"
        );
        assert_eq!(handle.settle_failures(), 0);
        stop(handle, task).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_job_in_hold_survives_the_worker_shutting_down() {
        let backend = backend();
        let producer = producer(&backend).await;
        let seen = Arc::new(Recorder::default());

        let worker = {
            let seen = seen.clone();
            Worker::<TestQueues, _>::builder(backend.clone())
                .handler(FnHandler::<Greet, _>::new(
                    move |_j: Greet, _c: JobContext| {
                        let seen = seen.clone();
                        async move {
                            seen.enter();
                            seen.leave();
                            Err(JobError::deferred(Duration::from_secs(3600)))
                        }
                    },
                ))
                .build()
                .await
                .unwrap()
        };
        let (handle, task) = start(worker);

        let id = producer.enqueue(&Greet::new("held")).await.unwrap();
        wait_for(|| backend.acked(ALPHA).len() == 1).await;
        assert_eq!(backend.deferred(ALPHA), 1);

        // The worker goes away while the job is still in hold.
        stop(handle, task).await.unwrap();
        assert_eq!(seen.attempts(), 1);
        assert_eq!(backend.deferred(ALPHA), 1, "still held, not lost");
        assert_eq!(backend.pending(ALPHA), 0);

        // The backend is shared and stays open, so the hold still expires.
        tokio::time::sleep(Duration::from_secs(3601)).await;
        assert_eq!(backend.deferred(ALPHA), 0);
        assert_eq!(backend.pending(ALPHA), 1);
        let waiting = backend.acked(ALPHA);
        assert_eq!(waiting[0].job_id, id);
        assert!(backend.dead_letters(ALPHA).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_deferred_job_beats_work_enqueued_while_it_waited() {
        /// Runs one `Greet` at a time, recording the order, and defers the job named
        /// `"limited"` the first time it sees it.
        async fn worker(
            backend: Arc<MemoryBackend>,
            order: Arc<Mutex<Vec<String>>>,
            seen: Arc<Recorder>,
        ) -> Worker<TestQueues, MemoryBackend> {
            Worker::<TestQueues, _>::builder(backend)
                .queues(&[TestQueues::Alpha])
                .concurrency(1)
                .handler(FnHandler::<Greet, _>::new(
                    move |job: Greet, ctx: JobContext| {
                        let (order, seen) = (order.clone(), seen.clone());
                        async move {
                            seen.enter();
                            seen.leave();
                            order.lock().unwrap().push(job.name.clone());
                            if job.name == "limited" && ctx.deferrals == 0 {
                                Err(JobError::deferred(Duration::from_secs(30)))
                            } else {
                                Ok(())
                            }
                        }
                    },
                ))
                .build()
                .await
                .unwrap()
        }

        let backend = backend();
        let producer = producer(&backend).await;
        let order = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen = Arc::new(Recorder::default());

        let first = worker(backend.clone(), order.clone(), seen.clone()).await;
        let (handle, task) = start(first);
        producer.enqueue(&Greet::new("limited")).await.unwrap();
        wait_for(|| backend.deferred(ALPHA) == 1).await;
        // Nothing consumes from here on, so the queue order is observable.
        stop(handle, task).await.unwrap();

        // Two ordinary jobs pile up while the deferred one is still in hold, and only
        // then does its delay elapse.
        producer.enqueue(&Greet::new("backlog-1")).await.unwrap();
        producer.enqueue(&Greet::new("backlog-2")).await.unwrap();
        assert_eq!(backend.pending(ALPHA), 2);
        wait_for(|| backend.deferred(ALPHA) == 0).await;
        assert_eq!(backend.pending(ALPHA), 3);

        let second = worker(backend.clone(), order.clone(), seen.clone()).await;
        let (handle, task) = start(second);
        wait_for(|| seen.attempts() == 4).await;

        assert_eq!(
            *order.lock().unwrap(),
            vec!["limited", "limited", "backlog-1", "backlog-2"],
            "the deferred job must overtake the backlog that built up"
        );
        assert_eq!(handle.settle_failures(), 0);
        stop(handle, task).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_defer_is_counted_as_a_settle_failure() {
        let inner = backend();
        let backend = Arc::new(Unsettleable(inner.clone()));
        let producer = Producer::<TestQueues, _>::new(backend.clone())
            .await
            .unwrap();

        let worker = Worker::<TestQueues, _>::builder(backend.clone())
            .handler(FnHandler::<Greet, _>::new(
                |_j: Greet, _c: JobContext| async move {
                    Err(JobError::deferred(Duration::from_secs(1)))
                },
            ))
            .build()
            .await
            .unwrap();
        let handle = worker.handle();
        let task = tokio::spawn(worker.run());

        producer.enqueue(&Greet::new("ada")).await.unwrap();
        wait_for(|| handle.settle_failures() == 1).await;

        assert!(inner.acked(ALPHA).is_empty());
        assert_eq!(inner.deferred(ALPHA), 0);
        handle.shutdown();
        task.await.expect("worker task panicked").unwrap();
    }

    #[tokio::test]
    async fn duplicate_handler_fails_the_build() {
        let backend = backend();
        let err = Worker::<TestQueues, _>::builder(backend)
            .handler(FnHandler::<Greet, _>::new(
                |_j: Greet, _c: JobContext| async move { Ok(()) },
            ))
            .handler(FnHandler::<Greet, _>::new(
                |_j: Greet, _c: JobContext| async move { Ok(()) },
            ))
            .build()
            .await
            .unwrap_err();
        match err {
            Error::DuplicateHandler(name) => assert_eq!(name, Greet::NAME),
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn queues_restricts_consumption_to_the_chosen_subset() {
        let backend = backend();
        let producer = producer(&backend).await;
        let alpha_seen = Arc::new(Recorder::default());
        let beta_seen = Arc::new(Recorder::default());

        let worker = {
            let (a, b) = (alpha_seen.clone(), beta_seen.clone());
            Worker::<TestQueues, _>::builder(backend.clone())
                .queues(&[TestQueues::Alpha, TestQueues::Alpha])
                .handler(FnHandler::<Greet, _>::new(
                    move |_j: Greet, _c: JobContext| {
                        let a = a.clone();
                        async move {
                            a.enter();
                            a.leave();
                            Ok(())
                        }
                    },
                ))
                .handler(FnHandler::<Ping, _>::new(
                    move |_j: Ping, _c: JobContext| {
                        let b = b.clone();
                        async move {
                            b.enter();
                            b.leave();
                            Ok(())
                        }
                    },
                ))
                .build()
                .await
                .unwrap()
        };
        assert_eq!(worker.queues().len(), 1, "duplicates must be collapsed");
        let (handle, task) = start(worker);

        producer.enqueue(&Ping { seq: 1 }).await.unwrap();
        producer.enqueue(&Greet::new("ada")).await.unwrap();
        wait_for(|| alpha_seen.attempts() == 1).await;

        assert_eq!(beta_seen.attempts(), 0);
        // The beta message is still sitting on its queue, untouched.
        assert_eq!(backend.pending(BETA), 1);
        stop(handle, task).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn graceful_shutdown_waits_for_in_flight_jobs() {
        let backend = backend();
        let producer = producer(&backend).await;
        let started = Arc::new(Recorder::default());
        let finished = Arc::new(Recorder::default());

        let worker = {
            let (s, f) = (started.clone(), finished.clone());
            Worker::<TestQueues, _>::builder(backend.clone())
                .handler(FnHandler::<Greet, _>::new(
                    move |_j: Greet, _c: JobContext| {
                        let (s, f) = (s.clone(), f.clone());
                        async move {
                            s.enter();
                            tokio::time::sleep(Duration::from_secs(30)).await;
                            f.enter();
                            f.leave();
                            s.leave();
                            Ok(())
                        }
                    },
                ))
                .build()
                .await
                .unwrap()
        };
        let (handle, task) = start(worker);

        producer.enqueue(&Greet::new("slow")).await.unwrap();
        wait_for(|| started.attempts() == 1).await;
        assert_eq!(finished.attempts(), 0);

        handle.shutdown();
        assert!(handle.is_shutdown());
        task.await.expect("worker task panicked").unwrap();

        assert_eq!(
            finished.attempts(),
            1,
            "shutdown must not abandon a running job"
        );
        assert_eq!(backend.acked(ALPHA).len(), 1);
        assert!(!backend.is_closed(), "closing the backend is opt-in");
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_processes_the_deliveries_it_already_pulled() {
        const PUBLISHED: usize = 6;

        let backend = backend();
        let producer = producer(&backend).await;
        let seen = Arc::new(Recorder::default());

        // Prefetch 4 on alpha with room for one job at a time: the consumer keeps
        // pulling while the dispatch loop is busy, so deliveries pile up in between.
        let worker = {
            let seen = seen.clone();
            Worker::<TestQueues, _>::builder(backend.clone())
                .queues(&[TestQueues::Alpha])
                .concurrency(1)
                .handler(FnHandler::<Greet, _>::new(
                    move |_j: Greet, _c: JobContext| {
                        let seen = seen.clone();
                        async move {
                            seen.enter();
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            seen.leave();
                            Ok(())
                        }
                    },
                ))
                .build()
                .await
                .unwrap()
        };
        let (handle, task) = start(worker);

        for i in 0..PUBLISHED {
            producer
                .enqueue(&Greet::new(&format!("job-{i}")))
                .await
                .unwrap();
        }
        // One job is running; the rest are buffered in the worker, in the backend's
        // consumer, or still on the queue.
        wait_for(|| seen.attempts() == 1).await;

        handle.shutdown();
        task.await.expect("worker task panicked").unwrap();

        let acked = backend.acked(ALPHA).len();
        let pending = backend.pending(ALPHA);
        assert_eq!(
            acked + pending,
            PUBLISHED,
            "{acked} acked + {pending} pending must account for every message"
        );
        assert!(
            acked >= 2,
            "buffered deliveries must be processed, not dropped; only {acked} were"
        );
        assert_eq!(acked, seen.attempts(), "every job that ran was settled");
        assert!(backend.dead_letters(ALPHA).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn closing_the_backend_on_shutdown_is_opt_in() {
        let backend = backend();

        let worker = Worker::<TestQueues, _>::builder(backend.clone())
            .handler(FnHandler::<Greet, _>::new(
                |_j: Greet, _c: JobContext| async move { Ok(()) },
            ))
            .build()
            .await
            .unwrap();
        let (handle, task) = start(worker);
        stop(handle, task).await.unwrap();
        assert!(!backend.is_closed(), "default must leave the backend alone");

        // Still usable afterwards: this is what a shared `Producer` relies on.
        producer(&backend)
            .await
            .enqueue(&Greet::new("after"))
            .await
            .unwrap();

        let worker = Worker::<TestQueues, _>::builder(backend.clone())
            .close_backend_on_shutdown(true)
            .handler(FnHandler::<Greet, _>::new(
                |_j: Greet, _c: JobContext| async move { Ok(()) },
            ))
            .build()
            .await
            .unwrap();
        assert!(format!("{worker:?}").contains("close_backend_on_shutdown: true"));
        let (handle, task) = start(worker);
        stop(handle, task).await.unwrap();
        assert!(backend.is_closed());
    }

    #[tokio::test(start_paused = true)]
    async fn failures_to_settle_are_counted() {
        let inner = backend();
        let backend = Arc::new(Unsettleable(inner.clone()));
        let producer = Producer::<TestQueues, _>::new(backend.clone())
            .await
            .unwrap();

        let worker = Worker::<TestQueues, _>::builder(backend.clone())
            .handler(FnHandler::<Greet, _>::new(
                |_j: Greet, _c: JobContext| async move { Ok(()) },
            ))
            .build()
            .await
            .unwrap();
        let handle = worker.handle();
        assert_eq!(handle.settle_failures(), 0);
        let task = tokio::spawn(worker.run());

        producer.enqueue(&Greet::new("ada")).await.unwrap();
        wait_for(|| handle.settle_failures() == 1).await;

        // The handler ran, but the broker never heard about it.
        assert!(inner.acked(ALPHA).is_empty());
        assert!(format!("{handle:?}").contains("settle_failures: 1"));
        handle.shutdown();
        task.await.expect("worker task panicked").unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn concurrency_is_capped() {
        let backend = backend();
        let producer = producer(&backend).await;
        let seen = Arc::new(Recorder::default());

        let worker = {
            let seen = seen.clone();
            Worker::<TestQueues, _>::builder(backend.clone())
                .concurrency(2)
                .handler(FnHandler::<Greet, _>::new(
                    move |_j: Greet, _c: JobContext| {
                        let seen = seen.clone();
                        async move {
                            seen.enter();
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            seen.leave();
                            Ok(())
                        }
                    },
                ))
                .build()
                .await
                .unwrap()
        };
        assert_eq!(worker.concurrency(), 2);
        let (handle, task) = start(worker);

        for i in 0..8 {
            producer
                .enqueue(&Greet::new(&format!("job-{i}")))
                .await
                .unwrap();
        }
        wait_for(|| backend.acked(ALPHA).len() == 8).await;

        assert_eq!(seen.attempts(), 8);
        assert!(
            seen.max_running() <= 2,
            "ran {} jobs at once",
            seen.max_running()
        );
        stop(handle, task).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn default_concurrency_is_the_sum_of_prefetch() {
        let backend = backend();
        let worker = Worker::<TestQueues, _>::builder(backend)
            .handler(FnHandler::<Greet, _>::new(
                |_j: Greet, _c: JobContext| async move { Ok(()) },
            ))
            .build()
            .await
            .unwrap();
        // Alpha prefetch 4 + Beta prefetch 2 + Gamma prefetch 1.
        assert_eq!(worker.concurrency(), 7);
        assert_eq!(worker.queues().len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn lost_backend_ends_the_run_with_an_error() {
        let backend = backend();
        let worker = Worker::<TestQueues, _>::builder(backend.clone())
            .handler(FnHandler::<Greet, _>::new(
                |_j: Greet, _c: JobContext| async move { Ok(()) },
            ))
            .build()
            .await
            .unwrap();
        let (_handle, task) = start(worker);

        // Simulate the connection going away underneath the worker, once both
        // consumers have actually registered with the backend.
        {
            let backend = backend.clone();
            wait_for_tasks(move || {
                backend.consumer_count(ALPHA) == 1 && backend.consumer_count(BETA) == 1
            })
            .await;
        }
        backend.close().await.unwrap();

        match task.await.expect("worker task panicked") {
            Err(Error::ConsumerStopped(queue)) => assert!(queue.starts_with("test.")),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_before_run_stops_immediately() {
        let backend = backend();
        let worker = Worker::<TestQueues, _>::builder(backend.clone())
            .handler(FnHandler::<Greet, _>::new(
                |_j: Greet, _c: JobContext| async move { Ok(()) },
            ))
            .build()
            .await
            .unwrap();
        let handle = worker.handle();
        handle.shutdown();
        worker.run().await.unwrap();
        assert!(!backend.is_closed());
        assert!(format!("{handle:?}").contains("shutdown: true"));
    }

    #[tokio::test(start_paused = true)]
    async fn handlers_of_different_job_types_are_routed_independently() {
        let backend = backend();
        let producer = producer(&backend).await;
        let greets = Arc::new(Recorder::default());
        let pings = Arc::new(Recorder::default());

        let worker = {
            let (g, p) = (greets.clone(), pings.clone());
            Worker::<TestQueues, _>::builder(backend.clone())
                .handler(FnHandler::<Greet, _>::new(
                    move |_j: Greet, _c: JobContext| {
                        let g = g.clone();
                        async move {
                            g.enter();
                            g.leave();
                            Ok(())
                        }
                    },
                ))
                .handler(FnHandler::<Ping, _>::new(
                    move |_j: Ping, _c: JobContext| {
                        let p = p.clone();
                        async move {
                            p.enter();
                            p.leave();
                            Ok(())
                        }
                    },
                ))
                .build()
                .await
                .unwrap()
        };
        let (handle, task) = start(worker);

        producer.enqueue(&Greet::new("ada")).await.unwrap();
        producer.enqueue(&Ping { seq: 1 }).await.unwrap();
        producer.enqueue(&Ping { seq: 2 }).await.unwrap();

        wait_for(|| greets.attempts() == 1 && pings.attempts() == 2).await;
        assert_eq!(backend.acked(ALPHA).len(), 1);
        assert_eq!(backend.acked(BETA).len(), 2);
        stop(handle, task).await.unwrap();
    }
}
