//! End-to-end coverage of the whole stack through the facade only.
//!
//! Nothing here names `queuey-core`, `queuey-macros` or a
//! `crate = "..."` attribute: this is exactly the surface a downstream user sees.
//! The transport is [`MemoryBackend`], and every test runs on a paused clock so
//! that retry backoff costs no wall time.

use std::{sync::Mutex, time::Duration};

use queuey::prelude::*;

// ---------------------------------------------------------------- queue set

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
#[queues(prefix = "e2e")]
enum AppQueues {
    /// Three attempts, exponential backoff.
    #[queue(
        prefetch = 4,
        retry(
            max_attempts = 3,
            backoff = "exponential",
            base = "1s",
            max = "10s",
            jitter = false
        )
    )]
    Emails,
    /// Renamed, and with the default "no retries" policy.
    #[queue(name = "img", prefetch = 2)]
    Images,
    /// Deliberately *not* a priority queue: deferred jobs come back FIFO, at `0`.
    #[queue(name = "slow", prefetch = 2, max_priority = 0)]
    SlowApi,
}

const EMAILS: &str = "e2e.emails";
const IMAGES: &str = "e2e.img";
const SLOW: &str = "e2e.slow";

// --------------------------------------------------------------------- jobs

/// What a handler should do, encoded in the payload so tests stay declarative.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Plan {
    /// Succeed on the first attempt.
    Succeed,
    /// Fail (retryably) for the first `n` attempts, then succeed.
    FailTimes(u32),
    /// Never succeed; the policy eventually gives up.
    AlwaysFail,
    /// Fail fatally: straight to the dead-letter queue, no retry.
    Fatal,
}

impl Plan {
    fn apply(self, attempt: u32) -> Result<(), JobError> {
        match self {
            Plan::Succeed => Ok(()),
            Plan::FailTimes(n) if attempt <= n => Err(JobError::retryable_msg(format!(
                "flaky on attempt {attempt}"
            ))),
            Plan::FailTimes(_) => Ok(()),
            Plan::AlwaysFail => Err(JobError::retryable_msg("service is down")),
            Plan::Fatal => Err(JobError::fatal_msg("malformed recipient")),
        }
    }
}

/// Inherits the three-attempt exponential policy of `AppQueues::Emails`.
#[derive(Debug, Serialize, Deserialize, Job)]
#[job(queue = AppQueues::Emails)]
struct SendEmail {
    to: String,
    plan: Plan,
}

/// Overrides the queue: `AppQueues::Images` has no retries, this job gets two
/// attempts with a fixed delay.
#[derive(Debug, Serialize, Deserialize, Job)]
#[job(queue = AppQueues::Images, retry(max_attempts = 2, backoff = "fixed", delay = "500ms"))]
struct ResizeImage {
    path: String,
    plan: Plan,
}

/// Calls a rate-limited API on the priority queue. The handler defers the job named
/// `"limited"` the first time it sees it, as if the API had answered `429`.
#[derive(Debug, Serialize, Deserialize, Job)]
#[job(queue = AppQueues::Emails)]
struct CallApi {
    name: String,
}

impl CallApi {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
        }
    }
}

/// The same thing on `AppQueues::SlowApi`, which has no priority levels.
#[derive(Debug, Serialize, Deserialize, Job)]
#[job(queue = AppQueues::SlowApi)]
struct CallSlowApi {
    name: String,
}

// ----------------------------------------------------------------- handlers

/// One handler invocation: which job ran, and what its context said.
#[derive(Debug, Clone)]
struct Run {
    job: String,
    attempt: u32,
    deferrals: u32,
    priority: u8,
}

/// Records every handler invocation, in the order they happened.
#[derive(Default)]
struct Log(Mutex<Vec<Run>>);

impl Log {
    fn record(&self, job: &str, ctx: &JobContext) {
        self.lock().push(Run {
            job: job.to_owned(),
            attempt: ctx.attempt,
            deferrals: ctx.deferrals,
            priority: ctx.priority,
        });
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Run>> {
        // A failing assertion inside a handler must not poison later reads.
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Every run of `job`, in the order they ran.
    fn runs_of(&self, job: &str) -> Vec<Run> {
        self.lock()
            .iter()
            .filter(|r| r.job == job)
            .cloned()
            .collect()
    }

    /// Attempt numbers seen for `job`, in the order they ran.
    fn attempts_of(&self, job: &str) -> Vec<u32> {
        self.runs_of(job).iter().map(|r| r.attempt).collect()
    }

    /// The names of every run, in order. This is what makes priority observable.
    fn order(&self) -> Vec<String> {
        self.lock().iter().map(|r| r.job.clone()).collect()
    }

    fn runs(&self) -> usize {
        self.lock().len()
    }
}

/// A plain `impl JobHandler` handler.
struct EmailHandler {
    log: Arc<Log>,
}

#[async_trait]
impl JobHandler for EmailHandler {
    type Job = SendEmail;

    async fn handle(&self, job: SendEmail, ctx: JobContext) -> Result<(), JobError> {
        assert_eq!(ctx.queue, EMAILS);
        assert_eq!(ctx.max_attempts, 3, "queue policy applies to SendEmail");
        self.log.record(&job.to, &ctx);
        job.plan.apply(ctx.attempt)
    }
}

/// The same thing as a closure, via `FnHandler`.
fn image_handler(log: Arc<Log>) -> impl JobHandler<Job = ResizeImage> {
    FnHandler::<ResizeImage, _>::new(move |job: ResizeImage, ctx: JobContext| {
        let log = log.clone();
        async move {
            assert_eq!(ctx.queue, IMAGES);
            assert_eq!(ctx.max_attempts, 2, "job override beats the queue policy");
            log.record(&job.path, &ctx);
            job.plan.apply(ctx.attempt)
        }
    })
}

/// Defers the job named `"limited"` once, as if the API had answered
/// `429 Too Many Requests` with `Retry-After: 30`; everything else succeeds.
fn api_handler(log: Arc<Log>) -> impl JobHandler<Job = CallApi> {
    FnHandler::<CallApi, _>::new(move |job: CallApi, ctx: JobContext| {
        let log = log.clone();
        async move {
            assert_eq!(ctx.queue, EMAILS);
            log.record(&job.name, &ctx);
            if job.name == "limited" && ctx.deferrals == 0 {
                Err(JobError::deferred_msg(
                    Duration::from_secs(30),
                    "rate limited",
                ))
            } else {
                Ok(())
            }
        }
    })
}

/// The same, on the queue that has no priority levels.
fn slow_api_handler(log: Arc<Log>) -> impl JobHandler<Job = CallSlowApi> {
    FnHandler::<CallSlowApi, _>::new(move |job: CallSlowApi, ctx: JobContext| {
        let log = log.clone();
        async move {
            assert_eq!(ctx.queue, SLOW);
            log.record(&job.name, &ctx);
            if ctx.deferrals == 0 {
                Err(JobError::deferred_msg(
                    Duration::from_secs(30),
                    "rate limited",
                ))
            } else {
                Ok(())
            }
        }
    })
}

// ------------------------------------------------------------------ harness

/// A running worker over `backend` with every handler registered.
///
/// One job at a time, so the order in which the backend hands work out is the order
/// the log sees, which is what makes the priority of a deferred job observable.
async fn spawn_worker(
    backend: Arc<MemoryBackend>,
    log: Arc<Log>,
) -> (WorkerHandle, tokio::task::JoinHandle<queuey::Result<()>>) {
    let worker = Worker::<AppQueues, _>::builder(backend)
        .concurrency(1)
        .handler(EmailHandler { log: log.clone() })
        .handler(image_handler(log.clone()))
        .handler(api_handler(log.clone()))
        .handler(slow_api_handler(log.clone()))
        .build()
        .await
        .unwrap();
    let handle = worker.handle();
    (handle, tokio::spawn(worker.run()))
}

/// Polls `cond` while letting the paused clock (and so retry and hold delays) advance.
async fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..1_000 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for: {what}");
}

/// Everything a test needs: a backend to inspect, a producer, and a running worker.
struct Harness {
    backend: Arc<MemoryBackend>,
    producer: Producer<AppQueues, MemoryBackend>,
    log: Arc<Log>,
    handle: WorkerHandle,
    worker: tokio::task::JoinHandle<queuey::Result<()>>,
}

impl Harness {
    async fn start() -> Self {
        let backend = Arc::new(MemoryBackend::new());
        let log = Arc::new(Log::default());
        let producer = Producer::<AppQueues, _>::new(backend.clone())
            .await
            .unwrap();

        let (handle, worker) = spawn_worker(backend.clone(), log.clone()).await;

        Self {
            backend,
            producer,
            log,
            handle,
            worker,
        }
    }

    /// Polls `cond` while letting the paused clock (and so retry delays) advance.
    async fn wait_for(&self, what: &str, cond: impl FnMut() -> bool) {
        wait_for(what, cond).await;
    }

    /// Graceful shutdown; returns whatever `Worker::run` returned.
    async fn stop(self) -> queuey::Result<()> {
        self.handle.shutdown();
        self.worker.await.expect("worker task panicked")
    }
}

// -------------------------------------------------------------------- tests

#[test]
fn derived_queue_metadata_is_what_the_attributes_say() {
    assert_eq!(
        AppQueues::all(),
        &[AppQueues::Emails, AppQueues::Images, AppQueues::SlowApi]
    );
    assert_eq!(AppQueues::Emails.name(), EMAILS);
    assert_eq!(AppQueues::Images.name(), IMAGES);
    assert_eq!(AppQueues::SlowApi.name(), SLOW);
    assert_eq!(AppQueues::from_name(IMAGES), Some(AppQueues::Images));

    let emails: QueueConfig = AppQueues::Emails.config();
    assert_eq!(emails.prefetch, 4);
    assert_eq!(emails.retry.max_attempts, 3);
    assert!(matches!(emails.retry.backoff, Backoff::Exponential { .. }));

    // Priority levels: the default unless the attribute says otherwise, and `0`
    // means "not a priority queue".
    assert_eq!(emails.max_priority, Some(DEFAULT_MAX_PRIORITY));
    assert_eq!(AppQueues::Images.config().max_priority, Some(10));
    assert_eq!(AppQueues::SlowApi.config().max_priority, None);

    // The queue itself does not retry; the job overrides it.
    assert_eq!(AppQueues::Images.config().retry, RetryPolicy::none());
    assert_eq!(
        ResizeImage::retry_policy(),
        Some(RetryPolicy::fixed(2, Duration::from_millis(500)))
    );
    assert_eq!(SendEmail::retry_policy(), None);
    assert_eq!(SendEmail::QUEUE, AppQueues::Emails);
    assert!(SendEmail::NAME.ends_with("::SendEmail"));
}

#[tokio::test(start_paused = true)]
async fn every_enqueued_job_is_processed() {
    let h = Harness::start().await;

    for to in ["ada@example.com", "grace@example.com", "alan@example.com"] {
        h.producer
            .enqueue(&SendEmail {
                to: to.to_owned(),
                plan: Plan::Succeed,
            })
            .await
            .unwrap();
    }
    h.producer
        .enqueue(&ResizeImage {
            path: "cat.png".to_owned(),
            plan: Plan::Succeed,
        })
        .await
        .unwrap();

    h.wait_for("4 jobs to run", || h.log.runs() == 4).await;

    assert_eq!(h.backend.acked(EMAILS).len(), 3);
    assert_eq!(h.backend.acked(IMAGES).len(), 1);
    assert!(h.backend.dead_letters(EMAILS).is_empty());
    assert!(h.backend.dead_letters(IMAGES).is_empty());
    assert_eq!(h.backend.pending(EMAILS), 0);

    h.stop().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_flaky_job_is_retried_with_increasing_attempt_numbers() {
    let h = Harness::start().await;

    // Queue policy allows three attempts; fail the first two.
    let id = h
        .producer
        .enqueue(&SendEmail {
            to: "flaky@example.com".to_owned(),
            plan: Plan::FailTimes(2),
        })
        .await
        .unwrap();

    h.wait_for("the third attempt to succeed", || {
        h.log.attempts_of("flaky@example.com").len() == 3
    })
    .await;

    assert_eq!(h.log.attempts_of("flaky@example.com"), vec![1, 2, 3]);

    // Each retry acks the envelope it replaces, so all three attempts show up.
    let acked = h.backend.acked(EMAILS);
    assert_eq!(
        acked.iter().map(|e| e.attempt).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(
        acked.iter().all(|e| e.job_id == id),
        "same job id throughout"
    );
    assert!(h.backend.dead_letters(EMAILS).is_empty());

    h.stop().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_fatal_error_dead_letters_without_retrying() {
    let h = Harness::start().await;

    h.producer
        .enqueue(&SendEmail {
            to: "fatal@example.com".to_owned(),
            plan: Plan::Fatal,
        })
        .await
        .unwrap();

    h.wait_for("the dead letter", || {
        !h.backend.dead_letters(EMAILS).is_empty()
    })
    .await;

    assert_eq!(
        h.log.attempts_of("fatal@example.com"),
        vec![1],
        "a fatal error must not be retried"
    );
    let dead = h.backend.dead_letters(EMAILS);
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].0.attempt, 1);
    assert!(dead[0].1.contains("malformed recipient"), "{:?}", dead[0].1);
    assert!(h.backend.acked(EMAILS).is_empty());

    h.stop().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn exhausting_the_attempts_dead_letters_the_job() {
    let h = Harness::start().await;

    // Two attempts (the job override), both retryable failures.
    h.producer
        .enqueue(&ResizeImage {
            path: "broken.png".to_owned(),
            plan: Plan::AlwaysFail,
        })
        .await
        .unwrap();

    h.wait_for("the dead letter", || {
        !h.backend.dead_letters(IMAGES).is_empty()
    })
    .await;

    assert_eq!(h.log.attempts_of("broken.png"), vec![1, 2]);
    let dead = h.backend.dead_letters(IMAGES);
    assert_eq!(dead[0].0.attempt, 2);
    assert!(dead[0].1.contains("max attempts"), "{:?}", dead[0].1);
    assert!(dead[0].1.contains("service is down"), "{:?}", dead[0].1);

    h.stop().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_deferred_job_keeps_its_attempt_and_overtakes_the_backlog() {
    let backend = Arc::new(MemoryBackend::new());
    let log = Arc::new(Log::default());
    let producer = Producer::<AppQueues, _>::new(backend.clone())
        .await
        .unwrap();

    // First run: the API says `429`, so the handler defers for 30s instead of failing.
    let (handle, task) = spawn_worker(backend.clone(), log.clone()).await;
    let id = producer.enqueue(&CallApi::new("limited")).await.unwrap();
    wait_for("the job to land in hold", || backend.deferred(EMAILS) == 1).await;

    // With a live consumer the backlog drains the instant it is enqueued, so the
    // ordering is only observable while nothing is consuming: stop the worker, let
    // the hold expire, then start a fresh one on the same backend.
    handle.shutdown();
    task.await.unwrap().unwrap();

    producer.enqueue(&CallApi::new("backlog-1")).await.unwrap();
    producer.enqueue(&CallApi::new("backlog-2")).await.unwrap();
    assert_eq!(backend.pending(EMAILS), 2);

    wait_for("the hold to expire", || backend.deferred(EMAILS) == 0).await;
    assert_eq!(backend.pending(EMAILS), 3);

    let (handle, task) = spawn_worker(backend.clone(), log.clone()).await;
    wait_for("all four runs", || log.runs() == 4).await;

    let runs = log.runs_of("limited");
    assert_eq!(runs.len(), 2, "one deferred run and one that succeeded");
    assert_eq!(runs[0].deferrals, 0);
    assert_eq!(runs[1].attempt, 1, "a deferral does not spend an attempt");
    assert_eq!(runs[1].deferrals, 1);
    assert_eq!(
        runs[1].priority, 10,
        "back at the highest priority the queue knows"
    );

    assert_eq!(
        log.order(),
        vec!["limited", "limited", "backlog-1", "backlog-2"],
        "the deferred job must overtake the backlog that built up while it waited"
    );

    // Nothing failed, so nothing was dead-lettered; the deferral acked the original
    // envelope and the re-delivery acked the copy.
    assert!(backend.dead_letters(EMAILS).is_empty());
    let acked = backend.acked(EMAILS);
    let mine: Vec<_> = acked.iter().filter(|e| e.job_id == id).collect();
    assert_eq!(mine.len(), 2);
    assert_eq!((mine[0].attempt, mine[0].deferrals), (1, 0));
    assert_eq!((mine[1].attempt, mine[1].deferrals), (1, 1));

    handle.shutdown();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_producer_side_defer_arrives_at_the_top_without_a_deferral() {
    let h = Harness::start().await;

    let id = h
        .producer
        .defer(&CallApi::new("scheduled"), Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(h.backend.deferred(EMAILS), 1, "held, not deliverable yet");
    assert_eq!(h.backend.pending(EMAILS), 0);

    h.wait_for("the held job to run", || h.log.runs() == 1)
        .await;

    let runs = h.log.runs_of("scheduled");
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].attempt, 1);
    assert_eq!(runs[0].deferrals, 0, "the producer never deferred it once");
    assert_eq!(
        runs[0].priority, 10,
        "it still arrives ahead of the backlog"
    );

    assert_eq!(h.backend.deferred(EMAILS), 0);
    assert_eq!(h.backend.acked(EMAILS).len(), 1);
    assert_eq!(h.backend.acked(EMAILS)[0].job_id, id);
    assert!(h.backend.dead_letters(EMAILS).is_empty());

    h.stop().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn deferring_on_a_queue_without_priorities_comes_back_at_zero() {
    let h = Harness::start().await;

    h.producer
        .enqueue(&CallSlowApi {
            name: "throttled".to_owned(),
        })
        .await
        .unwrap();

    h.wait_for("the second run", || h.log.runs() == 2).await;

    let runs = h.log.runs_of("throttled");
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0].priority, 0);
    assert_eq!(runs[1].attempt, 1);
    assert_eq!(runs[1].deferrals, 1);
    assert_eq!(
        runs[1].priority, 0,
        "`max_priority = 0` is not a priority queue, so the job comes back FIFO"
    );
    assert!(h.backend.dead_letters(SLOW).is_empty());

    h.stop().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn shutdown_returns_from_run_and_leaves_the_backend_to_its_owner() {
    let h = Harness::start().await;

    h.producer
        .enqueue(&SendEmail {
            to: "last@example.com".to_owned(),
            plan: Plan::Succeed,
        })
        .await
        .unwrap();
    h.wait_for("the job to run", || h.log.runs() == 1).await;

    assert!(!h.handle.is_shutdown());
    let backend = h.backend.clone();
    h.stop().await.expect("graceful shutdown is not an error");

    // The backend is shared with the producer, so the worker leaves it open unless
    // `close_backend_on_shutdown(true)` says otherwise; closing it is the caller's call.
    assert!(!backend.is_closed());
    backend.close().await.unwrap();
    assert!(backend.is_closed());
}
