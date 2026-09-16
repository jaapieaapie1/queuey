//! The dead-letter hook and the runtime retry overrides, through the facade only.
//!
//! Nothing here names `queuey-core` or `queuey-macros`: this is the surface a
//! downstream user sees, which is what makes it worth testing separately from the
//! unit tests in the core crate. The transport is [`MemoryBackend`] and the clock is
//! paused, so retry backoff costs no wall time.

use std::{sync::Mutex, time::Duration};

use queuey::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
#[queues(prefix = "dl")]
enum AppQueues {
    /// Three attempts with a short fixed backoff, unless a worker overrides it.
    #[queue(prefetch = 4, retry(max_attempts = 3, backoff = "fixed", delay = "1s"))]
    Webhooks,
}

const WEBHOOKS: &str = "dl.webhooks";

#[derive(Debug, Serialize, Deserialize, Job)]
#[job(queue = AppQueues::Webhooks)]
struct Deliver {
    endpoint: String,
    /// Fail every attempt when true.
    broken: bool,
}

/// A job on the same queue that no worker registers a handler for.
#[derive(Debug, Serialize, Deserialize, Job)]
#[job(queue = AppQueues::Webhooks)]
struct Forgotten {
    id: u32,
}

/// Collects what the hook was told, in order.
#[derive(Default)]
struct Reported(Mutex<Vec<DeadLetter>>);

/// Reads the endpoint back out of a dead-lettered envelope's payload.
trait Endpoint {
    fn url_of(&self) -> String;
}

impl Endpoint for DeadLetter {
    fn url_of(&self) -> String {
        self.envelope.payload["endpoint"]
            .as_str()
            .unwrap_or_default()
            .to_owned()
    }
}

impl Reported {
    fn all(&self) -> Vec<DeadLetter> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn len(&self) -> usize {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

/// Polls `cond` while letting the paused clock (and so retry delays) advance.
async fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..1_000 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

/// A worker with the hook registered and, optionally, a retry override.
async fn start(
    backend: Arc<MemoryBackend>,
    reported: Arc<Reported>,
    override_policy: Option<RetryPolicy>,
) -> (WorkerHandle, tokio::task::JoinHandle<queuey::Result<()>>) {
    let mut builder = Worker::<AppQueues, _>::builder(backend)
        .on_dead_letter(FnDeadLetterHook::new(move |dead: DeadLetter| {
            let reported = reported.clone();
            async move {
                reported
                    .0
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(dead);
            }
        }))
        .handler(FnHandler::<Deliver, _>::new(
            |job: Deliver, _ctx: JobContext| async move {
                if job.broken {
                    Err(JobError::retryable_msg(format!(
                        "{} refused the delivery",
                        job.endpoint
                    )))
                } else {
                    Ok(())
                }
            },
        ));
    if let Some(policy) = override_policy {
        builder = builder.retry_override(AppQueues::Webhooks, policy);
    }
    let worker = builder.build().await.unwrap();
    let handle = worker.handle();
    (handle, tokio::spawn(worker.run()))
}

#[tokio::test(start_paused = true)]
async fn the_hook_reports_a_job_the_policy_gave_up_on() {
    let backend = Arc::new(MemoryBackend::new());
    let producer = Producer::<AppQueues, _>::new(backend.clone())
        .await
        .unwrap();
    let reported = Arc::new(Reported::default());
    let (handle, worker) = start(backend.clone(), reported.clone(), None).await;

    producer
        .enqueue(&Deliver {
            endpoint: "https://example.test/hook".to_owned(),
            broken: true,
        })
        .await
        .unwrap();
    wait_for("the hook to fire", || reported.len() == 1).await;

    let dead = &reported.all()[0];
    assert_eq!(dead.cause, DeadLetterCause::Exhausted);
    assert_eq!(dead.attempts(), 3, "what the queue declared");
    assert_eq!(dead.max_attempts, Some(3));
    assert!(dead.reason.contains("refused the delivery"));
    // The hook is a notification, not a replacement: the job is dead-lettered too.
    assert_eq!(backend.dead_letters(WEBHOOKS).len(), 1);
    assert!(backend.succeeded(WEBHOOKS).is_empty());

    handle.shutdown();
    worker.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn an_override_decides_how_many_attempts_the_hook_reports() {
    let backend = Arc::new(MemoryBackend::new());
    let producer = Producer::<AppQueues, _>::new(backend.clone())
        .await
        .unwrap();
    let reported = Arc::new(Reported::default());
    // What the macro compiled in is three attempts; this process was configured for
    // five, without touching the queue declaration.
    let (handle, worker) = start(
        backend.clone(),
        reported.clone(),
        Some(RetryPolicy::fixed(5, Duration::from_secs(1))),
    )
    .await;

    producer
        .enqueue(&Deliver {
            endpoint: "https://example.test/hook".to_owned(),
            broken: true,
        })
        .await
        .unwrap();
    wait_for("the hook to fire", || reported.len() == 1).await;

    let dead = &reported.all()[0];
    assert_eq!(dead.attempts(), 5);
    assert_eq!(dead.max_attempts, Some(5));
    // Four attempts were acked as retries; the fifth is the dead letter.
    assert_eq!(backend.retried(WEBHOOKS).len(), 4);

    handle.shutdown();
    worker.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn the_hook_reports_a_job_that_never_reached_any_handler() {
    let backend = Arc::new(MemoryBackend::new());
    let producer = Producer::<AppQueues, _>::new(backend.clone())
        .await
        .unwrap();
    let reported = Arc::new(Reported::default());
    let (handle, worker) = start(backend.clone(), reported.clone(), None).await;

    producer.enqueue(&Forgotten { id: 42 }).await.unwrap();
    wait_for("the hook to fire", || reported.len() == 1).await;

    let dead = &reported.all()[0];
    assert_eq!(dead.cause, DeadLetterCause::NoHandler);
    assert!(!dead.cause.reached_handler());
    assert_eq!(dead.max_attempts, None, "no handler, so no policy");
    assert_eq!(dead.envelope.job_type, <Forgotten as Job>::NAME);

    handle.shutdown();
    worker.await.unwrap().unwrap();
}

/// A handler that reads its policy off the payload: the endpoint this test calls
/// "lenient" gets five attempts, everything else keeps the queue's three.
struct Picky;

#[async_trait]
impl JobHandler for Picky {
    type Job = Deliver;

    fn set_retry_policy(&self, job: &Deliver) -> Option<RetryPolicy> {
        job.endpoint
            .contains("lenient")
            .then(|| RetryPolicy::fixed(5, Duration::from_secs(1)))
    }

    async fn handle(&self, job: Deliver, _ctx: JobContext) -> Result<(), JobError> {
        if job.broken {
            Err(JobError::retryable_msg("refused"))
        } else {
            Ok(())
        }
    }
}

#[tokio::test(start_paused = true)]
async fn one_endpoint_can_be_treated_more_leniently_than_another() {
    let backend = Arc::new(MemoryBackend::new());
    let producer = Producer::<AppQueues, _>::new(backend.clone())
        .await
        .unwrap();
    let reported = Arc::new(Reported::default());

    let worker = {
        let reported = reported.clone();
        Worker::<AppQueues, _>::builder(backend.clone())
            .on_dead_letter(FnDeadLetterHook::new(move |dead: DeadLetter| {
                let reported = reported.clone();
                async move {
                    reported
                        .0
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(dead);
                }
            }))
            .handler(Picky)
            .build()
            .await
            .unwrap()
    };
    let handle = worker.handle();
    let task = tokio::spawn(worker.run());

    producer
        .enqueue(&Deliver {
            endpoint: "https://lenient.test/hook".to_owned(),
            broken: true,
        })
        .await
        .unwrap();
    producer
        .enqueue(&Deliver {
            endpoint: "https://strict.test/hook".to_owned(),
            broken: true,
        })
        .await
        .unwrap();
    wait_for("both jobs to die", || reported.len() == 2).await;

    let mut by_endpoint: Vec<(String, u32)> = reported
        .all()
        .iter()
        .map(|dead| (dead.url_of(), dead.attempts()))
        .collect();
    by_endpoint.sort();
    assert_eq!(
        by_endpoint,
        vec![
            ("https://lenient.test/hook".to_owned(), 5),
            ("https://strict.test/hook".to_owned(), 3),
        ],
        "same job type, same queue, different leniency"
    );

    handle.shutdown();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_job_that_works_is_never_reported() {
    let backend = Arc::new(MemoryBackend::new());
    let producer = Producer::<AppQueues, _>::new(backend.clone())
        .await
        .unwrap();
    let reported = Arc::new(Reported::default());
    let (handle, worker) = start(backend.clone(), reported.clone(), None).await;

    producer
        .enqueue(&Deliver {
            endpoint: "https://example.test/hook".to_owned(),
            broken: false,
        })
        .await
        .unwrap();
    wait_for("the job to succeed", || {
        backend.succeeded(WEBHOOKS).len() == 1
    })
    .await;

    assert_eq!(reported.len(), 0);
    assert!(backend.dead_letters(WEBHOOKS).is_empty());

    handle.shutdown();
    worker.await.unwrap().unwrap();
}
