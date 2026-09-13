//! The whole library in one file, with no broker in sight.
//!
//! ```sh
//! cargo run -p queuey --example memory_quickstart
//! RUST_LOG=debug cargo run -p queuey --example memory_quickstart
//! ```
//!
//! It enqueues six jobs: three that succeed, one that fails twice before
//! succeeding, one that fails fatally, and one that a rate limit defers before it
//! succeeds. Then it waits for all of them to settle, shuts the worker down
//! gracefully and prints what the backend saw.

use std::{
    error::Error,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use queuey::prelude::*;

/// Broker queue names, spelled out here so the summary can read them back.
const EMAILS: &str = "quickstart.emails";
const IMAGES: &str = "quickstart.images";
const API: &str = "quickstart.api";

/// Jobs that must reach a terminal state (success or dead letter) before we stop.
const EXPECTED: usize = 6;

/// Give up if the jobs have not settled by then.
const DEADLINE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
#[queues(prefix = "quickstart")]
enum AppQueues {
    /// Three attempts, 50ms apart, so the example finishes quickly.
    #[queue(
        prefetch = 8,
        retry(max_attempts = 3, backoff = "fixed", delay = "50ms")
    )]
    Emails,
    /// No retry policy: a failure here is dead-lettered immediately.
    #[queue(prefetch = 4)]
    Images,
    /// Ten priority levels (the default), which is what lets a deferred job come
    /// back ahead of whatever piled up while it was waiting.
    #[queue(prefetch = 4, max_priority = 10)]
    Api,
}

#[derive(Debug, Serialize, Deserialize, Job)]
#[job(queue = AppQueues::Emails)]
struct SendEmail {
    to: String,
    /// Fail this many times (retryably) before succeeding.
    flaky_for: u32,
    /// Fail fatally instead: no retry, straight to the dead-letter queue.
    fatal: bool,
}

#[derive(Debug, Serialize, Deserialize, Job)]
#[job(queue = AppQueues::Images, retry(max_attempts = 2, backoff = "exponential", base = "20ms", max = "100ms"))]
struct ResizeImage {
    path: String,
}

/// Calls an API that answers `429 Too Many Requests` the first time.
#[derive(Debug, Serialize, Deserialize, Job)]
#[job(queue = AppQueues::Api)]
struct CallApi {
    url: String,
    /// What the (simulated) `Retry-After` header says, in milliseconds. Real code
    /// would parse it off the response; 150ms keeps the example quick.
    retry_after_ms: u64,
}

/// Counts jobs that will never be seen again, so the example knows when to stop.
struct Settled(AtomicUsize);

impl Settled {
    fn bump(&self) -> usize {
        self.0.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

struct EmailHandler {
    settled: Arc<Settled>,
}

#[async_trait]
impl JobHandler for EmailHandler {
    type Job = SendEmail;

    async fn handle(&self, job: SendEmail, ctx: JobContext) -> Result<(), JobError> {
        tracing::info!(
            to = %job.to,
            attempt = ctx.attempt,
            max_attempts = ctx.max_attempts,
            last = ctx.is_last_attempt(),
            "handling email"
        );

        if job.fatal {
            // Fatal errors skip the retry policy entirely.
            self.settled.bump();
            return Err(JobError::fatal_msg(format!("{} is not a mailbox", job.to)));
        }
        if ctx.attempt <= job.flaky_for {
            return Err(JobError::retryable_msg("smtp connection reset"));
        }

        self.settled.bump();
        Ok(())
    }
}

/// Deferral: the call is *not* a failure, it just cannot happen yet.
struct ApiHandler {
    settled: Arc<Settled>,
}

#[async_trait]
impl JobHandler for ApiHandler {
    type Job = CallApi;

    async fn handle(&self, job: CallApi, ctx: JobContext) -> Result<(), JobError> {
        // Pretend the first call came back `429 Too Many Requests` with a
        // `Retry-After` header, and every later one succeeds.
        if ctx.deferrals == 0 {
            let retry_after = Duration::from_millis(job.retry_after_ms);
            tracing::info!(url = %job.url, ?retry_after, "429 Too Many Requests");
            // Waits exactly that long, leaves `attempt` alone, and comes back at
            // the queue's highest priority, ahead of the backlog.
            return Err(JobError::deferred_msg(
                retry_after,
                "rate limited (HTTP 429)",
            ));
        }

        tracing::info!(
            url = %job.url,
            attempt = ctx.attempt,
            deferrals = ctx.deferrals,
            priority = ctx.priority,
            "calling api"
        );
        self.settled.bump();
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,queuey_core=info".into()),
        )
        .init();

    let backend = Arc::new(MemoryBackend::new());
    let settled = Arc::new(Settled(AtomicUsize::new(0)));

    // Declares every queue in the set.
    let producer = Producer::<AppQueues, _>::new(backend.clone()).await?;

    let worker = Worker::<AppQueues, _>::builder(backend.clone())
        .handler(EmailHandler {
            settled: settled.clone(),
        })
        .handler(ApiHandler {
            settled: settled.clone(),
        })
        // The same thing without a struct: any async closure is a handler.
        .handler(FnHandler::<ResizeImage, _>::new({
            let settled = settled.clone();
            move |job: ResizeImage, ctx: JobContext| {
                let settled = settled.clone();
                async move {
                    tracing::info!(path = %job.path, attempt = ctx.attempt, "resizing image");
                    settled.bump();
                    Ok(())
                }
            }
        }))
        .build()
        .await?;

    let handle = worker.handle();
    let running = tokio::spawn(worker.run());

    for to in ["ada@example.com", "grace@example.com"] {
        producer
            .enqueue(&SendEmail {
                to: to.to_owned(),
                flaky_for: 0,
                fatal: false,
            })
            .await?;
    }
    producer
        .enqueue(&SendEmail {
            to: "flaky@example.com".to_owned(),
            flaky_for: 2,
            fatal: false,
        })
        .await?;
    producer
        .enqueue(&SendEmail {
            to: "not-an-address".to_owned(),
            flaky_for: 0,
            fatal: true,
        })
        .await?;
    producer
        .enqueue(&ResizeImage {
            path: "cat.png".to_owned(),
        })
        .await?;
    // Deferral, handler side: this one runs, hits the rate limit, waits out its
    // `Retry-After`, and then runs again with the same attempt number.
    producer
        .enqueue(&CallApi {
            url: "https://api.example.com/v1/things".to_owned(),
            retry_after_ms: 150,
        })
        .await?;

    let finished = tokio::time::timeout(DEADLINE, async {
        while settled.count() < EXPECTED {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    // Stops pulling new work, processes whatever was already pulled, waits for the
    // jobs in flight. The backend stays open: the producer above still shares it.
    handle.shutdown();
    running.await??;

    if finished.is_err() {
        return Err(format!(
            "only {}/{EXPECTED} jobs settled within {DEADLINE:?}",
            settled.count()
        )
        .into());
    }

    for queue in [EMAILS, IMAGES, API] {
        tracing::info!(
            queue,
            // Every retry acks the envelope it replaces, so this counts attempts.
            acked = backend.acked(queue).len(),
            dead = backend.dead_letters(queue).len(),
            pending = backend.pending(queue),
            "queue summary"
        );
        for (envelope, reason) in backend.dead_letters(queue) {
            tracing::warn!(job_id = %envelope.job_id, attempt = envelope.attempt, %reason, "dead letter");
        }
    }

    // Nothing else is going to use the backend, so close it explicitly.
    backend.close().await?;

    tracing::info!(settled = settled.count(), "done");
    Ok(())
}
