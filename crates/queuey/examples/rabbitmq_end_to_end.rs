//! The same tour as `memory_quickstart`, but against a real broker.
//!
//! ```sh
//! docker run --rm -d -p 5672:5672 -p 15672:15672 rabbitmq:4-management
//! cargo run -p queuey --example rabbitmq_end_to_end
//! AMQP_URL=amqp://guest:guest@rabbit:5672/%2f cargo run -p queuey --example rabbitmq_end_to_end
//! ```
//!
//! It declares the queues (plus their `.dead` companions), enqueues
//! three jobs (one of which fails twice before succeeding), runs a worker until
//! all three have settled, and shuts down. Ctrl-C also stops it cleanly.

use std::{
    error::Error,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use queuey::prelude::*;

/// Broker to talk to when `AMQP_URL` is not set.
const DEFAULT_URL: &str = "amqp://guest:guest@localhost:5672/%2f";

/// Jobs that must reach a terminal state before the worker is asked to stop.
const EXPECTED: usize = 3;

/// Give up (with a non-zero exit) if the jobs have not settled by then.
const DEADLINE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
#[queues(prefix = "aq-example")]
enum AppQueues {
    /// Three attempts with a short exponential backoff, so the retries are visible
    /// in the broker's management UI without being slow.
    #[queue(
        prefetch = 8,
        retry(
            max_attempts = 3,
            backoff = "exponential",
            base = "500ms",
            max = "5s",
            jitter = false
        )
    )]
    Emails,
}

#[derive(Debug, Serialize, Deserialize, Job)]
#[job(queue = AppQueues::Emails)]
struct SendEmail {
    to: String,
    /// Fail this many times (retryably) before succeeding.
    flaky_for: u32,
}

/// Counts jobs that will never be seen again.
struct Settled(AtomicUsize);

impl Settled {
    fn bump(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
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
            age = ?ctx.age,
            "handling email"
        );

        if ctx.attempt <= job.flaky_for {
            return Err(JobError::retryable_msg("smtp connection reset"));
        }

        self.settled.bump();
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let url = std::env::var("AMQP_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned());
    tracing::info!(%url, "connecting");
    let backend = Arc::new(RabbitMqBackend::connect(&url).await?);

    let settled = Arc::new(Settled(AtomicUsize::new(0)));

    // Declares `aq-example.emails` and `aq-example.emails.dead`. Hold queues for
    // retry delays appear on demand and expire on their own.
    let producer = Producer::<AppQueues, _>::new(backend.clone()).await?;

    let worker = Worker::<AppQueues, _>::builder(backend)
        // This process exists to run the worker, so let it close the connection too.
        // (The default is to leave the shared backend alone.)
        .close_backend_on_shutdown(true)
        .handler(EmailHandler {
            settled: settled.clone(),
        })
        .build()
        .await?;
    let handle = worker.handle();
    let running = tokio::spawn(worker.run());

    for (to, flaky_for) in [
        ("ada@example.com", 0),
        ("grace@example.com", 0),
        ("flaky@example.com", 2),
    ] {
        let id = producer
            .enqueue(&SendEmail {
                to: to.to_owned(),
                flaky_for,
            })
            .await?;
        tracing::info!(%id, to, flaky_for, "enqueued");
    }

    let all_settled = async {
        while settled.count() < EXPECTED {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };

    let timed_out = tokio::select! {
        () = all_settled => false,
        _ = tokio::time::sleep(DEADLINE) => true,
        _ = tokio::signal::ctrl_c() => {
            tracing::warn!("interrupted; shutting down");
            false
        }
    };

    // Graceful: stop consuming, finish the jobs in flight, close the connection.
    tracing::info!(settled = settled.count(), "shutting down");
    handle.shutdown();
    running.await??;

    if timed_out {
        return Err(format!(
            "only {}/{EXPECTED} jobs settled within {DEADLINE:?}",
            settled.count()
        )
        .into());
    }
    tracing::info!("done");
    Ok(())
}
