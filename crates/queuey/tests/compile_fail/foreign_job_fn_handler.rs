//! The same guarantee for closure handlers: `FnHandler` does not launder a job
//! from one queue set into a worker for another.

use queuey::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
enum AppQueues {
    Emails,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
enum OtherQueues {
    Reports,
}

#[derive(Serialize, Deserialize, Job)]
#[job(queue = AppQueues::Emails)]
struct SendEmail {
    to: String,
}

/// Belongs to `OtherQueues`, so no `AppQueues` worker may handle it.
#[derive(Serialize, Deserialize, Job)]
#[job(queue = OtherQueues::Reports, retry(max_attempts = 2))]
struct BuildReport {
    id: u64,
}

async fn wrong_queue_set(backend: Arc<MemoryBackend>) {
    let _worker = Worker::<AppQueues, _>::builder(backend)
        .handler(FnHandler::<SendEmail, _>::new(
            |_job: SendEmail, _ctx: JobContext| async move { Ok(()) },
        ))
        .handler(FnHandler::<BuildReport, _>::new(
            |_job: BuildReport, _ctx: JobContext| async move { Ok(()) },
        ))
        .build()
        .await
        .unwrap();
}

fn main() {}
