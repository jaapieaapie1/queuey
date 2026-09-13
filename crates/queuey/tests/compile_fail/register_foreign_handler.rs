//! A `Worker` is pinned to one queue set: a handler for another application's
//! job cannot be registered on it.

use queuey::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
#[queues(prefix = "app")]
enum AppQueues {
    Emails,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
#[queues(prefix = "other")]
enum OtherQueues {
    Reports,
}

#[derive(Serialize, Deserialize, Job)]
#[job(queue = AppQueues::Emails)]
struct SendEmail {
    to: String,
}

#[derive(Serialize, Deserialize, Job)]
#[job(queue = OtherQueues::Reports)]
struct BuildReport {
    id: u64,
}

struct EmailHandler;

#[async_trait]
impl JobHandler for EmailHandler {
    type Job = SendEmail;
    async fn handle(&self, _job: SendEmail, _ctx: JobContext) -> Result<(), JobError> {
        Ok(())
    }
}

struct ReportHandler;

#[async_trait]
impl JobHandler for ReportHandler {
    type Job = BuildReport;
    async fn handle(&self, _job: BuildReport, _ctx: JobContext) -> Result<(), JobError> {
        Ok(())
    }
}

async fn wrong_queue_set(backend: Arc<MemoryBackend>) {
    let _worker = Worker::<AppQueues, _>::builder(backend)
        // Fine: `EmailHandler::Job::Queue == AppQueues`.
        .handler(EmailHandler)
        // Not fine: `ReportHandler::Job::Queue == OtherQueues`.
        .handler(ReportHandler)
        .build()
        .await
        .unwrap();
}

fn main() {}
