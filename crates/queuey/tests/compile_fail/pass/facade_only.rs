//! The positive control: a crate whose only dependency is `queuey`
//! derives both traits, builds a producer and a worker, and runs a job.

use queuey::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
#[queues(prefix = "app")]
enum AppQueues {
    #[queue(prefetch = 2, retry(max_attempts = 2, backoff = "fixed", delay = "1ms"))]
    Emails,
    #[queue(name = "img")]
    Images,
}

#[derive(Serialize, Deserialize, Job)]
#[job(queue = AppQueues::Emails)]
struct SendEmail {
    to: String,
}

#[derive(Serialize, Deserialize, Job)]
#[job(queue = AppQueues::Images, retry(max_attempts = 3))]
struct ResizeImage {
    path: String,
}

struct EmailHandler;

#[async_trait]
impl JobHandler for EmailHandler {
    type Job = SendEmail;
    async fn handle(&self, job: SendEmail, _ctx: JobContext) -> Result<(), JobError> {
        assert_eq!(job.to, "a@b.c");
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    assert_eq!(AppQueues::Emails.name(), "app.emails");
    assert_eq!(AppQueues::Images.name(), "app.img");
    assert_eq!(SendEmail::QUEUE, AppQueues::Emails);
    assert!(ResizeImage::retry_policy().is_some());

    let backend = Arc::new(MemoryBackend::new());
    let producer = Producer::<AppQueues, _>::new(backend.clone()).await.unwrap();
    producer
        .enqueue(&SendEmail {
            to: "a@b.c".to_owned(),
        })
        .await
        .unwrap();

    let worker = Worker::<AppQueues, _>::builder(backend.clone())
        .handler(EmailHandler)
        .handler(FnHandler::<ResizeImage, _>::new(
            |_job: ResizeImage, _ctx: JobContext| async move { Ok(()) },
        ))
        .build()
        .await
        .unwrap();
    let handle = worker.handle();
    let task = tokio::spawn(worker.run());

    for _ in 0..500 {
        if backend.acked("app.emails").len() == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    assert_eq!(backend.acked("app.emails").len(), 1);

    handle.shutdown();
    task.await.unwrap().unwrap();
}
