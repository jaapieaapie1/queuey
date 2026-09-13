//! A `Producer` is pinned to one queue set: it cannot enqueue another
//! application's job, even though that job is a perfectly valid `Job`.

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

async fn wrong_queue_set(producer: Producer<AppQueues, MemoryBackend>) {
    // Fine: `SendEmail::Queue == AppQueues`.
    producer.enqueue(&SendEmail { to: "a@b.c".into() }).await.unwrap();

    // Not fine: `BuildReport::Queue == OtherQueues`.
    producer.enqueue(&BuildReport { id: 1 }).await.unwrap();
}

fn main() {}
