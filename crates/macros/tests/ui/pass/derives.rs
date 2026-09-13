use queuey_core::{Job as _, QueueSet as _};
use queuey_macros::{Job, Queues};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
#[queues(prefix = "myapp")]
enum AppQueues {
    #[queue(prefetch = 10, max_priority = 5)]
    Emails,
    #[queue(
        name = "img",
        durable = false,
        message_ttl = "30s",
        max_priority = 0,
        retry(max_attempts = 3, backoff = "exponential", base = "1s", max = "2m")
    )]
    Images,
    HTTPCalls,
}

#[derive(Serialize, Deserialize, Job)]
#[job(queue = AppQueues::Emails, retry(max_attempts = 5))]
struct SendEmail {
    to: String,
    body: String,
}

#[derive(Serialize, Deserialize, Job)]
#[job(queue = AppQueues::Images, name = "images.resize")]
struct ResizeImage {
    path: String,
}

fn main() {
    assert_eq!(AppQueues::all().len(), 3);
    assert_eq!(AppQueues::Emails.name(), "myapp.emails");
    assert_eq!(AppQueues::HTTPCalls.name(), "myapp.http_calls");
    assert!(AppQueues::Images.config().message_ttl.is_some());
    assert_eq!(AppQueues::Emails.config().max_priority, Some(5));
    assert_eq!(AppQueues::Images.config().max_priority, None);
    assert_eq!(AppQueues::HTTPCalls.config().max_priority, Some(10));
    assert_eq!(SendEmail::QUEUE, AppQueues::Emails);
    assert!(SendEmail::retry_policy().is_some());
    assert_eq!(ResizeImage::NAME, "images.resize");
    assert!(ResizeImage::retry_policy().is_none());
}
