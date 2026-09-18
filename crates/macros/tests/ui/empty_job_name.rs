use queuey_macros::{Job, Queues};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
enum AppQueues {
    Emails,
}

// `Job::NAME` is the envelope's `job_type` and the handler-dispatch key.
#[derive(Serialize, Deserialize, Job)]
#[job(queue = AppQueues::Emails, name = "")]
struct SendEmail {
    to: String,
}

fn main() {}
