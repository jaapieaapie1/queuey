use queuey_macros::Job;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Job)]
#[job(queue = AppQueues::Emails, retry(backoff = "fixed", delay = "0ms"))]
struct SendEmail {
    to: String,
}

fn main() {}
