use queuey_macros::{Job, Queues};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
enum AppQueues {
    Emails,
}

#[derive(Serialize, Deserialize, Job)]
#[job(queue = AppQueues::Emails)]
struct SendEmail<T> {
    payload: T,
}

fn main() {}
