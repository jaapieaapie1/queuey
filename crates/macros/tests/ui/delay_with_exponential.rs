use queuey_macros::Queues;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
enum AppQueues {
    #[queue(retry(backoff = "exponential", delay = "1s"))]
    Emails,
}

fn main() {}
