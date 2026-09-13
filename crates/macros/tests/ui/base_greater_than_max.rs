use queuey_macros::Queues;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
enum AppQueues {
    #[queue(retry(backoff = "exponential", base = "10s", max = "1s"))]
    Emails,
}

fn main() {}
