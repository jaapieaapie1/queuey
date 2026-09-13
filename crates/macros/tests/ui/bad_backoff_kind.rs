use queuey_macros::Queues;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
enum AppQueues {
    #[queue(retry(max_attempts = 3, backoff = "linear"))]
    Emails,
}

fn main() {}
