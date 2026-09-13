use queuey_macros::Queues;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
enum AppQueues {
    #[queue(retry(max_attempts = 0))]
    Emails,
}

fn main() {}
