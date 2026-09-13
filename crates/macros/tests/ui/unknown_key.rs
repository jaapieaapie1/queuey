use queuey_macros::Queues;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
enum AppQueues {
    #[queue(prefetch = 10, concurrency = 4)]
    Emails,
}

fn main() {}
