use queuey_macros::Queues;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
enum AppQueues {
    #[queue(prefetch = 10, prefetch = 20)]
    Emails,
}

fn main() {}
