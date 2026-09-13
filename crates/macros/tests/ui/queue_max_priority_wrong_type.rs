use queuey_macros::Queues;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
enum AppQueues {
    #[queue(max_priority = "10")]
    Emails,
}

fn main() {}
