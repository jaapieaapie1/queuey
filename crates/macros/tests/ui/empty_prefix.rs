use queuey_macros::Queues;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
#[queues(prefix = "")]
enum AppQueues {
    Emails,
}

fn main() {}
