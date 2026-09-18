use queuey_macros::Queues;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
enum AppQueues {
    #[queues(prefix = "myapp")]
    Emails,
}

fn main() {}
