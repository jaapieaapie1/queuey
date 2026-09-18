use queuey_macros::Queues;

// This used to expand to the queue name `myapp..emails`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
#[queues(prefix = "myapp.")]
enum AppQueues {
    Emails,
}

fn main() {}
