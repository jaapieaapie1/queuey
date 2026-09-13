use queuey_macros::Queues;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
#[queues(prefix = "myapp")]
enum AppQueues {
    #[queue(name = "emails")]
    Emails,
    #[queue(name = "emails")]
    Mail,
}

fn main() {}
