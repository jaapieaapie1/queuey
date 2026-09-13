use queuey_macros::Queues;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
enum AppQueues {
    #[queue(message_ttl = "0s")]
    Emails,
}

fn main() {}
