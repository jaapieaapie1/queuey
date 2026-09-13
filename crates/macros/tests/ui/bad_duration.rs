use queuey_macros::Queues;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
enum AppQueues {
    #[queue(message_ttl = "30 fortnights")]
    Emails,
}

fn main() {}
