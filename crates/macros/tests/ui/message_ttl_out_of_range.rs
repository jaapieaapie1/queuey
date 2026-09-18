use queuey_macros::Queues;

// A broker stores `x-message-ttl` as unsigned 32-bit milliseconds (~49.7 days),
// so this used to be accepted here and silently clamped by the backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
enum AppQueues {
    #[queue(message_ttl = "100d")]
    Emails,
}

fn main() {}
