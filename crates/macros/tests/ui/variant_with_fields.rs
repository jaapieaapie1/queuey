use queuey_macros::Queues;

// `Copy` is intentionally not derived here: the payload field would add a
// second, unrelated error to the expected output.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Queues)]
enum AppQueues {
    Emails,
    Images(String),
}

fn main() {}
