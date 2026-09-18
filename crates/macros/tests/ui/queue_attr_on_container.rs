use queuey_macros::Queues;

// `#[derive(Queues)]` registers both `queues` and `queue`, which makes both
// inert anywhere on the item. Before this was diagnosed, the whole attribute
// was dropped: `prefix` did nothing, `prefetch` did nothing, and `nonsense`
// was never reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
#[queue(prefix = "myapp", prefetch = 99, nonsense = 1)]
enum AppQueues {
    Emails,
}

fn main() {}
