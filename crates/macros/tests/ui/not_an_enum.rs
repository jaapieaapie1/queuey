use queuey_macros::Queues;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
struct NotAnEnum {
    field: u8,
}

fn main() {}
