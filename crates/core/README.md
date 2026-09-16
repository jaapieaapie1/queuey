# queuey-core

The transport-agnostic core of [queuey](https://crates.io/crates/queuey): the traits
every other crate in the workspace is written against.

Applications depend on the `queuey` facade instead, which re-exports everything here
plus the derive macros and the RabbitMQ backend. Depend on this crate directly only to
**write a backend**: implement `Backend` and `Delivery` for your broker and the
`Producer`, `Worker`, retry policies, deferral and dead-letter machinery come with it.

| item | role |
|---|---|
| `Backend`, `Delivery`, `DeliveryStream` | the transport contract; publish before ack, nothing released early |
| `Envelope` | the wire format, frozen for 1.x: fields are only ever added, always `#[serde(default)]` |
| `Producer`, `EnqueueOptions` | type-safe publishing, delays, deferrals, correlation ids |
| `Worker`, `WorkerBuilder`, `WorkerHandle` | the runtime: dispatch, concurrency, timeouts, graceful shutdown |
| `JobHandler`, `FnHandler`, `JobContext`, `JobError` | what user code implements and returns |
| `RetryPolicy`, `Backoff`, `RetryDecision` | attempts and backoff, including full jitter |
| `DeadLetterHook`, `DeadLetter`, `DeadLetterCause` | one callback for every job a worker gives up on |
| `MemoryBackend`, `AckKind` | an in-process backend for tests, honouring `tokio::time` |

`Backend` and `Delivery` are meant to be implemented outside this workspace: for the
whole of 1.x they only ever gain methods that have a default implementation.

See the [workspace README](https://github.com/jaapieaapie1/queuey) and
`ARCHITECTURE.md` for the design, the RabbitMQ topology and the retry/deferral
semantics in full.

## License

MIT OR Apache-2.0
