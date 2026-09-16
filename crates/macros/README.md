# queuey-macros

The derive macros behind [queuey](https://crates.io/crates/queuey): `#[derive(Queues)]`
and `#[derive(Job)]`.

Nothing depends on this crate directly. The `queuey` facade re-exports both derives, and
the generated code resolves its path to `queuey-core` through whichever of the two crates
the calling `Cargo.toml` names, so applications need one dependency and no
`crate = "..."` attribute.

```rust,ignore
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
#[queues(prefix = "myapp")]
enum AppQueues {
    #[queue(prefetch = 10, retry(max_attempts = 3, backoff = "exponential", base = "1s"))]
    Emails,
}

#[derive(Debug, Serialize, Deserialize, Job)]
#[job(queue = AppQueues::Emails, retry(max_attempts = 5))]
struct SendEmail { to: String }
```

Every mistake is a compile error spanned on the offending token: an empty queue name, a
duplicate resolved name, `prefetch = 0`, a zero duration, `base` greater than `max`, an
unknown key. Each one is pinned by a `trybuild` case with its expected stderr.

The full attribute grammar is documented in the
[facade README](https://crates.io/crates/queuey).

## License

MIT OR Apache-2.0
