# queuey-macros

The derive macros behind [queuey](https://crates.io/crates/queuey): `#[derive(Queues)]`
and `#[derive(Job)]`.

Nothing depends on this crate directly. The `queuey` facade re-exports both derives, and the
generated code resolves its path to `queuey-core` through whichever of the two crates the
calling `Cargo.toml` names — `queuey-core` first, then the facade — so applications need one
dependency and no `crate = "..."` attribute. The core crate wins because a manifest does not
say which target is compiling and `[dev-dependencies]` are read along with `[dependencies]`:
`::queuey_core` is right whenever the core crate is named at all, since the facade only
re-exports it. If neither is named, the derive says so and points at
`#[queues(crate = "...")]` / `#[job(crate = "...")]`, which always win.

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

Every mistake is a compile error spanned on the offending token: a queue or job name that is
empty or only whitespace, a duplicate resolved name, a `prefix` ending in the `.` the macro
adds itself, `prefetch = 0`, a zero duration, a `message_ttl` past the 32-bit millisecond
`x-message-ttl` a broker can store, `base` greater than `max`, an unknown key, and a helper
attribute on the wrong level (`#[queue(...)]` on the enum, `#[queues(...)]` on a variant),
which `attributes(queues, queue)` makes inert and which would otherwise be dropped in
silence. Each one is pinned by a `trybuild` case with its expected stderr.

The full attribute grammar is documented in the
[facade README](https://crates.io/crates/queuey).

## License

MIT OR Apache-2.0. Both texts (`LICENSE-MIT`, `LICENSE-APACHE`) ship inside this crate.
