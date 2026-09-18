//! Derive macros for [`queuey`](https://docs.rs/queuey).
//!
//! * [`macro@Queues`] implements `queuey_core::QueueSet` for a fieldless enum.
//! * [`macro@Job`] implements `queuey_core::Job` for a serializable payload type.
//!
//! # Where the generated code points
//!
//! Generated code needs a path to `queuey-core`. Both macros work that
//! out from the *calling* crate's `Cargo.toml` (via `proc-macro-crate`):
//!
//! 1. a dependency on `queuey-core`, which emits `::queuey_core`;
//! 2. otherwise a dependency on `queuey` (the facade), which emits
//!    `::queuey::__core`, its hidden re-export of the core crate;
//! 3. otherwise a compile error naming the `crate = "..."` escape hatch below,
//!    unless there was no manifest to read at all (a non-cargo build), in which
//!    case it guesses `::queuey_core`.
//!
//! The core crate wins because a manifest is not a build graph: it does not say
//! which target is compiling, and `[dev-dependencies]` are read along with
//! `[dependencies]`. A crate that depends on `queuey-core` and keeps the facade
//! for its tests only would otherwise be handed `::queuey::__core` in its lib,
//! where the facade is not linked. `::queuey_core` is correct whenever the core
//! crate is in the manifest at all, since the facade only re-exports it.
//!
//! Renamed dependencies (`aq = { package = "queuey" }`) are handled. For
//! anything else (a vendored copy, a re-export under yet another name) say so
//! explicitly with `#[queues(crate = "...")]` / `#[job(crate = "...")]`, which
//! always wins and never reads the manifest.
//!
//! # Duration literals
//!
//! Every duration in these attributes is a string literal parsed while the macro
//! runs: an integer followed by an optional unit of `ms`, `s`, `m`, `h` or `d`.
//! A bare integer means seconds. Whitespace is ignored, so `"500ms"`, `"30s"`,
//! `"2 m"` and `"30"` are all valid. Anything else is a compile error pointing at
//! the literal. Zero is rejected everywhere a duration is accepted: a zero
//! `message_ttl` discards every message on publish, and a zero backoff is
//! spelled `backoff = "none"`.
//!
//! # `retry(...)` grammar
//!
//! Shared by `#[queue(...)]` and `#[job(...)]`:
//!
//! ```text
//! retry(
//!     max_attempts = 3,            // u32 >= 1, default 3 (1 means no retries)
//!     backoff = "exponential",     // "none" | "fixed" | "exponential", default "exponential"
//!     delay = "1s",                // fixed only, required for "fixed"
//!     base = "1s",                 // exponential only, default "1s", must be <= max
//!     factor = 2.0,                // exponential only, default 2.0, must be > 0
//!     max = "5m",                  // exponential only, default "5m"
//!     jitter = true,               // exponential only, default true
//! )
//! ```
//!
//! The exponential defaults match `queuey_core::Backoff::exponential()`.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(unreachable_pub)]

mod attrs;
mod duration;
mod job;
mod queues;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

/// Implement `queuey_core::QueueSet` for a fieldless enum.
///
/// The enum must also derive the trait's supertraits; the macro deliberately does
/// not add them for you:
///
/// ```text
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
/// ```
///
/// # Attributes
///
/// Container attribute `#[queues(...)]`, optional:
///
/// ```text
/// #[queues(
///     prefix = "myapp",                  // queue names become "myapp.<name>"
///     crate = "queuey",         // path to the core crate re-export
/// )]
/// ```
///
/// Variant attribute `#[queue(...)]`, optional on every variant:
///
/// ```text
/// #[queue(
///     name = "img",                      // default: snake_case of the variant
///     prefetch = 10,                     // u16
///     durable = true,                    // bool
///     message_ttl = "30s",               // duration literal
///     max_priority = 10,                 // u8 in 0..=255; 0 disables priorities
///     retry(max_attempts = 3, backoff = "exponential", base = "1s", factor = 2.0,
///           max = "5m", jitter = true),
/// )]
/// ```
///
/// `max_priority` is the number of AMQP priority levels the queue is declared
/// with (`x-max-priority`). Omitted, the `QueueConfig` default applies;
/// `max_priority = 0` turns priorities off, so the queue carries no
/// `x-max-priority` argument at all. **Changing this value on a queue
/// that already exists is refused by the broker**: RabbitMQ answers a redeclare
/// with different arguments with `PRECONDITION_FAILED`, so an existing
/// deployment must delete the queue first.
///
/// # Example
///
/// ```
/// use queuey_core::{QueueSet, Backoff};
/// use queuey_macros::Queues;
///
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
/// #[queues(prefix = "myapp")]
/// enum AppQueues {
///     #[queue(prefetch = 10, max_priority = 5)]
///     Emails,
///     #[queue(name = "img", message_ttl = "30s", max_priority = 0, retry(max_attempts = 5))]
///     ImageResize,
/// }
///
/// assert_eq!(AppQueues::Emails.name(), "myapp.emails");
/// assert_eq!(AppQueues::ImageResize.name(), "myapp.img");
/// assert_eq!(AppQueues::Emails.config().prefetch, 10);
/// assert_eq!(AppQueues::Emails.config().max_priority, Some(5));
/// assert_eq!(AppQueues::ImageResize.config().max_priority, None);
/// assert_eq!(AppQueues::from_name("myapp.img"), Some(AppQueues::ImageResize));
/// ```
///
/// # Errors
///
/// Compile errors, spanned at the offending token, are produced for: a non-enum
/// item, a generic enum, an enum without variants, a variant with fields,
/// duplicate resolved queue names, unknown or duplicated attribute keys, a
/// literal of the wrong type, an empty `prefix`/`name`/resolved queue name, a
/// `prefetch` outside `1..=65535` (`0` means *unlimited* in AMQP, so omit the key
/// instead), a `max_priority` outside `0..=255`, a `max_attempts` outside
/// `1..=u32::MAX`, a malformed or zero
/// duration, an unknown backoff kind, `base` greater than `max`, `delay` outside
/// of `backoff = "fixed"`, and `base`/`factor`/`max`/`jitter` outside of
/// `backoff = "exponential"`.
#[proc_macro_derive(Queues, attributes(queues, queue))]
pub fn derive_queues(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    queues::derive(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Implement `queuey_core::Job` for a struct or enum.
///
/// The type must also be `Serialize + DeserializeOwned`; add those derives
/// yourself, this macro never generates them.
///
/// # Attributes
///
/// ```text
/// #[job(
///     queue = AppQueues::Emails,         // required; Job::Queue = AppQueues
///     name = "emails.send",              // default: module_path!() + "::" + type name
///     retry(max_attempts = 5),           // optional; generates retry_policy()
///     crate = "queuey",         // path to the core crate re-export
/// )]
/// ```
///
/// `queue` is a path with at least two segments: the last segment is the variant
/// (`Job::QUEUE`) and everything before it is the queue set type (`Job::Queue`).
///
/// # Example
///
/// ```
/// use queuey_core::Job;
/// use queuey_macros::{Job, Queues};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
/// enum AppQueues {
///     Emails,
/// }
///
/// #[derive(Job, Serialize, Deserialize)]
/// #[job(queue = AppQueues::Emails, retry(max_attempts = 5, backoff = "fixed", delay = "2s"))]
/// struct SendEmail {
///     to: String,
/// }
///
/// assert_eq!(SendEmail::QUEUE, AppQueues::Emails);
/// assert!(SendEmail::retry_policy().is_some());
/// ```
///
/// # Errors
///
/// Compile errors, spanned at the offending token, are produced for: a missing
/// `queue` key, a `queue` path with a single segment, a generic type, unknown or
/// duplicated attribute keys, and every `retry(...)` error listed on
/// [`macro@Queues`].
#[proc_macro_derive(Job, attributes(job))]
pub fn derive_job(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    job::derive(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
