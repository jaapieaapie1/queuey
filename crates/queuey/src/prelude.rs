//! Everything a typical application needs, in one import.
//!
//! ```
//! use queuey::prelude::*;
//! ```
//!
//! This pulls in the two derive macros, the traits they implement, the runtime
//! types, the dead-letter hook ([`DeadLetterHook`], [`DeadLetter`],
//! [`DeadLetterCause`], [`FnDeadLetterHook`]), [`MemoryBackend`] with [`AckKind`],
//! [`DEFAULT_MAX_PRIORITY`], and the three third-party
//! items that user code cannot avoid naming: [`macro@async_trait`],
//! [`Serialize`]/[`Deserialize`] and [`Arc`].
//!
// The link only resolves when the re-export below exists, and a broken
// intra-doc link is a warning, which this workspace treats as an error.
#![cfg_attr(
    feature = "rabbitmq",
    doc = "With the default `rabbitmq` feature it also re-exports [`RabbitMqBackend`]."
)]
#![cfg_attr(
    not(feature = "rabbitmq"),
    doc = "The default `rabbitmq` feature, which would also re-export `RabbitMqBackend`, is off in this build."
)]
//!
//! Deriving [`Serialize`]/[`Deserialize`] through this prelude still requires
//! `serde` in the calling crate's `Cargo.toml`: the derive expands to code that
//! names the `serde` crate directly.

#[doc(no_inline)]
pub use queuey_core::{
    AckKind, Backend, Backoff, DEFAULT_MAX_PRIORITY, DeadLetter, DeadLetterCause, DeadLetterHook,
    EnqueueOptions, FnDeadLetterHook, FnHandler, Job, JobContext, JobError, JobHandler,
    MemoryBackend, Producer, QueueConfig, QueueSet, RetryPolicy, Worker, WorkerBuilder,
    WorkerHandle,
};

#[doc(no_inline)]
pub use queuey_macros::{Job, Queues};

#[cfg(feature = "rabbitmq")]
#[doc(no_inline)]
pub use queuey_rabbitmq::RabbitMqBackend;

#[doc(no_inline)]
pub use async_trait::async_trait;
#[doc(no_inline)]
pub use serde::{Deserialize, Serialize};
#[doc(no_inline)]
pub use std::sync::Arc;
