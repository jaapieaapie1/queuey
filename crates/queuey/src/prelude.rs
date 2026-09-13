//! Everything a typical application needs, in one import.
//!
//! ```
//! use queuey::prelude::*;
//! ```
//!
//! This pulls in the two derive macros, the traits they implement, the runtime
//! types, [`MemoryBackend`], [`DEFAULT_MAX_PRIORITY`], and the three third-party
//! items that user code cannot avoid naming: [`macro@async_trait`],
//! [`Serialize`]/[`Deserialize`] and [`Arc`]. With the default `rabbitmq` feature
//! it also re-exports [`RabbitMqBackend`].
//!
//! Deriving [`Serialize`]/[`Deserialize`] through this prelude still requires
//! `serde` in the calling crate's `Cargo.toml`: the derive expands to code that
//! names the `serde` crate directly.

#[doc(no_inline)]
pub use queuey_core::{
    Backend, Backoff, DEFAULT_MAX_PRIORITY, FnHandler, Job, JobContext, JobError, JobHandler,
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
