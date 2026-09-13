//! Core abstractions for `queuey`.
//!
//! This crate is transport-agnostic. It defines:
//!
//! * [`QueueSet`]: implemented (usually via `#[derive(Queues)]`) by an enum whose
//!   variants are the queues of an application.
//! * [`Job`]: implemented (usually via `#[derive(Job)]`) by a serializable payload
//!   type. A job statically knows which queue (variant) it belongs to.
//! * [`JobHandler`]: user code that processes one job type.
//! * [`Backend`]: a message transport (RabbitMQ, in-memory, ...).
//! * [`Producer`] / [`Worker`]: the runtime that ties everything together.
//! * [`RetryPolicy`] / [`Backoff`]: optional (exponential) retry configuration.
//! * Deferral: a handler returns [`JobError::Deferred`] (or a producer calls
//!   [`Producer::defer`]) to park a job for an exact delay and have it come back
//!   *ahead* of the backlog, at [`QueueConfig::max_priority`], without spending an
//!   attempt. The motivating case is an API answering `429` with `Retry-After`.
//!
//! Module structure and public API below is the *contract* shared by the macro crate
//! and the backend crates. Keep signatures stable; extend rather than change.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod backend;
pub mod envelope;
pub mod error;
pub mod handler;
pub mod job;
pub mod memory;
pub mod producer;
pub mod queue;
pub mod retry;
#[cfg(test)]
pub(crate) mod test_support;
pub mod worker;

pub use backend::{Backend, Delivery, DeliveryStream};
pub use envelope::Envelope;
pub use error::{Error, JobError, Result};
pub use handler::{FnHandler, JobContext, JobHandler};
pub use job::Job;
pub use memory::MemoryBackend;
pub use producer::Producer;
pub use queue::{DEFAULT_MAX_PRIORITY, QueueConfig, QueueSet};
pub use retry::{Backoff, RetryDecision, RetryPolicy};
pub use worker::{Worker, WorkerBuilder, WorkerHandle};

/// Re-exports needed by generated code from `queuey-macros`.
/// Not part of the stable public API.
#[doc(hidden)]
pub mod __private {
    pub use serde;
    pub use serde_json;
}
