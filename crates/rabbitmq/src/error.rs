//! Error mapping between `lapin` and [`queuey_core::Error`].
//!
//! [`queuey_core::Error`] is a foreign type and `lapin::Error` is a
//! foreign type, so `From` cannot be implemented here. Everything funnels
//! through [`amqp`] instead, used as `.map_err(amqp)?`.

use lapin::types::ShortString;
use queuey_core::Error;
use thiserror::Error as ThisError;

/// Wrap a `lapin` error as an [`queuey_core::Error::Backend`].
pub(crate) fn amqp(error: lapin::Error) -> Error {
    Error::backend(error)
}

/// Failures that originate in this backend rather than in `lapin` itself.
#[derive(Debug, ThisError)]
pub(crate) enum RabbitMqError {
    /// The broker explicitly refused to take responsibility for a message.
    #[error("broker nacked the message published to `{queue}`")]
    Nacked {
        /// Queue the message was routed to.
        queue: String,
    },

    /// The broker accepted the frames but could not route them to any queue, so
    /// it handed the message back (`basic.return`). Every publish is
    /// `mandatory`, so this is what an unknown or undeclared queue looks like:
    /// it must never be mistaken for a successful publish.
    #[error(
        "broker returned the message published to `{routing_key}` as unroutable: {reply_code} {reply_text}"
    )]
    Returned {
        /// AMQP reply code from the `basic.return`, e.g. `312 NO_ROUTE`.
        reply_code: u16,
        /// Human-readable reason from the `basic.return`.
        reply_text: String,
        /// Routing key (queue name) the message was published with.
        routing_key: String,
    },

    /// `confirm_select` was never accepted, so a publish cannot be confirmed.
    /// Acking the original delivery anyway would risk losing the message.
    #[error("publisher confirms are not enabled; cannot confirm publish to `{queue}`")]
    ConfirmsNotEnabled {
        /// Queue the message was routed to.
        queue: String,
    },

    /// A delivery could not be settled: its acker had already been used, or the
    /// channel it belongs to is gone. Either way the broker did not record the
    /// outcome the caller asked for, so this is reported rather than swallowed.
    #[error(
        "cannot {operation} delivery of job `{job_id}` on `{queue}`: it was already settled or its channel is gone"
    )]
    AlreadySettled {
        /// What was attempted: `"ack"` or `"reject"`.
        operation: &'static str,
        /// Job the delivery carried.
        job_id: String,
        /// Queue the delivery came from.
        queue: String,
    },

    /// A queue name (or other AMQP short string) exceeded 255 bytes.
    #[error("invalid AMQP name `{name}`: {source}")]
    InvalidName {
        /// The offending value.
        name: String,
        /// Why it was rejected.
        #[source]
        source: lapin::types::ShortStringError,
    },

    /// A retry, delayed enqueue or deferral asked for a delay longer than a
    /// hold queue can express.
    ///
    /// A hold queue times the wait with `x-message-ttl` and outlives it with
    /// `x-expires = 2 * ttl`, both 32-bit millisecond counts, so the longest
    /// holdable delay is
    /// [`MAX_DEFERRAL_MS`](crate::topology::MAX_DEFERRAL_MS) (~24.8 days).
    /// Anything longer is refused rather than shortened: releasing a job early
    /// would break the one timing guarantee a hold makes. Rounding up to the
    /// granularity happens first, so a delay just under the cap can be refused
    /// too.
    #[error(
        "delay of {requested:?} is longer than a hold queue can wait ({max:?}); it was refused rather than released early"
    )]
    DelayTooLong {
        /// Delay the caller asked for.
        requested: std::time::Duration,
        /// Longest delay this backend can hold.
        max: std::time::Duration,
    },

    /// A queue name is short enough for AMQP itself, but its hold queue names
    /// are not, so deferring on it could fail per job.
    ///
    /// Caught at [`declare`](queuey_core::Backend::declare) time
    /// against the *longest* possible hold queue name, the one for
    /// [`MAX_DEFERRAL_MS`](crate::topology::MAX_DEFERRAL_MS), so a shorter name
    /// is demanded up front rather than a deferral failing in production on the
    /// day someone picks a long delay.
    #[error(
        "queue `{queue}` leaves no room for its hold queues: `{hold}` is {length} bytes, over the {limit}-byte AMQP limit"
    )]
    DeferredNameTooLong {
        /// The queue that was declared.
        queue: String,
        /// The longest hold queue name it would need.
        hold: String,
        /// Length of `hold` in bytes.
        length: usize,
        /// The AMQP short string limit.
        limit: usize,
    },
}

impl RabbitMqError {
    /// Convert into the core error type.
    pub(crate) fn into_core(self) -> Error {
        Error::backend(self)
    }
}

/// Convert `value` into an AMQP short string, erroring instead of panicking.
///
/// `ShortString::from` panics past 255 bytes; queue names come from user
/// configuration, so this must be a recoverable error rather than a panic.
pub(crate) fn short_string(value: &str) -> Result<ShortString, Error> {
    ShortString::try_new(value).map_err(|source| {
        RabbitMqError::InvalidName {
            name: value.to_owned(),
            source,
        }
        .into_core()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_string_accepts_normal_queue_names() {
        assert_eq!(
            short_string("myapp.emails").unwrap().as_str(),
            "myapp.emails"
        );
    }

    #[test]
    fn short_string_accepts_exactly_255_bytes() {
        let name = "a".repeat(255);
        assert_eq!(short_string(&name).unwrap().as_str(), name);
    }

    #[test]
    fn short_string_rejects_over_long_names_without_panicking() {
        let name = "a".repeat(256);
        let err = short_string(&name).expect_err("expected rejection");
        assert!(matches!(err, Error::Backend(_)), "got {err:?}");
        assert!(err.to_string().contains("invalid AMQP name"));
    }

    #[test]
    fn nack_error_names_the_queue() {
        let err = RabbitMqError::Nacked {
            queue: "emails.deferred.1000".to_owned(),
        };
        assert_eq!(
            err.to_string(),
            "broker nacked the message published to `emails.deferred.1000`"
        );
    }

    #[test]
    fn returned_error_carries_the_brokers_reply() {
        let err = RabbitMqError::Returned {
            reply_code: 312,
            reply_text: "NO_ROUTE".to_owned(),
            routing_key: "emails.dead".to_owned(),
        };
        let text = err.to_string();
        assert!(text.contains("emails.dead"), "{text}");
        assert!(text.contains("312"), "{text}");
        assert!(text.contains("NO_ROUTE"), "{text}");
    }

    #[test]
    fn already_settled_error_names_the_operation_and_job() {
        let err = RabbitMqError::AlreadySettled {
            operation: "ack",
            job_id: "67e55044-10b1-426f-9247-bb680e5fe0c8".to_owned(),
            queue: "emails".to_owned(),
        };
        let text = err.to_string();
        assert!(text.contains("cannot ack delivery"), "{text}");
        assert!(
            text.contains("67e55044-10b1-426f-9247-bb680e5fe0c8"),
            "{text}"
        );
        assert!(text.contains("emails"), "{text}");
    }

    #[test]
    fn delay_too_long_says_it_was_refused_not_shortened() {
        let err = RabbitMqError::DelayTooLong {
            requested: std::time::Duration::from_secs(30 * 86_400),
            max: std::time::Duration::from_millis(u64::from(crate::topology::MAX_DEFERRAL_MS)),
        };
        let text = err.to_string();
        assert!(text.contains("longer than a hold queue can wait"), "{text}");
        assert!(
            text.contains("refused rather than released early"),
            "{text}"
        );
        assert!(matches!(err.into_core(), Error::Backend(_)));
    }

    #[test]
    fn deferred_name_too_long_names_both_queues() {
        let err = RabbitMqError::DeferredNameTooLong {
            queue: "q".repeat(250),
            hold: format!("{}.deferred.2147483647", "q".repeat(250)),
            length: 270,
            limit: 255,
        };
        let text = err.to_string();
        assert!(
            text.contains("leaves no room for its hold queues"),
            "{text}"
        );
        assert!(text.contains("270 bytes"), "{text}");
        assert!(text.contains("255-byte"), "{text}");
    }

    #[test]
    fn confirms_not_enabled_error_names_the_queue() {
        let err = RabbitMqError::ConfirmsNotEnabled {
            queue: "emails".to_owned(),
        };
        assert!(
            err.to_string()
                .contains("publisher confirms are not enabled")
        );
    }
}
