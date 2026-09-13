//! Pure mapping between [`Envelope`] and AMQP [`BasicProperties`] / headers.
//!
//! Nothing here touches the broker, so every function can be unit-tested
//! without a running RabbitMQ.

use lapin::{
    BasicProperties,
    types::{AMQPValue, FieldTable, LongString, MAX_SHORT_STRING_LENGTH, ShortString},
};
use queuey_core::Envelope;

use crate::topology::{
    HEADER_ATTEMPT, HEADER_ATTEMPTS, HEADER_DEATH_REASON, HEADER_DEFERRALS, HEADER_ORIGINAL_QUEUE,
};

/// `content-type` set on every published message.
pub const CONTENT_TYPE_JSON: &str = "application/json";

/// `delivery-mode` for a persistent message.
pub const DELIVERY_MODE_PERSISTENT: u8 = 2;

/// Reason recorded on bodies that could not be decoded as an [`Envelope`].
pub const REASON_MALFORMED: &str = "malformed envelope";

/// Headers carried by every published envelope.
///
/// `x-attempt` and `x-deferrals` mirror [`Envelope::attempt`] and
/// [`Envelope::deferrals`], so an operator can read both counters in the
/// management UI without decoding the body. The body stays the source of truth:
/// nothing in this crate reads these back.
#[must_use]
pub fn base_headers(envelope: &Envelope) -> FieldTable {
    let mut headers = FieldTable::default();
    headers.insert(HEADER_ATTEMPT.into(), AMQPValue::LongUInt(envelope.attempt));
    headers.insert(
        HEADER_DEFERRALS.into(),
        AMQPValue::LongUInt(envelope.deferrals),
    );
    headers
}

/// AMQP properties for publishing `envelope`.
///
/// * `content-type` is `application/json`, matching [`Envelope::to_bytes`].
/// * `delivery-mode` is `2` (persistent).
/// * `message-id` is the job id, stable across retries.
/// * `type` is the job type.
/// * `priority` is [`Envelope::priority`], always set. Normal work carries `0`;
///   a deferred envelope carries its queue's top level so it overtakes the
///   backlog. A queue declared without `x-max-priority` ignores the property,
///   and a priority above the queue's `x-max-priority` is treated by the broker
///   as that maximum, so this is safe to set unconditionally.
/// * `expiration` is **never** set, not even for a message bound for a hold
///   queue. The wait is the hold queue's queue-wide `x-message-ttl`; a
///   per-message expiration on top of it would reintroduce exactly the
///   mixed-TTL head-of-line blocking that hold queues exist to avoid, and a
///   shorter one would release the job early.
#[must_use]
pub fn props_for(envelope: &Envelope) -> BasicProperties {
    BasicProperties::default()
        .with_content_type(CONTENT_TYPE_JSON.into())
        .with_delivery_mode(DELIVERY_MODE_PERSISTENT)
        .with_message_id(clamped(&envelope.job_id.to_string()))
        .with_type(clamped(&envelope.job_type))
        .with_priority(envelope.priority)
        .with_headers(base_headers(envelope))
}

/// Headers recorded on a message routed to `q.dead`.
///
/// Extends [`base_headers`] with `x-death-reason`, `x-original-queue` and
/// `x-attempts`.
#[must_use]
pub fn dead_letter_headers(envelope: &Envelope, reason: &str) -> FieldTable {
    let mut headers = base_headers(envelope);
    headers.insert(
        HEADER_DEATH_REASON.into(),
        AMQPValue::LongString(LongString::from(reason)),
    );
    headers.insert(
        HEADER_ORIGINAL_QUEUE.into(),
        AMQPValue::LongString(LongString::from(envelope.queue.as_str())),
    );
    headers.insert(
        HEADER_ATTEMPTS.into(),
        AMQPValue::LongUInt(envelope.attempt),
    );
    headers
}

/// AMQP properties for publishing `envelope` to its dead-letter queue.
#[must_use]
pub fn dead_letter_props(envelope: &Envelope, reason: &str) -> BasicProperties {
    props_for(envelope).with_headers(dead_letter_headers(envelope, reason))
}

/// AMQP properties for a body that could not be decoded as an [`Envelope`].
///
/// The original bytes are forwarded verbatim, so there is no attempt counter and
/// no job metadata to carry, only where it came from and why it was rejected.
#[must_use]
pub fn malformed_props(original_queue: &str, reason: &str) -> BasicProperties {
    let mut headers = FieldTable::default();
    headers.insert(
        HEADER_DEATH_REASON.into(),
        AMQPValue::LongString(LongString::from(reason)),
    );
    headers.insert(
        HEADER_ORIGINAL_QUEUE.into(),
        AMQPValue::LongString(LongString::from(original_queue)),
    );
    BasicProperties::default()
        .with_delivery_mode(DELIVERY_MODE_PERSISTENT)
        .with_headers(headers)
}

/// Convert to a [`ShortString`], truncating at a UTF-8 boundary if needed.
///
/// AMQP short strings are capped at 255 bytes and `ShortString::from` panics
/// past that. Library code must never panic on user-supplied job types, so an
/// over-long value is truncated rather than rejected.
fn clamped(value: &str) -> ShortString {
    ShortString::from(truncate_at_boundary(value, MAX_SHORT_STRING_LENGTH))
}

/// The longest prefix of `value` that is at most `max` bytes and still valid
/// UTF-8 (i.e. it never splits a multi-byte character).
pub(crate) fn truncate_at_boundary(value: &str, max: usize) -> &str {
    if value.len() <= max {
        return value;
    }
    let mut end = max;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    use queuey_core::Envelope;
    use serde_json::json;

    fn envelope() -> Envelope {
        Envelope {
            job_id: "67e55044-10b1-426f-9247-bb680e5fe0c8".parse().unwrap(),
            job_type: "myapp::jobs::SendEmail".to_owned(),
            queue: "myapp.emails".to_owned(),
            attempt: 3,
            enqueued_at_ms: 1_700_000_000_000,
            deferrals: 0,
            priority: 0,
            payload: json!({ "to": "a@b.c" }),
        }
    }

    /// The same envelope after two deferrals onto a 10-level priority queue.
    fn deferred_envelope() -> Envelope {
        envelope().deferred(10).deferred(10)
    }

    #[test]
    fn props_carry_content_type_and_persistence() {
        let props = props_for(&envelope());
        assert_eq!(
            props.content_type().as_ref().map(ShortString::to_string),
            Some("application/json".to_owned())
        );
        assert_eq!(*props.delivery_mode(), Some(2));
    }

    #[test]
    fn props_carry_message_id_and_type() {
        let props = props_for(&envelope());
        assert_eq!(
            props.message_id().as_ref().map(ShortString::to_string),
            Some("67e55044-10b1-426f-9247-bb680e5fe0c8".to_owned())
        );
        assert_eq!(
            props.kind().as_ref().map(ShortString::to_string),
            Some("myapp::jobs::SendEmail".to_owned())
        );
    }

    #[test]
    fn props_carry_the_attempt_and_deferrals_headers() {
        let props = props_for(&envelope());
        let headers = props.headers().as_ref().expect("headers");
        assert_eq!(
            headers.inner().get(HEADER_ATTEMPT),
            Some(&AMQPValue::LongUInt(3))
        );
        assert_eq!(
            headers.inner().get(HEADER_DEFERRALS),
            Some(&AMQPValue::LongUInt(0))
        );
        assert_eq!(headers.inner().len(), 2);
    }

    #[test]
    fn the_deferrals_header_tracks_the_envelope() {
        let props = props_for(&deferred_envelope());
        let headers = props.headers().as_ref().expect("headers");
        assert_eq!(
            headers.inner().get(HEADER_DEFERRALS),
            Some(&AMQPValue::LongUInt(2))
        );
        // A deferral is not an attempt.
        assert_eq!(
            headers.inner().get(HEADER_ATTEMPT),
            Some(&AMQPValue::LongUInt(3))
        );
    }

    #[test]
    fn props_carry_the_envelope_priority() {
        // Normal work is priority 0, and the property is always set so a queue
        // with `x-max-priority` orders every message the same way.
        assert_eq!(*props_for(&envelope()).priority(), Some(0));
        assert_eq!(*props_for(&deferred_envelope()).priority(), Some(10));
        assert_eq!(
            *dead_letter_props(&deferred_envelope(), "boom").priority(),
            Some(10)
        );
    }

    #[test]
    fn props_never_carry_an_expiration_because_the_hold_queue_times_the_wait() {
        // A per-message expiration would fight the hold queue's x-message-ttl
        // and bring back head-of-line blocking between different delays.
        assert!(props_for(&envelope()).expiration().is_none());
        assert!(props_for(&deferred_envelope()).expiration().is_none());
    }

    #[test]
    fn dead_letter_headers_record_reason_queue_and_attempts() {
        let headers = dead_letter_headers(&envelope(), "handler returned Fatal");
        assert_eq!(
            headers.inner().get(HEADER_DEATH_REASON),
            Some(&AMQPValue::LongString(LongString::from(
                "handler returned Fatal"
            )))
        );
        assert_eq!(
            headers.inner().get(HEADER_ORIGINAL_QUEUE),
            Some(&AMQPValue::LongString(LongString::from("myapp.emails")))
        );
        assert_eq!(
            headers.inner().get(HEADER_ATTEMPTS),
            Some(&AMQPValue::LongUInt(3))
        );
        assert_eq!(
            headers.inner().get(HEADER_ATTEMPT),
            Some(&AMQPValue::LongUInt(3))
        );
    }

    #[test]
    fn dead_letter_props_keep_identity_and_drop_expiration() {
        let props = dead_letter_props(&envelope(), "boom");
        assert_eq!(
            props.message_id().as_ref().map(ShortString::to_string),
            Some("67e55044-10b1-426f-9247-bb680e5fe0c8".to_owned())
        );
        assert!(props.expiration().is_none());
        let headers = props.headers().as_ref().expect("headers");
        assert!(headers.contains_key(HEADER_DEATH_REASON));
    }

    #[test]
    fn malformed_props_record_origin_and_reason_only() {
        let props = malformed_props("myapp.emails", REASON_MALFORMED);
        assert_eq!(*props.delivery_mode(), Some(2));
        assert!(props.message_id().is_none());
        let headers = props.headers().as_ref().expect("headers");
        assert_eq!(
            headers.inner().get(HEADER_DEATH_REASON),
            Some(&AMQPValue::LongString(LongString::from(
                "malformed envelope"
            )))
        );
        assert_eq!(
            headers.inner().get(HEADER_ORIGINAL_QUEUE),
            Some(&AMQPValue::LongString(LongString::from("myapp.emails")))
        );
        assert!(!headers.contains_key(HEADER_ATTEMPT));
    }

    #[test]
    fn over_long_job_type_is_truncated_not_panicked() {
        let mut env = envelope();
        env.job_type = "é".repeat(400);
        let props = props_for(&env);
        let kind = props.kind().as_ref().expect("type").to_string();
        assert!(
            kind.len() <= MAX_SHORT_STRING_LENGTH,
            "len was {}",
            kind.len()
        );
        // 'é' is two bytes, so truncation must land on an even byte offset.
        assert_eq!(kind.len(), 254);
        assert!(kind.chars().all(|c| c == 'é'));
    }

    #[test]
    fn truncation_never_splits_a_character() {
        assert_eq!(truncate_at_boundary("héllo", 2), "h");
        assert_eq!(truncate_at_boundary("héllo", 3), "hé");
        assert_eq!(truncate_at_boundary("héllo", 99), "héllo");
        assert_eq!(truncate_at_boundary("é", 1), "");
    }

    #[test]
    fn exactly_max_length_is_kept_verbatim() {
        let mut env = envelope();
        env.job_type = "a".repeat(MAX_SHORT_STRING_LENGTH);
        let props = props_for(&env);
        assert_eq!(
            props.kind().as_ref().map(ShortString::to_string),
            Some(env.job_type)
        );
    }
}
