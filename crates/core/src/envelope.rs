//! The [`Envelope`]: the wire format every job travels in.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{error::Result, job::Job, queue::QueueSet};

/// Wire format for a job message. Serialized as JSON in the message body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    /// Unique id, stable across retries.
    pub job_id: Uuid,
    /// `Job::NAME` of the payload.
    pub job_type: String,
    /// Broker queue name this envelope was published to.
    pub queue: String,
    /// 1-based attempt counter. First delivery is attempt 1.
    pub attempt: u32,
    /// Unix epoch milliseconds when the job was first enqueued.
    pub enqueued_at_ms: u64,
    /// How often this job was deferred (see [`crate::JobError::Deferred`]).
    ///
    /// Independent of [`Envelope::attempt`]: a deferral is not a failed attempt.
    /// Defaults to `0` so envelopes written before this field existed still decode.
    #[serde(default)]
    pub deferrals: u32,
    /// Broker message priority; `0` (the default) is normal work.
    ///
    /// Deferred envelopes carry the highest level their queue supports
    /// ([`crate::QueueConfig::max_priority`]) so they run ahead of the backlog. A queue
    /// that is not a priority queue makes the broker ignore this. Only the scheduling
    /// action that just happened decides this: a deferral raises it,
    /// [`Envelope::next_attempt`] puts it back to `0`. Defaults to `0` so envelopes
    /// written before this field existed still decode.
    #[serde(default)]
    pub priority: u8,
    /// The serialized job.
    pub payload: serde_json::Value,
}

impl Envelope {
    /// Build a first-attempt envelope for `job`.
    pub fn new<J: Job>(job: &J) -> Result<Self> {
        Ok(Self {
            job_id: Uuid::new_v4(),
            job_type: J::NAME.to_owned(),
            queue: J::QUEUE.name().to_owned(),
            attempt: 1,
            enqueued_at_ms: now_ms(),
            deferrals: 0,
            priority: 0,
            payload: serde_json::to_value(job)?,
        })
    }

    /// Deserialize the payload as `J`. Does not check `job_type`.
    pub fn decode<J: Job>(&self) -> Result<J> {
        Ok(serde_json::from_value(self.payload.clone())?)
    }

    /// Copy of this envelope with `attempt` incremented and `priority` back at `0`.
    ///
    /// `deferrals` rides along untouched, so a retry still knows how often the job was
    /// deferred. `priority` does not: a retry is scheduled like any other failed
    /// attempt and must not jump the backlog just because the job happened to defer
    /// itself earlier. Only the scheduling action that just happened decides the
    /// priority. See [`Envelope::deferred`] for the one that raises it.
    pub fn next_attempt(&self) -> Self {
        Self {
            attempt: self.attempt + 1,
            priority: 0,
            ..self.clone()
        }
    }

    /// Copy of this envelope for a deferral: `deferrals + 1` and `priority` set.
    ///
    /// `attempt`, `job_id`, `job_type`, `queue` and `enqueued_at_ms` are unchanged:
    /// a deferral is not a failed attempt, and `age` keeps measuring the time since
    /// the job was first enqueued. `priority` is normally
    /// `QueueConfig::max_priority.unwrap_or(0)` of the job's queue, so the job comes
    /// back ahead of everything published normally. It lasts only until the job is
    /// next scheduled some other way: [`Envelope::next_attempt`] drops it back to `0`.
    /// See [`crate::JobError::Deferred`].
    pub fn deferred(&self, priority: u8) -> Self {
        Self {
            deferrals: self.deferrals + 1,
            priority,
            ..self.clone()
        }
    }

    /// Serialize the envelope to the JSON message body.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    /// Parse an envelope from a JSON message body.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

pub(crate) fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{Greet, Ping};

    #[test]
    fn new_fills_in_the_job_metadata() {
        let env = Envelope::new(&Greet::new("ada")).unwrap();
        assert_eq!(env.job_type, Greet::NAME);
        assert_eq!(env.queue, "test.alpha");
        assert_eq!(env.attempt, 1);
        assert_ne!(env.job_id, Uuid::nil());
        assert!(env.enqueued_at_ms > 0);
        assert_eq!(env.deferrals, 0);
        assert_eq!(env.priority, 0);
        assert_eq!(env.payload, serde_json::json!({ "name": "ada" }));
    }

    #[test]
    fn distinct_envelopes_get_distinct_ids() {
        let a = Envelope::new(&Greet::new("a")).unwrap();
        let b = Envelope::new(&Greet::new("a")).unwrap();
        assert_ne!(a.job_id, b.job_id);
    }

    #[test]
    fn bytes_round_trip() {
        let env = Envelope::new(&Greet::new("grace")).unwrap();
        let bytes = env.to_bytes().unwrap();
        let back = Envelope::from_bytes(&bytes).unwrap();
        assert_eq!(env, back);
    }

    #[test]
    fn from_bytes_rejects_garbage() {
        assert!(Envelope::from_bytes(b"not json").is_err());
        assert!(Envelope::from_bytes(br#"{"job_id":"nope"}"#).is_err());
    }

    #[test]
    fn decode_returns_the_original_job() {
        let job = Greet::new("linus");
        let env = Envelope::new(&job).unwrap();
        assert_eq!(env.decode::<Greet>().unwrap(), job);
    }

    #[test]
    fn decode_with_wrong_shape_errors() {
        let env = Envelope::new(&Greet::new("linus")).unwrap();
        // `Ping { seq: u32 }` cannot be built from `{ "name": "linus" }`.
        let err = env.decode::<Ping>().unwrap_err();
        assert!(
            matches!(err, crate::error::Error::Serde(_)),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn decode_after_round_trip_still_works() {
        let env = Envelope::new(&Ping { seq: 7 }).unwrap();
        let back = Envelope::from_bytes(&env.to_bytes().unwrap()).unwrap();
        assert_eq!(back.decode::<Ping>().unwrap(), Ping { seq: 7 });
    }

    #[test]
    fn next_attempt_increments_and_preserves_identity() {
        let env = Envelope::new(&Greet::new("ada")).unwrap();
        let second = env.next_attempt();
        assert_eq!(second.attempt, 2);
        assert_eq!(second.job_id, env.job_id);
        assert_eq!(second.job_type, env.job_type);
        assert_eq!(second.queue, env.queue);
        assert_eq!(second.enqueued_at_ms, env.enqueued_at_ms);
        assert_eq!(second.payload, env.payload);
        assert_eq!(second.priority, 0);
        // The original is untouched.
        assert_eq!(env.attempt, 1);
        assert_eq!(second.next_attempt().attempt, 3);
    }

    #[test]
    fn next_attempt_keeps_the_deferral_count_but_drops_the_priority() {
        let env = Envelope::new(&Greet::new("ada")).unwrap().deferred(10);
        let retried = env.next_attempt();
        assert_eq!(retried.attempt, 2);
        assert_eq!(retried.deferrals, 1, "a retry is not a second deferral");
        assert_eq!(
            retried.priority, 0,
            "a retry is not a deferral: it goes behind the backlog"
        );
        // The deferred envelope it came from is untouched.
        assert_eq!(env.priority, 10);
        assert_eq!(env.attempt, 1);
        // And a further retry stays at zero.
        assert_eq!(retried.next_attempt().priority, 0);
    }

    #[test]
    fn deferred_increments_deferrals_and_sets_the_priority() {
        let env = Envelope::new(&Greet::new("ada")).unwrap();
        let held = env.deferred(10);

        assert_eq!(held.deferrals, 1);
        assert_eq!(held.priority, 10);
        // A deferral is not an attempt, and the identity is untouched.
        assert_eq!(held.attempt, env.attempt);
        assert_eq!(held.job_id, env.job_id);
        assert_eq!(held.job_type, env.job_type);
        assert_eq!(held.queue, env.queue);
        assert_eq!(held.enqueued_at_ms, env.enqueued_at_ms);
        assert_eq!(held.payload, env.payload);
        // The original is untouched.
        assert_eq!(env.deferrals, 0);
        assert_eq!(env.priority, 0);
    }

    #[test]
    fn deferrals_accumulate_and_a_zero_priority_queue_stays_at_zero() {
        let env = Envelope::new(&Greet::new("ada")).unwrap();
        let twice = env.deferred(10).deferred(0);
        assert_eq!(twice.deferrals, 2);
        assert_eq!(twice.priority, 0);
        assert_eq!(twice.attempt, 1);
    }

    #[test]
    fn json_without_the_deferral_fields_still_decodes() {
        // Exactly the wire format from before deferral existed.
        let old = serde_json::json!({
            "job_id": "8b1a9953-4c2f-4a5b-9c2e-0d1f2a3b4c5d",
            "job_type": "test::Greet",
            "queue": "test.alpha",
            "attempt": 2,
            "enqueued_at_ms": 1_700_000_000_000u64,
            "payload": { "name": "ada" },
        });
        let env = Envelope::from_bytes(&serde_json::to_vec(&old).unwrap()).unwrap();

        assert_eq!(env.deferrals, 0);
        assert_eq!(env.priority, 0);
        assert_eq!(env.attempt, 2);
        assert_eq!(env.decode::<Greet>().unwrap(), Greet::new("ada"));
        // And it round-trips through the new format unchanged.
        assert_eq!(Envelope::from_bytes(&env.to_bytes().unwrap()).unwrap(), env);
    }

    #[test]
    fn now_ms_is_monotonic_enough() {
        assert!(now_ms() >= 1_700_000_000_000);
    }
}
