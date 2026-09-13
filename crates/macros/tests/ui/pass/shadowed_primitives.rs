//! The generated code must not depend on the primitive type names being in
//! scope: a user type called `str` (or `u16`, `bool`, `f64`) in the same module
//! as the derived item must not break the derive.

use queuey_core::{Job as _, QueueSet as _};

#[allow(non_camel_case_types, dead_code)]
mod shadowed {
    use queuey_macros::{Job, Queues};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub struct str;
    pub struct u16;
    pub struct u32;
    pub struct u64;
    pub struct bool;
    pub struct f64;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
    #[queues(prefix = "myapp")]
    pub enum AppQueues {
        #[queue(
            name = "emails",
            prefetch = 10,
            durable = false,
            message_ttl = "30s",
            retry(
                max_attempts = 3,
                backoff = "exponential",
                base = "1s",
                factor = 2.0,
                max = "2m",
                jitter = true
            )
        )]
        Emails,
    }

    #[derive(Job)]
    #[job(
        queue = AppQueues::Emails,
        name = "emails.send",
        retry(max_attempts = 2, backoff = "fixed", delay = "500ms")
    )]
    pub struct SendEmail;

    // Hand written rather than derived: `#[derive(Serialize)]` itself mentions
    // bare primitives, which is serde's business and not what this test is about.
    impl Serialize for SendEmail {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.serialize_unit_struct("SendEmail")
        }
    }

    impl<'de> Deserialize<'de> for SendEmail {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            <() as Deserialize>::deserialize(deserializer)?;
            Ok(Self)
        }
    }
}

use shadowed::{AppQueues, SendEmail};

fn main() {
    assert_eq!(AppQueues::Emails.name(), "myapp.emails");
    assert_eq!(AppQueues::from_name("myapp.emails"), Some(AppQueues::Emails));
    let config = AppQueues::Emails.config();
    assert_eq!(config.prefetch, 10);
    assert!(!config.durable);
    assert!(config.message_ttl.is_some());
    assert_eq!(SendEmail::NAME, "emails.send");
    assert_eq!(SendEmail::QUEUE, AppQueues::Emails);
    assert!(SendEmail::retry_policy().is_some());
}
