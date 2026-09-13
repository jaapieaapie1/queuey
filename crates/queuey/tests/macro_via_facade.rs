//! The derive macros must work for a crate that depends on `queuey`
//! alone: one glob import, no `crate = "..."` escape hatch, no direct dependency
//! on `queuey-core`.
//!
//! If the path the macros emit ever stops resolving through the facade, this file
//! stops compiling.

use std::time::Duration;

use queuey::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
#[queues(prefix = "smoke")]
enum Queues1 {
    #[queue(prefetch = 7, durable = false, message_ttl = "30s", max_priority = 3)]
    Alpha,
    /// `max_priority = 0`: not a priority queue at all.
    #[queue(
        name = "renamed",
        max_priority = 0,
        retry(max_attempts = 4, backoff = "fixed", delay = "250ms")
    )]
    BetaGamma,
}

/// A second set, without a prefix, to prove the two are independent types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Queues)]
enum Queues2 {
    Solo,
}

#[derive(Debug, Serialize, Deserialize, Job)]
#[job(queue = Queues1::Alpha)]
struct Plain {
    id: u64,
}

#[derive(Debug, Serialize, Deserialize, Job)]
#[job(
    queue = Queues1::BetaGamma,
    name = "smoke.named",
    retry(max_attempts = 2, backoff = "none")
)]
struct Named;

#[derive(Debug, Serialize, Deserialize, Job)]
#[job(queue = Queues2::Solo)]
struct Other {
    id: u64,
}

#[test]
fn queue_set_impl_is_generated() {
    assert_eq!(Queues1::all(), &[Queues1::Alpha, Queues1::BetaGamma]);
    assert_eq!(Queues1::Alpha.name(), "smoke.alpha");
    assert_eq!(Queues1::BetaGamma.name(), "smoke.renamed");
    assert_eq!(Queues2::Solo.name(), "solo");

    let config = Queues1::Alpha.config();
    assert_eq!(config.prefetch, 7);
    assert!(!config.durable);
    assert_eq!(config.message_ttl, Some(Duration::from_secs(30)));
    assert_eq!(
        Queues1::BetaGamma.config().retry,
        RetryPolicy::fixed(4, Duration::from_millis(250))
    );
}

/// `max_priority`, the attribute deferred jobs depend on to come back first,
/// survives the trip through the facade re-export of `#[derive(Queues)]`.
#[test]
fn max_priority_is_carried_through_the_facade() {
    assert_eq!(Queues1::Alpha.config().max_priority, Some(3));
    assert_eq!(
        Queues1::BetaGamma.config().max_priority,
        None,
        "`max_priority = 0` means the queue is not a priority queue"
    );
    assert_eq!(
        Queues2::Solo.config().max_priority,
        Some(DEFAULT_MAX_PRIORITY),
        "omitting the attribute keeps the default ten levels"
    );
    assert_eq!(DEFAULT_MAX_PRIORITY, 10);
}

#[test]
fn job_impl_is_generated() {
    assert_eq!(Plain::QUEUE, Queues1::Alpha);
    assert_eq!(Plain::NAME, concat!(module_path!(), "::", "Plain"));
    assert_eq!(Plain::retry_policy(), None);

    assert_eq!(Named::NAME, "smoke.named");
    assert_eq!(
        Named::retry_policy(),
        Some(RetryPolicy::new(2, Backoff::None))
    );

    // Distinct queue sets, so the two jobs are not interchangeable anywhere.
    assert_eq!(Other::QUEUE, Queues2::Solo);
}

/// The generic bounds a `Producer`/`Worker` place on a job, spelled out.
#[test]
fn jobs_satisfy_the_queue_set_bound() {
    fn queue_name_of<J: Job<Queue = Q>, Q: QueueSet>() -> &'static str {
        J::QUEUE.name()
    }
    assert_eq!(queue_name_of::<Plain, Queues1>(), "smoke.alpha");
    assert_eq!(queue_name_of::<Other, Queues2>(), "solo");
}

#[tokio::test]
async fn the_runtime_accepts_the_derived_types() {
    let backend = Arc::new(MemoryBackend::new());
    let producer = Producer::<Queues1, _>::new(backend.clone()).await.unwrap();
    let id = producer.enqueue(&Plain { id: 1 }).await.unwrap();

    assert_eq!(backend.pending("smoke.alpha"), 1);
    assert_eq!(backend.queue_names(), vec!["smoke.alpha", "smoke.renamed"]);

    let worker = Worker::<Queues1, _>::builder(backend.clone())
        .handler(FnHandler::<Plain, _>::new(
            |job: Plain, ctx: JobContext| async move {
                assert_eq!(job.id, 1);
                assert_eq!(ctx.attempt, 1);
                Ok(())
            },
        ))
        .build()
        .await
        .unwrap();
    let handle = worker.handle();
    let task = tokio::spawn(worker.run());

    for _ in 0..100 {
        if backend.acked("smoke.alpha").len() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(backend.acked("smoke.alpha")[0].job_id, id);

    handle.shutdown();
    task.await.unwrap().unwrap();
}
