//! Integration tests against a real RabbitMQ.
//!
//! These are skipped (with a notice on stderr) unless `AMQP_URL` is set, so the
//! default `cargo test` run stays hermetic. To run them:
//!
//! ```sh
//! docker run --rm -p 5672:5672 rabbitmq:4-management
//! AMQP_URL=amqp://guest:guest@localhost:5672/%2f cargo test -p queuey-rabbitmq
//! ```
//!
//! Every test uses queue names carrying a fresh UUID and deletes them again at
//! the end, so concurrent runs against one broker do not collide.

use std::time::{Duration, Instant};

use futures::StreamExt;
use lapin::{
    Channel, Connection, ConnectionProperties,
    options::{
        BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions,
        QueueDeleteOptions,
    },
    types::{AMQPValue, FieldTable},
};
use queuey_core::{Backend, Delivery, Envelope, Error, QueueConfig};
use queuey_rabbitmq::{RabbitMqBackend, RabbitMqOptions};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

/// A queue name that is unique to this test run.
fn unique_queue(label: &str) -> String {
    format!("aq-test.{label}.{}", Uuid::new_v4().simple())
}

/// The two long-lived broker queues backing `queue`, under the default suffixes.
fn family(queue: &str) -> [String; 2] {
    [queue.to_owned(), format!("{queue}.dead")]
}

/// The hold queue a `delay` on `queue` waits in, at the default one-second
/// granularity.
fn hold_queue(queue: &str, delay: Duration) -> String {
    format!("{queue}.deferred.{}", delay.as_millis())
}

fn envelope(queue: &str, attempt: u32) -> Envelope {
    let mut envelope = Envelope::raw(
        "queuey_rabbitmq::tests::Probe",
        queue,
        serde_json::json!({ "probe": true, "n": attempt }),
    );
    envelope.attempt = attempt;
    envelope.enqueued_at_ms = 1_700_000_000_000;
    envelope
}

/// A second connection, used for assertions and cleanup so that nothing the
/// backend does (or fails to do) can hide a problem.
async fn control(url: &str) -> Connection {
    Connection::connect(url, ConnectionProperties::default())
        .await
        .expect("control connection")
}

/// Number of *ready* (delivered-to-nobody) messages on `queue`.
///
/// Uses a throwaway channel: a passive declare of a queue that does not exist
/// closes the channel it was issued on.
async fn ready_count(control: &Connection, queue: &str) -> u32 {
    let channel = control.create_channel().await.expect("channel");
    let declared = channel
        .queue_declare(
            queue.into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap_or_else(|error| panic!("passive declare of `{queue}` failed: {error}"));
    let count = declared.message_count();
    let _ = channel.close(200, "OK".into()).await;
    count
}

/// Whether `queue` exists, via a passive declare on a throwaway channel.
async fn queue_exists(control: &Connection, queue: &str) -> bool {
    let channel = control.create_channel().await.expect("channel");
    let exists = channel
        .queue_declare(
            queue.into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .is_ok();
    let _ = channel.close(200, "OK".into()).await;
    exists
}

/// Poll `queue` until it holds `expected` ready messages, or panic.
async fn await_count(control: &Connection, queue: &str, expected: u32) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut last = u32::MAX;
    while std::time::Instant::now() < deadline {
        last = ready_count(control, queue).await;
        if last == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("`{queue}` held {last} ready messages, expected {expected}");
}

/// Publish raw bytes straight to `queue`, bypassing the backend.
///
/// The channel is put into confirm mode first: without `confirm_select` the
/// awaited `PublisherConfirm` resolves immediately to `NotRequested`, so the
/// `.await` below would prove nothing about the message having landed.
async fn publish_raw(channel: &Channel, queue: &str, body: &[u8]) {
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .expect("confirm_select");

    let confirmation = channel
        .basic_publish(
            "".into(),
            queue.into(),
            BasicPublishOptions {
                mandatory: true,
                immediate: false,
            },
            body,
            lapin::BasicProperties::default(),
        )
        .await
        .expect("publish")
        .await
        .expect("confirm");

    assert!(
        confirmation.is_ack() && confirmation.take_message().is_none(),
        "raw publish to `{queue}` was not routed anywhere"
    );
}

async fn cleanup(control: &Connection, queue: &str) {
    delete_queues(control, &family(queue)).await;
}

/// Delete `names`, ignoring the ones that are not there.
///
/// Hold queues are not part of [`family`]: their names depend on the delays a
/// test actually used, and the broker may well have expired them already.
async fn delete_queues(control: &Connection, names: &[String]) {
    let channel = control.create_channel().await.expect("channel");
    for name in names {
        let _ = channel
            .queue_delete(name.as_str().into(), QueueDeleteOptions::default())
            .await;
    }
    let _ = channel.close(200, "OK".into()).await;
}

/// Declare `queue` straight on the broker with the given arguments.
///
/// Used to prove what a queue was *actually* declared with: RabbitMQ answers a
/// declaration whose arguments differ from the existing queue's with
/// `PRECONDITION_FAILED`, so a successful redeclare is an equivalence assertion.
/// A passive declare cannot do this: it does not report arguments at all.
async fn redeclare_raw(
    control: &Connection,
    queue: &str,
    args: FieldTable,
) -> Result<(), lapin::Error> {
    let channel = control.create_channel().await.expect("channel");
    let result = channel
        .queue_declare(
            queue.into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            args,
        )
        .await
        .map(|_| ());
    let _ = channel.close(200, "OK".into()).await;
    result
}

fn header_string(headers: &FieldTable, key: &str) -> Option<String> {
    match headers.inner().get(key)? {
        AMQPValue::LongString(value) => Some(value.to_string()),
        AMQPValue::ShortString(value) => Some(value.to_string()),
        _ => None,
    }
}

/// Read an integer header without assuming which width RabbitMQ echoes back.
fn header_u32(headers: &FieldTable, key: &str) -> Option<u32> {
    match headers.inner().get(key)? {
        AMQPValue::ShortShortInt(value) => u32::try_from(*value).ok(),
        AMQPValue::ShortShortUInt(value) => Some(u32::from(*value)),
        AMQPValue::ShortInt(value) => u32::try_from(*value).ok(),
        AMQPValue::ShortUInt(value) => Some(u32::from(*value)),
        AMQPValue::LongInt(value) => u32::try_from(*value).ok(),
        AMQPValue::LongUInt(value) => Some(*value),
        AMQPValue::LongLongInt(value) => u32::try_from(*value).ok(),
        _ => None,
    }
}

/// Take the next delivery, failing the test if it does not arrive in time.
async fn next_delivery(
    stream: &mut queuey_core::DeliveryStream,
    within: Duration,
) -> Box<dyn Delivery> {
    match tokio::time::timeout(within, stream.next()).await {
        Ok(Some(Ok(delivery))) => delivery,
        Ok(Some(Err(error))) => panic!("consumer stream yielded an error: {error}"),
        Ok(None) => panic!("consumer stream ended unexpectedly"),
        Err(_) => panic!("no delivery within {within:?}"),
    }
}

/// Assert that nothing arrives within `within`.
async fn expect_idle(stream: &mut queuey_core::DeliveryStream, within: Duration) {
    match tokio::time::timeout(within, stream.next()).await {
        Err(_) => {}
        Ok(Some(Ok(delivery))) => panic!(
            "unexpected delivery of job {} (attempt {})",
            delivery.envelope().job_id,
            delivery.envelope().attempt
        ),
        Ok(Some(Err(error))) => panic!("consumer stream yielded an error: {error}"),
        Ok(None) => panic!("consumer stream ended unexpectedly"),
    }
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn declare_is_idempotent() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let queue = unique_queue("declare");
    let config = QueueConfig::new(queue.clone());
    let backend = RabbitMqBackend::connect(&url).await.expect("connect");
    let control = control(&url).await;

    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("first declare");
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("second declare with identical arguments");

    // Both queues exist and are empty.
    for name in family(&queue) {
        assert_eq!(ready_count(&control, &name).await, 0, "queue {name}");
    }

    // A third declare through a *different* backend instance must also work:
    // declaration happens on a throwaway channel, so nothing is left poisoned.
    let other = RabbitMqBackend::connect(&url).await.expect("connect");
    other
        .declare(std::slice::from_ref(&config))
        .await
        .expect("third declare");
    other.close().await.expect("close");

    cleanup(&control, &queue).await;
    backend.close().await.expect("close");
}

#[tokio::test]
async fn publish_then_consume_and_ack() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let queue = unique_queue("roundtrip");
    let config = QueueConfig::new(queue.clone());
    let backend = RabbitMqBackend::connect(&url).await.expect("connect");
    let control = control(&url).await;
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");

    let sent = envelope(&queue, 1);
    backend.publish(&sent, None).await.expect("publish");
    await_count(&control, &queue, 1).await;

    let mut stream = backend.consume(&config).await.expect("consume");
    let delivery = next_delivery(&mut stream, Duration::from_secs(5)).await;
    assert_eq!(delivery.envelope(), &sent, "envelope round-tripped intact");
    delivery.ack().await.expect("ack");

    // Dropping the consumer would requeue anything unacked, so a zero count
    // after closing proves the ack landed.
    drop(stream);
    backend.close().await.expect("close");
    await_count(&control, &queue, 0).await;

    cleanup(&control, &queue).await;
}

#[tokio::test]
async fn prefetch_limits_unacked_deliveries() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let queue = unique_queue("prefetch");
    let config = QueueConfig::new(queue.clone()).prefetch(2);
    let backend = RabbitMqBackend::connect(&url).await.expect("connect");
    let control = control(&url).await;
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");

    for attempt in 1..=5 {
        backend
            .publish(&envelope(&queue, attempt), None)
            .await
            .expect("publish");
    }
    await_count(&control, &queue, 5).await;

    let mut stream = backend.consume(&config).await.expect("consume");
    let first = next_delivery(&mut stream, Duration::from_secs(5)).await;
    let second = next_delivery(&mut stream, Duration::from_secs(5)).await;

    // Two unacked deliveries fill the prefetch window; the broker must hold the
    // remaining three back.
    expect_idle(&mut stream, Duration::from_millis(750)).await;

    // Acking one frees exactly one slot.
    first.ack().await.expect("ack");
    let third = next_delivery(&mut stream, Duration::from_secs(5)).await;
    expect_idle(&mut stream, Duration::from_millis(750)).await;

    second.ack().await.expect("ack");
    third.ack().await.expect("ack");

    drop(stream);
    backend.close().await.expect("close");
    cleanup(&control, &queue).await;
}

#[tokio::test]
async fn delayed_publish_waits_for_the_delay() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let queue = unique_queue("delayed");
    let config = QueueConfig::new(queue.clone());
    let backend = RabbitMqBackend::connect(&url).await.expect("connect");
    let control = control(&url).await;
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");

    let mut stream = backend.consume(&config).await.expect("consume");

    let sent = envelope(&queue, 1);
    let delay = Duration::from_secs(2);
    backend
        .publish(&sent, Some(delay))
        .await
        .expect("delayed publish");

    // It waits in the hold queue for its delay, not on `queue`. Asserting on
    // `queue` would be vacuous: a consumer is attached, so anything landing
    // there is taken off again immediately and the ready count reads 0 either
    // way.
    let hold = hold_queue(&queue, delay);
    await_count(&control, &hold, 1).await;
    expect_idle(&mut stream, Duration::from_millis(1_200)).await;

    let delivery = next_delivery(&mut stream, Duration::from_secs(8)).await;
    assert_eq!(delivery.envelope(), &sent);
    assert_eq!(
        *delivery.envelope(),
        sent,
        "a delayed publish comes back byte for byte"
    );
    delivery.ack().await.expect("ack");

    drop(stream);
    backend.close().await.expect("close");
    cleanup(&control, &queue).await;
    delete_queues(&control, &[hold]).await;
}

#[tokio::test]
async fn retry_redelivers_with_the_next_attempt() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let queue = unique_queue("retry");
    let config = QueueConfig::new(queue.clone());
    let backend = RabbitMqBackend::connect(&url).await.expect("connect");
    let control = control(&url).await;
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");

    let sent = envelope(&queue, 1);
    backend.publish(&sent, None).await.expect("publish");

    let mut stream = backend.consume(&config).await.expect("consume");
    let first = next_delivery(&mut stream, Duration::from_secs(5)).await;
    assert_eq!(first.envelope().attempt, 1);

    let next = first.envelope().next_attempt();
    let started = Instant::now();
    first
        .retry(next.clone(), Duration::from_millis(500))
        .await
        .expect("retry");

    // 500ms rounds up to the default one-second granularity, so the retry
    // waits in `q.deferred.1000` alongside any other sub-second delay.
    let hold = hold_queue(&queue, Duration::from_secs(1));
    assert_eq!(ready_count(&control, &hold).await, 1, "held in `{hold}`");

    // Not before the delay ...
    expect_idle(&mut stream, Duration::from_millis(250)).await;

    // ... and then the same job with attempt + 1.
    let second = next_delivery(&mut stream, Duration::from_secs(8)).await;
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(900),
        "retry came back after {elapsed:?}; rounding up to the granularity means never early"
    );
    assert_eq!(second.envelope().job_id, sent.job_id, "same job id");
    assert_eq!(second.envelope().attempt, 2, "attempt was incremented");
    assert_eq!(second.envelope(), &next);
    second.ack().await.expect("ack");

    drop(stream);
    backend.close().await.expect("close");
    await_count(&control, &queue, 0).await;
    await_count(&control, &hold, 0).await;
    cleanup(&control, &queue).await;
    delete_queues(&control, &[hold]).await;
}

#[tokio::test]
async fn a_short_retry_is_not_stuck_behind_a_long_one() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    // The reason retries live in hold queues at all. With one shared wait queue
    // and per-message expirations, RabbitMQ only expires the head, so the
    // 6-second retry published *first* would hold the 1-second retry behind it
    // for the full six seconds. In separate hold queues each expires on its
    // own clock.
    let queue = unique_queue("retry-hol");
    let config = QueueConfig::new(queue.clone());
    let backend = RabbitMqBackend::connect(&url).await.expect("connect");
    let control = control(&url).await;
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");

    let slow = envelope(&queue, 1);
    let fast = envelope(&queue, 1);
    backend.publish(&slow, None).await.expect("publish");
    backend.publish(&fast, None).await.expect("publish");

    let mut stream = backend.consume(&config).await.expect("consume");
    let first = next_delivery(&mut stream, Duration::from_secs(5)).await;
    let second = next_delivery(&mut stream, Duration::from_secs(5)).await;
    assert_eq!(first.envelope(), &slow, "FIFO on the work queue");
    assert_eq!(second.envelope(), &fast);

    let slow_next = slow.next_attempt();
    let fast_next = fast.next_attempt();
    let long_delay = Duration::from_secs(6);
    let short_delay = Duration::from_secs(1);
    let started = Instant::now();
    // The long one goes first, so it would be at the head of a shared queue.
    first
        .retry(slow_next.clone(), long_delay)
        .await
        .expect("retry (long)");
    second
        .retry(fast_next.clone(), short_delay)
        .await
        .expect("retry (short)");

    let long_hold = hold_queue(&queue, long_delay);
    let short_hold = hold_queue(&queue, short_delay);
    assert_eq!(ready_count(&control, &long_hold).await, 1);
    assert_eq!(ready_count(&control, &short_hold).await, 1);

    // The short retry comes back in about a second, not six.
    let delivery = next_delivery(&mut stream, Duration::from_secs(4)).await;
    let elapsed = started.elapsed();
    assert_eq!(
        delivery.envelope(),
        &fast_next,
        "the short retry returns first"
    );
    assert!(
        elapsed < Duration::from_secs(4),
        "the short retry took {elapsed:?}; it was stuck behind the long one"
    );
    assert!(
        elapsed >= Duration::from_millis(900),
        "the short retry came back after only {elapsed:?}"
    );
    delivery.ack().await.expect("ack");

    // The long one is still waiting, and does come back eventually.
    assert_eq!(ready_count(&control, &long_hold).await, 1);
    let delivery = next_delivery(&mut stream, Duration::from_secs(10)).await;
    assert_eq!(delivery.envelope(), &slow_next);
    assert!(
        started.elapsed() >= Duration::from_millis(5_900),
        "the long retry came back early"
    );
    delivery.ack().await.expect("ack");

    drop(stream);
    backend.close().await.expect("close");
    await_count(&control, &queue, 0).await;
    cleanup(&control, &queue).await;
    delete_queues(&control, &[long_hold, short_hold]).await;
}

#[tokio::test]
async fn retries_and_deferrals_share_a_hold_queue_but_return_at_their_own_priority() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    // Same rounded delay, same hold queue: the arguments depend only on the
    // TTL. The priority rides on the message, so once both are back on `q`
    // the deferral is served first even though it was published second.
    let queue = unique_queue("retry-shared-hold");
    let config = QueueConfig::new(queue.clone());
    let backend = RabbitMqBackend::connect(&url).await.expect("connect");
    let control = control(&url).await;
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");

    let retried = envelope(&queue, 1).next_attempt();
    let deferred = envelope(&queue, 1).deferred(10);
    assert_eq!(retried.priority, 0);
    let delay = Duration::from_secs(1);
    backend
        .publish(&retried, Some(delay))
        .await
        .expect("delayed publish");
    backend.defer(&deferred, delay).await.expect("defer");

    let hold = hold_queue(&queue, delay);
    assert_eq!(
        ready_count(&control, &hold).await,
        2,
        "both wait in `{hold}`"
    );

    // Let both expire back onto `q` before consuming, so the order below is
    // decided by priority and not by which one the broker moved first.
    await_count(&control, &queue, 2).await;

    let mut stream = backend.consume(&config).await.expect("consume");
    let first = next_delivery(&mut stream, Duration::from_secs(5)).await;
    assert_eq!(
        first.envelope(),
        &deferred,
        "the deferral overtakes the retry"
    );
    first.ack().await.expect("ack");
    let second = next_delivery(&mut stream, Duration::from_secs(5)).await;
    assert_eq!(second.envelope(), &retried);
    second.ack().await.expect("ack");

    drop(stream);
    backend.close().await.expect("close");
    cleanup(&control, &queue).await;
    delete_queues(&control, &[hold]).await;
}

#[tokio::test]
async fn retry_granularity_is_separate_from_deferral_granularity() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    // A coarse retry granularity bounds the number of hold queues a jittered
    // backoff creates, without touching how precisely a deferral waits.
    let queue = unique_queue("retry-granularity");
    let config = QueueConfig::new(queue.clone());
    let backend = RabbitMqBackend::with_options(
        &url,
        RabbitMqOptions::default()
            .retry_granularity(Duration::from_secs(5))
            .deferred_granularity(Duration::from_secs(1)),
    )
    .await
    .expect("connect");
    let control = control(&url).await;
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");

    let delay = Duration::from_millis(1_200);
    backend
        .publish(&envelope(&queue, 1), Some(delay))
        .await
        .expect("delayed publish");
    backend
        .defer(&envelope(&queue, 1).deferred(10), delay)
        .await
        .expect("defer");

    // 1.2s rounds up to 5s for the retry path and to 2s for the deferral.
    let retry_hold = backend.deferred_queue_name(&queue, 5_000);
    let defer_hold = backend.deferred_queue_name(&queue, 2_000);
    assert_eq!(
        ready_count(&control, &retry_hold).await,
        1,
        "`{retry_hold}`"
    );
    assert_eq!(
        ready_count(&control, &defer_hold).await,
        1,
        "`{defer_hold}`"
    );

    backend.close().await.expect("close");
    cleanup(&control, &queue).await;
    delete_queues(&control, &[retry_hold, defer_hold]).await;
}

#[tokio::test]
async fn retrying_onto_an_undeclared_queue_is_refused() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    // Same rule as for deferral, now that a delayed publish is a hold too: a
    // backend that never declared the queue cannot know the hold queue's
    // durability, and a TTL expiry into a missing queue is dropped silently.
    let queue = unique_queue("retry-undeclared");
    let backend = RabbitMqBackend::connect(&url).await.expect("connect");
    let control = control(&url).await;

    let error = backend
        .publish(&envelope(&queue, 1), Some(Duration::from_secs(1)))
        .await
        .expect_err("a delayed publish onto an undeclared queue must fail");
    assert!(
        matches!(&error, Error::UnknownQueue(name) if name == &queue),
        "expected UnknownQueue, got {error:?}"
    );
    let hold = hold_queue(&queue, Duration::from_secs(1));
    assert!(
        !queue_exists(&control, &hold).await,
        "`{hold}` must not have been created"
    );

    backend.close().await.expect("close");
}

#[tokio::test]
async fn dead_letter_moves_the_message_with_headers() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let queue = unique_queue("dead");
    let dead = format!("{queue}.dead");
    let config = QueueConfig::new(queue.clone());
    let backend = RabbitMqBackend::connect(&url).await.expect("connect");
    let control = control(&url).await;
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");

    let sent = envelope(&queue, 3);
    backend.publish(&sent, None).await.expect("publish");

    let mut stream = backend.consume(&config).await.expect("consume");
    let delivery = next_delivery(&mut stream, Duration::from_secs(5)).await;
    delivery
        .dead_letter("handler returned Fatal")
        .await
        .expect("dead_letter");

    drop(stream);
    backend.close().await.expect("close");

    // Gone from the work queue, present on the dead-letter queue.
    await_count(&control, &queue, 0).await;
    await_count(&control, &dead, 1).await;

    let channel = control.create_channel().await.expect("channel");
    let message = channel
        .basic_get(dead.as_str().into(), BasicGetOptions { no_ack: true })
        .await
        .expect("basic_get")
        .expect("a dead-lettered message");

    assert_eq!(
        Envelope::from_bytes(&message.delivery.data).expect("decode"),
        sent
    );

    let headers = message
        .delivery
        .properties
        .headers()
        .clone()
        .expect("headers");
    assert_eq!(
        header_string(&headers, "x-death-reason").as_deref(),
        Some("handler returned Fatal")
    );
    assert_eq!(
        header_string(&headers, "x-original-queue").as_deref(),
        Some(queue.as_str())
    );
    assert_eq!(header_u32(&headers, "x-attempts"), Some(3));

    let _ = channel.close(200, "OK".into()).await;
    cleanup(&control, &queue).await;
}

#[tokio::test]
async fn publishing_to_a_missing_queue_is_an_error() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    // Nothing is ever declared, so the default exchange has no route. A
    // non-mandatory publish would be silently discarded and still confirmed;
    // a mandatory one is returned and must surface as an error.
    let queue = unique_queue("undeclared");
    let backend = RabbitMqBackend::connect(&url).await.expect("connect");
    let control = control(&url).await;

    let error = backend
        .publish(&envelope(&queue, 1), None)
        .await
        .expect_err("publishing to a queue that does not exist must fail");
    let text = error.to_string();
    assert!(
        text.contains(&queue),
        "error did not name the queue: {text}"
    );
    assert!(
        text.contains("unroutable"),
        "error did not explain the return: {text}"
    );

    // The publishing channel survives a return, so the backend stays usable.
    let config = QueueConfig::new(queue.clone());
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");
    backend
        .publish(&envelope(&queue, 1), None)
        .await
        .expect("publish after declaring");
    await_count(&control, &queue, 1).await;

    backend.close().await.expect("close");
    cleanup(&control, &queue).await;
}

#[tokio::test]
async fn dead_letter_rejects_when_dead_letter_queues_are_disabled() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let queue = unique_queue("dead-nodlq");
    let dead = format!("{queue}.dead");
    let config = QueueConfig::new(queue.clone());
    let backend = RabbitMqBackend::with_options(
        &url,
        RabbitMqOptions::default().declare_dead_letter_queues(false),
    )
    .await
    .expect("connect");
    let control = control(&url).await;
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");

    backend
        .publish(&envelope(&queue, 3), None)
        .await
        .expect("publish");

    let mut stream = backend.consume(&config).await.expect("consume");
    let delivery = next_delivery(&mut stream, Duration::from_secs(5)).await;
    // No `q.dead` is published to, so this must not fail on an unroutable
    // publish: the delivery is rejected instead.
    delivery
        .dead_letter("handler returned Fatal")
        .await
        .expect("dead_letter must succeed by rejecting");

    drop(stream);
    backend.close().await.expect("close");

    // Settled, not redelivered, and no dead-letter queue was conjured up.
    await_count(&control, &queue, 0).await;
    assert!(
        !queue_exists(&control, &dead).await,
        "`{dead}` must not exist when dead-letter queues are disabled"
    );

    cleanup(&control, &queue).await;
}

#[tokio::test]
async fn malformed_body_is_skipped_without_stalling() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let queue = unique_queue("malformed");
    let dead = format!("{queue}.dead");
    let config = QueueConfig::new(queue.clone());
    let backend = RabbitMqBackend::connect(&url).await.expect("connect");
    let control = control(&url).await;
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");

    // Publish the garbage first and wait for it to settle, so the ordering of
    // the two messages on the queue is not in doubt.
    let channel = control.create_channel().await.expect("channel");
    publish_raw(&channel, &queue, b"{ this is not an envelope ]").await;
    await_count(&control, &queue, 1).await;

    let sent = envelope(&queue, 1);
    backend.publish(&sent, None).await.expect("publish");
    await_count(&control, &queue, 2).await;

    let mut stream = backend.consume(&config).await.expect("consume");

    // The malformed body never reaches the caller, and does not block the good
    // message behind it.
    let delivery = next_delivery(&mut stream, Duration::from_secs(5)).await;
    assert_eq!(delivery.envelope(), &sent);
    delivery.ack().await.expect("ack");
    expect_idle(&mut stream, Duration::from_millis(500)).await;

    drop(stream);
    backend.close().await.expect("close");
    await_count(&control, &queue, 0).await;

    // The bytes are preserved on the dead-letter queue for inspection.
    await_count(&control, &dead, 1).await;
    let message = channel
        .basic_get(dead.as_str().into(), BasicGetOptions { no_ack: true })
        .await
        .expect("basic_get")
        .expect("the malformed body");
    assert_eq!(message.delivery.data, b"{ this is not an envelope ]");
    let headers = message
        .delivery
        .properties
        .headers()
        .clone()
        .expect("headers");
    assert_eq!(
        header_string(&headers, "x-death-reason").as_deref(),
        Some("malformed envelope")
    );
    assert_eq!(
        header_string(&headers, "x-original-queue").as_deref(),
        Some(queue.as_str())
    );

    let _ = channel.close(200, "OK".into()).await;
    cleanup(&control, &queue).await;
}

#[tokio::test]
async fn malformed_body_is_rejected_when_dead_letter_queues_are_disabled() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let queue = unique_queue("malformed-nodlq");
    let config = QueueConfig::new(queue.clone());
    let backend = RabbitMqBackend::with_options(
        &url,
        RabbitMqOptions::default().declare_dead_letter_queues(false),
    )
    .await
    .expect("connect");
    let control = control(&url).await;
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");

    let channel = control.create_channel().await.expect("channel");
    publish_raw(&channel, &queue, b"nonsense").await;
    await_count(&control, &queue, 1).await;

    let mut stream = backend.consume(&config).await.expect("consume");
    // Nothing is ever yielded, and the message is dropped rather than
    // redelivered forever.
    expect_idle(&mut stream, Duration::from_secs(2)).await;

    drop(stream);
    backend.close().await.expect("close");
    await_count(&control, &queue, 0).await;

    let _ = channel.close(200, "OK".into()).await;
    cleanup(&control, &queue).await;
}

#[tokio::test]
async fn close_ends_the_consumer_stream() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let queue = unique_queue("close");
    let config = QueueConfig::new(queue.clone());
    let backend = RabbitMqBackend::connect(&url).await.expect("connect");
    let control = control(&url).await;
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");

    let mut stream = backend.consume(&config).await.expect("consume");
    expect_idle(&mut stream, Duration::from_millis(250)).await;

    backend.close().await.expect("close");

    // The stream terminates. A closing connection may surface a final error
    // item first; what matters is that it ends rather than hanging.
    let ended = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(item) = stream.next().await {
            if let Ok(delivery) = item {
                panic!(
                    "unexpected delivery after close: {}",
                    delivery.envelope().job_id
                );
            }
        }
    })
    .await;
    assert!(ended.is_ok(), "consumer stream did not end after close()");

    cleanup(&control, &queue).await;
}

// ---------------------------------------------------------------------------
// deferral
// ---------------------------------------------------------------------------

#[tokio::test]
async fn deferred_job_reappears_after_ttl() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let queue = unique_queue("defer-ttl");
    let config = QueueConfig::new(queue.clone());
    // A one-second deferral puts `x-expires` at 2s (always twice the TTL, never
    // tunable), so the test can watch the broker clean the hold queue up.
    let backend = RabbitMqBackend::connect(&url).await.expect("connect");
    let control = control(&url).await;
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");

    // The default granularity is 1s, so a 1s delay lands in `q.deferred.1000`.
    let hold = backend.deferred_queue_name(&queue, 1_000);
    assert_eq!(hold, format!("{queue}.deferred.1000"));

    // Attempt 2 on purpose: a deferral must not touch the attempt counter.
    let held = envelope(&queue, 2).deferred(10);
    let started = Instant::now();
    backend
        .defer(&held, Duration::from_secs(1))
        .await
        .expect("defer");

    // The hold queue is created on demand and holds the job while it waits.
    assert!(
        queue_exists(&control, &hold).await,
        "`{hold}` must exist while the job is held"
    );
    assert_eq!(ready_count(&control, &hold).await, 1);

    let mut stream = backend.consume(&config).await.expect("consume");
    // Nothing before the TTL is up ...
    expect_idle(&mut stream, Duration::from_millis(600)).await;

    // ... and then the job, byte for byte as it was deferred.
    let delivery = next_delivery(&mut stream, Duration::from_secs(8)).await;
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(900),
        "job came back after {elapsed:?}, expected to wait about a second"
    );
    assert_eq!(delivery.envelope(), &held);
    assert_eq!(delivery.envelope().deferrals, 1);
    assert_eq!(delivery.envelope().priority, 10);
    assert_eq!(
        delivery.envelope().attempt,
        2,
        "a deferral is not a failed attempt"
    );
    delivery.ack().await.expect("ack");

    drop(stream);
    backend.close().await.expect("close");

    // `x-expires` is 2 * ttl = 2s, counted from the last time the queue was
    // used. Checked exactly once, well past that: a passive declare is itself a
    // use, so polling for the deletion would keep postponing it.
    tokio::time::sleep_until((started + Duration::from_millis(3_500)).into()).await;
    assert!(
        !queue_exists(&control, &hold).await,
        "`{hold}` must have expired once it went unused for ttl + grace"
    );

    cleanup(&control, &queue).await;
    delete_queues(&control, &[hold]).await;
}

#[tokio::test]
async fn deferred_job_is_consumed_before_the_backlog() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let queue = unique_queue("defer-priority");
    // The default `max_priority` is 10 levels, which is what makes this work.
    let config = QueueConfig::new(queue.clone());
    let backend = RabbitMqBackend::connect(&url).await.expect("connect");
    let control = control(&url).await;
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");

    // A backlog, piled up with nobody consuming.
    let first = envelope(&queue, 1);
    let second = envelope(&queue, 2);
    backend.publish(&first, None).await.expect("publish");
    backend.publish(&second, None).await.expect("publish");
    await_count(&control, &queue, 2).await;

    let held = envelope(&queue, 7).deferred(10);
    backend
        .defer(&held, Duration::from_secs(1))
        .await
        .expect("defer");

    // Wait for the broker to move it back onto `q`, so all three are ready and
    // the ordering below is about priority rather than about arrival time.
    await_count(&control, &queue, 3).await;

    let mut stream = backend.consume(&config).await.expect("consume");
    let deferred = next_delivery(&mut stream, Duration::from_secs(5)).await;
    assert_eq!(
        deferred.envelope(),
        &held,
        "the deferred job must overtake the backlog"
    );
    deferred.ack().await.expect("ack");

    // ... and the backlog then follows in its original order.
    let one = next_delivery(&mut stream, Duration::from_secs(5)).await;
    assert_eq!(one.envelope(), &first);
    one.ack().await.expect("ack");
    let two = next_delivery(&mut stream, Duration::from_secs(5)).await;
    assert_eq!(two.envelope(), &second);
    two.ack().await.expect("ack");

    drop(stream);
    backend.close().await.expect("close");
    await_count(&control, &queue, 0).await;

    cleanup(&control, &queue).await;
    delete_queues(&control, &[format!("{queue}.deferred.1000")]).await;
}

#[tokio::test]
async fn queue_without_priorities_still_defers() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let queue = unique_queue("defer-nopriority");
    let config = QueueConfig::new(queue.clone()).max_priority(0);
    assert_eq!(config.max_priority, None);
    let backend = RabbitMqBackend::connect(&url).await.expect("connect");
    let control = control(&url).await;
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare must succeed without priorities");

    // Redeclaring with no arguments at all succeeds, which is only possible if
    // the queue really was declared without `x-max-priority`.
    redeclare_raw(&control, &queue, FieldTable::default())
        .await
        .expect("`q` must have been declared without any arguments");

    // `Envelope::deferred(max_priority.unwrap_or(0))` is what the worker does.
    let held = envelope(&queue, 1).deferred(0);
    backend
        .defer(&held, Duration::from_secs(1))
        .await
        .expect("defer");

    let mut stream = backend.consume(&config).await.expect("consume");
    let delivery = next_delivery(&mut stream, Duration::from_secs(8)).await;
    assert_eq!(delivery.envelope(), &held);
    assert_eq!(delivery.envelope().priority, 0);
    assert_eq!(delivery.envelope().deferrals, 1);
    delivery.ack().await.expect("ack");

    drop(stream);
    backend.close().await.expect("close");

    cleanup(&control, &queue).await;
    delete_queues(&control, &[format!("{queue}.deferred.1000")]).await;
}

#[tokio::test]
async fn redeclaring_with_different_max_priority_is_an_error() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let queue = unique_queue("defer-redeclare");
    let with_priorities = QueueConfig::new(queue.clone()).max_priority(10);
    let without = QueueConfig::new(queue.clone()).max_priority(0);
    let backend = RabbitMqBackend::connect(&url).await.expect("connect");
    let control = control(&url).await;

    backend
        .declare(std::slice::from_ref(&with_priorities))
        .await
        .expect("declare");

    // This is the upgrade hazard the crate docs warn about: `x-max-priority` is
    // a declaration argument, so it cannot be changed on an existing queue. It
    // must come back as an error rather than hanging on the dead channel.
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        backend.declare(std::slice::from_ref(&without)),
    )
    .await
    .expect("redeclaring must fail rather than hang")
    .expect_err("redeclaring with different arguments must be an error");
    let text = error.to_string();
    assert!(
        text.contains("PRECONDITION_FAILED"),
        "error did not explain the refusal: {text}"
    );

    // A second backend hits the same wall, and neither is left unusable: the
    // declaration ran on a throwaway channel, and the publishing channel is
    // reopened lazily if it ever did die.
    let other = RabbitMqBackend::connect(&url).await.expect("connect");
    other
        .declare(std::slice::from_ref(&without))
        .await
        .expect_err("a fresh backend must be refused too");
    other
        .publish(&envelope(&queue, 1), None)
        .await
        .expect("publishing must still work after a refused declaration");
    other.close().await.expect("close");

    backend
        .publish(&envelope(&queue, 2), None)
        .await
        .expect("publishing must still work after a refused declaration");
    await_count(&control, &queue, 2).await;

    // The original arguments survived untouched.
    backend
        .declare(std::slice::from_ref(&with_priorities))
        .await
        .expect("the queue still has its original arguments");

    backend.close().await.expect("close");
    cleanup(&control, &queue).await;
}

#[tokio::test]
async fn delivery_defer_acks_the_original_before_holding_the_next() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let queue = unique_queue("defer-delivery");
    let config = QueueConfig::new(queue.clone());
    let backend = RabbitMqBackend::connect(&url).await.expect("connect");
    let control = control(&url).await;
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");

    let sent = envelope(&queue, 1);
    backend.publish(&sent, None).await.expect("publish");

    let mut stream = backend.consume(&config).await.expect("consume");
    let delivery = next_delivery(&mut stream, Duration::from_secs(5)).await;

    // What a handler that returned `JobError::Deferred` leads to.
    let held = delivery.envelope().deferred(10);
    delivery
        .defer(held.clone(), Duration::from_secs(2))
        .await
        .expect("defer");

    let hold = backend.deferred_queue_name(&queue, 2_000);
    assert_eq!(ready_count(&control, &hold).await, 1, "held in `{hold}`");

    // Closing would requeue anything left unacked, so `q` staying empty is the
    // proof that the original was acked after the hold publish landed.
    drop(stream);
    backend.close().await.expect("close");
    assert_eq!(
        ready_count(&control, &queue).await,
        0,
        "the original must have been acked, not requeued"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(ready_count(&control, &queue).await, 0, "still nothing due");

    // ... and the deferral survives the backend that created it, because the
    // hold queue is the broker's now.
    await_count(&control, &queue, 1).await;
    let channel = control.create_channel().await.expect("channel");
    let message = channel
        .basic_get(queue.as_str().into(), BasicGetOptions { no_ack: true })
        .await
        .expect("basic_get")
        .expect("the deferred job");
    assert_eq!(
        Envelope::from_bytes(&message.delivery.data).expect("decode"),
        held
    );
    assert_eq!(*message.delivery.properties.priority(), Some(10));
    let headers = message
        .delivery
        .properties
        .headers()
        .clone()
        .expect("headers");
    assert_eq!(header_u32(&headers, "x-deferrals"), Some(1));
    assert_eq!(header_u32(&headers, "x-attempt"), Some(1));
    let _ = channel.close(200, "OK".into()).await;

    cleanup(&control, &queue).await;
    delete_queues(&control, &[hold]).await;
}

#[tokio::test]
async fn hold_queue_with_foreign_arguments_makes_defer_fail_and_leaves_the_original_unacked() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let queue = unique_queue("defer-foreign");
    let config = QueueConfig::new(queue.clone());
    let backend = RabbitMqBackend::connect(&url).await.expect("connect");
    let control = control(&url).await;
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");

    // Somebody else got to `q.deferred.1000` first and declared it with
    // different arguments, here without the dead-letter routing that sends
    // expired messages back to `q`. RabbitMQ will refuse this backend's
    // declaration with PRECONDITION_FAILED and close the channel it ran on.
    let hold = backend.deferred_queue_name(&queue, 1_000);
    let mut foreign = FieldTable::default();
    foreign.insert("x-message-ttl".into(), AMQPValue::LongLongInt(1_000));
    redeclare_raw(&control, &hold, foreign)
        .await
        .expect("pre-declaring the hold queue with foreign arguments");

    let sent = envelope(&queue, 1);
    backend.publish(&sent, None).await.expect("publish");

    let mut stream = backend.consume(&config).await.expect("consume");
    let delivery = next_delivery(&mut stream, Duration::from_secs(5)).await;

    let held = delivery.envelope().deferred(10);
    let error = delivery
        .defer(held, Duration::from_secs(1))
        .await
        .expect_err("deferring into a hold queue with foreign arguments must fail");
    let text = error.to_string();
    assert!(
        text.contains("PRECONDITION_FAILED"),
        "error did not explain the refusal: {text}"
    );

    // Nothing was smuggled into the foreign hold queue.
    assert_eq!(ready_count(&control, &hold).await, 0);

    // The refusal cost exactly that one deferral: the declaration ran on its own
    // channel, so the shared confirm channel is untouched and a normal publish
    // on the same backend still works.
    let extra = envelope(&queue, 9);
    backend
        .publish(&extra, None)
        .await
        .expect("publishing must still work after a refused hold queue declaration");

    // Closing requeues whatever was left unacked, so the broker still owning the
    // original is the proof that `defer` did not ack it.
    drop(stream);
    backend.close().await.expect("close");
    await_count(&control, &queue, 2).await;

    cleanup(&control, &queue).await;
    delete_queues(&control, &[hold]).await;
}

#[tokio::test]
async fn deferring_onto_an_undeclared_queue_is_refused() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    // What `Producer::new_undeclared` gives you: a backend that never declared
    // this queue. A hold queue would have to guess the durability and would
    // dead-letter into a routing key that matches nothing, and unlike a
    // `mandatory` publish, a TTL expiry into a missing queue is dropped in
    // silence. So it is refused instead.
    let queue = unique_queue("defer-undeclared");
    let backend = RabbitMqBackend::connect(&url).await.expect("connect");
    let control = control(&url).await;

    let held = envelope(&queue, 1).deferred(10);
    let error = backend
        .defer(&held, Duration::from_secs(1))
        .await
        .expect_err("deferring onto a queue this backend never declared must fail");
    assert!(
        matches!(&error, Error::UnknownQueue(name) if name == &queue),
        "expected UnknownQueue, got {error:?}"
    );

    let hold = backend.deferred_queue_name(&queue, 1_000);
    assert!(
        !queue_exists(&control, &hold).await,
        "`{hold}` must not have been created"
    );

    // Declaring the queue is all it takes; the backend was never poisoned.
    let config = QueueConfig::new(queue.clone());
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");
    backend
        .defer(&held, Duration::from_secs(1))
        .await
        .expect("deferring must work once the queue has been declared");
    assert_eq!(ready_count(&control, &hold).await, 1);

    backend.close().await.expect("close");
    cleanup(&control, &queue).await;
    delete_queues(&control, &[hold]).await;
}

#[tokio::test]
async fn deferral_past_the_cap_is_refused() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let queue = unique_queue("defer-too-long");
    let config = QueueConfig::new(queue.clone());
    let backend = RabbitMqBackend::connect(&url).await.expect("connect");
    let control = control(&url).await;
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");

    // Thirty days is past `MAX_DEFERRAL_MS` (~24.8 days). The old behaviour was
    // to clamp, which released the job about five days early; refusing is the
    // only answer that keeps "never early".
    let held = envelope(&queue, 1).deferred(10);
    let thirty_days = Duration::from_secs(30 * 86_400);
    let error = backend
        .defer(&held, thirty_days)
        .await
        .expect_err("a deferral past the cap must fail");
    let text = error.to_string();
    assert!(
        text.contains("longer than a hold queue can wait"),
        "error did not explain the cap: {text}"
    );

    // Nothing was published anywhere, and no hold queue was created.
    assert_eq!(ready_count(&control, &queue).await, 0);
    let absurd = backend.deferred_queue_name(&queue, 2_592_000_000);
    assert!(!queue_exists(&control, &absurd).await, "`{absurd}` exists");

    // A delay inside the cap still works on the same backend.
    let hold = backend.deferred_queue_name(&queue, 1_000);
    backend
        .defer(&held, Duration::from_secs(1))
        .await
        .expect("a delay within the cap must still work");
    assert_eq!(ready_count(&control, &hold).await, 1);

    backend.close().await.expect("close");
    cleanup(&control, &queue).await;
    delete_queues(&control, &[hold]).await;
}

#[tokio::test]
async fn a_borrowed_channel_works_an_application_queue_on_the_same_connection() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let queue = unique_queue("borrowed-jobs");
    // Not part of the library's topology: no suffix it knows, nothing declared
    // through `Backend::declare`. This is the queue an application shares with
    // some other system.
    let app = unique_queue("borrowed-app");
    let config = QueueConfig::new(queue.clone());
    let backend = RabbitMqBackend::connect(&url).await.expect("connect");
    let control = control(&url).await;
    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");

    let opened_on = backend.connection_generation();
    let channel = backend.create_channel().await.expect("create_channel");

    // Declaring on the borrowed channel is the caller's own business: the
    // backend neither knows about this queue nor deletes it.
    channel
        .queue_declare(
            app.as_str().into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("declare the application queue");

    let body: &[u8] = b"not an envelope, and nothing here should care";
    publish_raw(&channel, &app, body).await;
    assert_eq!(ready_count(&control, &app).await, 1);

    let message = channel
        .basic_get(app.as_str().into(), BasicGetOptions { no_ack: true })
        .await
        .expect("basic_get")
        .expect("the message published on the borrowed channel")
        .delivery;
    assert_eq!(message.data.as_slice(), body, "the body came back verbatim");

    // The backend's own channels are untouched by any of that, in particular by
    // the `confirm_select` the raw publish put on the borrowed channel.
    backend
        .publish(&envelope(&queue, 1), None)
        .await
        .expect("the backend still publishes on its own channel");
    await_count(&control, &queue, 1).await;

    // No reconnect happened, so the borrowed channel is still the one that was
    // handed out: the generation a caller would compare against has not moved.
    assert_eq!(
        backend.connection_generation(),
        opened_on,
        "the connection was never replaced during this exchange"
    );
    assert!(channel.status().connected(), "the borrowed channel is live");

    // Closing it is the caller's job; the backend would not do it.
    channel.close(200, "OK".into()).await.expect("close");

    backend.close().await.expect("close");
    cleanup(&control, &queue).await;
    delete_queues(&control, &[app]).await;
}

// ---------------------------------------------------------------------------
// reconnection
// ---------------------------------------------------------------------------

/// A TCP proxy in front of the broker, so a test can cut the connection the way
/// a network partition or a broker restart would.
///
/// The alternative, closing the connection through the management HTTP API,
/// needs the management plugin, credentials for it and a way to find our own
/// connection among everyone else's. A proxy needs none of that, cuts exactly
/// the connections this test owns, and cuts them at a moment the test chooses.
/// Reconnects go through the same proxy, so recovery is observable too.
struct BrokerProxy {
    addr: std::net::SocketAddr,
    /// Every live forwarding task holds a receiver; sending drops the sockets.
    cut: tokio::sync::broadcast::Sender<()>,
}

impl BrokerProxy {
    /// Start listening on an ephemeral port, forwarding to `upstream`.
    async fn start(upstream: String) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the proxy");
        let addr = listener.local_addr().expect("proxy address");
        let (cut, _) = tokio::sync::broadcast::channel(16);

        let accepts = cut.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut inbound, _)) = listener.accept().await else {
                    return;
                };
                let upstream = upstream.clone();
                let mut stop = accepts.subscribe();
                tokio::spawn(async move {
                    let Ok(mut outbound) = tokio::net::TcpStream::connect(&upstream).await else {
                        return;
                    };
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
                        // Returning drops both sockets, which is what the client
                        // sees as a connection that died under it.
                        _ = stop.recv() => {}
                    }
                });
            }
        });

        Self { addr, cut }
    }

    /// Drop every connection currently going through the proxy.
    ///
    /// New connections are still accepted afterwards, so this is a blip rather
    /// than an outage. [`Self::stop_accepting`] is the other half.
    fn cut(&self) {
        let _ = self.cut.send(());
    }

    /// The AMQP URL that reaches the broker through this proxy.
    fn url(&self, direct: &str) -> String {
        let (head, _, tail) = split_url(direct);
        format!("{head}{}{tail}", self.addr)
    }
}

/// Split an AMQP URL into everything up to the host, the `host:port`, and the
/// rest (vhost and query).
///
/// Only the host and port are ever replaced; credentials and vhost have to
/// survive intact or the proxied connection would not authenticate.
fn split_url(url: &str) -> (&str, &str, &str) {
    let scheme_end = url.find("://").expect("an amqp:// url") + "://".len();
    let rest = &url[scheme_end..];
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let tail = &rest[authority_end..];

    match authority.rfind('@') {
        // Credentials stay in the head, so only `host:port` is swapped.
        Some(at) => (&url[..scheme_end + at + 1], &authority[at + 1..], tail),
        None => (&url[..scheme_end], authority, tail),
    }
}

/// The `host:port` an AMQP URL points at, with the AMQP default port filled in.
fn upstream_addr(url: &str) -> String {
    let (_, hostport, _) = split_url(url);
    if hostport.contains(':') {
        hostport.to_owned()
    } else {
        format!("{hostport}:5672")
    }
}

/// Wait until the backend has noticed that its connection is gone.
///
/// Cutting the sockets and immediately asserting would race the client: lapin
/// marks the connection dead when its IO loop next touches it, not when the
/// packets stop.
async fn await_disconnected(backend: &RabbitMqBackend) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if !backend.is_connected() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("the backend never noticed its connection was cut");
}

#[tokio::test]
async fn publishing_reconnects_after_the_connection_drops() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let proxy = BrokerProxy::start(upstream_addr(&url)).await;
    let queue = unique_queue("reconnect-publish");
    let config = QueueConfig::new(queue.clone());
    let backend = RabbitMqBackend::connect(&proxy.url(&url))
        .await
        .expect("connect through the proxy");
    // Assertions go over a direct connection: the proxy is the thing under test.
    let control = control(&url).await;

    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");
    backend
        .publish(&envelope(&queue, 1), None)
        .await
        .expect("publish before the cut");
    await_count(&control, &queue, 1).await;

    proxy.cut();
    await_disconnected(&backend).await;

    // The publish is what notices the connection is gone: it must wait for a
    // replacement rather than fail.
    backend
        .publish(&envelope(&queue, 2), None)
        .await
        .expect("publish after the cut must reconnect, not fail");
    await_count(&control, &queue, 2).await;
    assert!(
        backend.is_connected(),
        "the backend is back on a connection"
    );

    cleanup(&control, &queue).await;
    backend.close().await.expect("close");
}

#[tokio::test]
async fn a_consumer_resubscribes_after_the_connection_drops() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let proxy = BrokerProxy::start(upstream_addr(&url)).await;
    let queue = unique_queue("reconnect-consume");
    let config = QueueConfig::new(queue.clone());
    let backend = RabbitMqBackend::connect(&proxy.url(&url))
        .await
        .expect("connect through the proxy");
    let control = control(&url).await;

    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");
    let mut stream = backend.consume(&config).await.expect("consume");

    backend
        .publish(&envelope(&queue, 1), None)
        .await
        .expect("publish");
    let first = next_delivery(&mut stream, Duration::from_secs(5)).await;
    assert_eq!(first.envelope().attempt, 1);
    first.ack().await.expect("ack before the cut");

    proxy.cut();
    await_disconnected(&backend).await;

    // The same stream, across a connection it did not start on. A job published
    // after the cut has to come out of it, which it only can if the consumer
    // resubscribed on the new connection.
    backend
        .publish(&envelope(&queue, 2), None)
        .await
        .expect("publish after the cut");

    // The first job may well arrive a second time. `basic_ack` is a
    // fire-and-forget AMQP frame with no broker confirmation, so cutting the
    // socket can drop the ack before the broker processed it, and the broker
    // then requeues the delivery. That is the at-least-once contract this
    // backend documents, not a failure of the resubscribe, so tolerate the
    // duplicate and keep reading until the job published after the cut shows up.
    let mut resubscribed = false;
    for _ in 0..3 {
        let delivery = next_delivery(&mut stream, Duration::from_secs(15)).await;
        let attempt = delivery.envelope().attempt;
        delivery.ack().await.expect("ack after the cut");
        if attempt == 2 {
            resubscribed = true;
            break;
        }
        assert_eq!(
            attempt, 1,
            "only the job that was in flight across the cut can be redelivered"
        );
    }
    assert!(
        resubscribed,
        "the resubscribed consumer never delivered the job published after the cut"
    );

    drop(stream);
    cleanup(&control, &queue).await;
    backend.close().await.expect("close");
}

#[tokio::test]
async fn a_reconnect_redeclares_the_queues_it_had_declared() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let proxy = BrokerProxy::start(upstream_addr(&url)).await;
    let queue = unique_queue("reconnect-redeclare");
    let config = QueueConfig::new(queue.clone());
    let backend = RabbitMqBackend::connect(&proxy.url(&url))
        .await
        .expect("connect through the proxy");
    let control = control(&url).await;

    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");

    proxy.cut();
    await_disconnected(&backend).await;

    // Delete the queues while the backend is away. This is what a broker that
    // restarted looks like from the client's side: the connection came back,
    // but the topology on it did not. Nothing here can race the backend, since
    // with no consumer running nothing asks for a connection until we do.
    delete_queues(&control, &family(&queue)).await;
    assert!(!queue_exists(&control, &queue).await, "the queue is gone");

    // Publishes are `mandatory`, so this can only succeed if the reconnect put
    // the queue back first.
    backend
        .publish(&envelope(&queue, 1), None)
        .await
        .expect("publish after the cut must find a re-declared queue");
    await_count(&control, &queue, 1).await;
    assert!(
        queue_exists(&control, &backend.dead_queue_name(&queue)).await,
        "the dead-letter queue is re-declared too"
    );

    cleanup(&control, &queue).await;
    backend.close().await.expect("close");
}

#[tokio::test]
async fn reconnection_can_be_turned_off() {
    let Some(url) = std::env::var("AMQP_URL").ok() else {
        eprintln!("skipping: AMQP_URL not set");
        return;
    };

    let proxy = BrokerProxy::start(upstream_addr(&url)).await;
    let queue = unique_queue("reconnect-disabled");
    let config = QueueConfig::new(queue.clone());
    let backend =
        RabbitMqBackend::with_options(&proxy.url(&url), RabbitMqOptions::default().reconnect(None))
            .await
            .expect("connect through the proxy");
    let control = control(&url).await;

    backend
        .declare(std::slice::from_ref(&config))
        .await
        .expect("declare");
    let mut stream = backend.consume(&config).await.expect("consume");

    proxy.cut();
    await_disconnected(&backend).await;

    // The pre-reconnection contract: the stream ends rather than recovering,
    // which is what `Worker::run` turns into `Error::ConsumerStopped`.
    match tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
        Ok(None) => {}
        Ok(Some(Ok(_))) => panic!("a delivery arrived on a connection that was cut"),
        // lapin may report the drop as a stream error before ending the stream;
        // either way the stream is over, which is the point.
        Ok(Some(Err(_))) => {}
        Err(_) => panic!("the consumer stream neither ended nor errored"),
    }

    let error = backend
        .publish(&envelope(&queue, 1), None)
        .await
        .expect_err("publishing must fail rather than reconnect");
    assert!(
        error.to_string().contains("reconnection is disabled"),
        "unexpected error: {error}"
    );

    drop(stream);
    cleanup(&control, &queue).await;
    let _ = backend.close().await;
}
