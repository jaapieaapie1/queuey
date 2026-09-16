//! In-memory backend for tests and local development.
//!
//! Contract:
//! * `MemoryBackend::new()`; `Clone`-able handle over shared `Arc` state.
//! * `publish` with `delay` uses `tokio::time::sleep` in a spawned task (so it is
//!   compatible with `tokio::time::pause()` / `advance()` in tests).
//! * `consume` honours `prefetch` (at most `prefetch` un-acked deliveries per consumer).
//! * `retry` re-publishes `next` after `delay`, then acks the original.
//! * `defer` holds `next` for `delay` and then inserts it *by priority*, then acks the
//!   original. The hold is a `tokio::time::sleep` too, so it is virtual-time friendly.
//! * Every enqueue (plain, delayed or deferred) goes through one priority insertion:
//!   the envelope lands behind every pending envelope whose `priority` is greater than
//!   or equal to its own and ahead of the rest, so equal priorities stay FIFO. A hold
//!   ends inside that same critical section, so no envelope is ever counted by both
//!   `deferred` and `pending`.
//! * `dead_letter` moves the envelope into an inspectable `dead_letters(queue)` list with reason.
//! * Test helpers: `pending(queue) -> usize`, `deferred(queue) -> usize`,
//!   `dead_letters(queue) -> Vec<(Envelope, String)>`, `acked(queue) -> Vec<Envelope>`,
//!   `succeeded(queue) -> Vec<Envelope>`, `retried(queue) -> Vec<Envelope>`,
//!   `settled(queue) -> Vec<(Envelope, AckKind)>`. `acked` is every attempt an ack
//!   settled (success, retry *and* deferral), so assert on `succeeded` when the
//!   question is "did this job succeed"; `settled` carries the [`AckKind`] of each.
//! * `close` ends all consumer streams. Afterwards `declare`, `publish`, `defer`,
//!   `retry` and `dead_letter` all fail with [`Error::ShutDown`]; a plain `ack` still
//!   records. A delayed publish or a deferral whose timer fires *after* the close is
//!   dropped, exactly like a broker that went away mid-wait.
//!
//! Implementation notes:
//! * Each consumer owns a [`tokio::sync::Semaphore`] with `prefetch` permits. A permit
//!   is moved into every [`Delivery`] handed out and released on ack / retry /
//!   dead-letter, which is what caps the number of outstanding deliveries. The
//!   semaphore is dropped from the queue when the consumer task ends.
//! * Dropping a stream requeues every message that was produced for it but not handed
//!   out yet, so a message can never vanish just because a consumer went away.
//! * A delivery that is dropped without being settled releases its permit but the
//!   message is *not* requeued (unlike a real broker). Tests should settle deliveries.

use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll},
    time::Duration,
};

use async_trait::async_trait;
use futures::Stream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};

use crate::{
    backend::{Backend, Delivery, DeliveryStream},
    envelope::Envelope,
    error::{Error, Result},
    queue::QueueConfig,
};

/// Why a delivery attempt was acked.
///
/// Three different outcomes all end in an ack on the wire: the job succeeded, a retry
/// was scheduled, or a deferral was held. The ack alone cannot tell them apart.
/// Recording the reason is what lets [`MemoryBackend::succeeded`] mean "the handler
/// returned `Ok(())`" while [`MemoryBackend::acked`] keeps meaning "settled by an ack".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum AckKind {
    /// The handler returned `Ok(())` and [`Delivery::ack`] settled the attempt. This is
    /// the only kind that means the job is done.
    Succeeded,
    /// [`Delivery::retry`] acked the original because a successor was published for a
    /// later attempt. The job is not done: a copy with `attempt + 1` follows.
    Retried,
    /// [`Delivery::defer`] acked the original because a successor was put in hold. The
    /// job is not done, and its `attempt` has not moved, because a deferral is not a
    /// retry.
    Deferred,
}

/// State of one queue inside the backend.
struct QueueState {
    /// Config from the first `declare` (or a default one, for implicit queues).
    config: QueueConfig,
    /// Messages waiting to be handed to a consumer.
    pending: VecDeque<Envelope>,
    /// Every attempt an ack settled, in settle order, each with the reason it was
    /// acked. One list rather than three keeps the ordering across kinds meaningful,
    /// which is what [`MemoryBackend::settled`] reports; the other helpers project it.
    settled: Vec<(Envelope, AckKind)>,
    /// Everything that was dead-lettered, with its reason.
    dead: Vec<(Envelope, String)>,
    /// One semaphore per live consumer; closing it stops that consumer.
    consumers: Vec<Arc<Semaphore>>,
    /// Deferred envelopes whose hold has not elapsed yet. Stands in for the hold
    /// queues a real broker would own.
    held: usize,
    /// Bumped whenever `pending` grows or the backend closes, to wake consumers.
    signal: watch::Sender<u64>,
}

impl QueueState {
    fn new(config: QueueConfig) -> Self {
        let (signal, _) = watch::channel(0);
        Self {
            config,
            pending: VecDeque::new(),
            settled: Vec::new(),
            dead: Vec::new(),
            consumers: Vec::new(),
            held: 0,
            signal,
        }
    }

    /// Queue `envelope` by priority: behind every pending envelope that is at least as
    /// important, ahead of the rest. Equal priorities therefore stay FIFO, and a
    /// deferred job (top priority) overtakes the normal backlog (priority `0`).
    ///
    /// The single insertion point for `publish` and `defer` alike, so priority
    /// ordering is one code path.
    fn insert_by_priority(&mut self, envelope: Envelope) {
        let position = self
            .pending
            .iter()
            .rposition(|pending| pending.priority >= envelope.priority)
            .map_or(0, |last| last + 1);
        self.pending.insert(position, envelope);
    }

    fn wake(&self) {
        self.signal.send_modify(|v| *v = v.wrapping_add(1));
    }
}

/// Everything the cloned handles share.
#[derive(Default)]
struct Shared {
    queues: HashMap<String, QueueState>,
    closed: bool,
}

impl Shared {
    fn queue_mut(&mut self, name: &str) -> &mut QueueState {
        self.queues
            .entry(name.to_owned())
            .or_insert_with(|| QueueState::new(QueueConfig::new(name)))
    }
}

/// An in-memory [`Backend`]. Cloning gives another handle onto the same state.
///
/// ```
/// # use queuey_core::MemoryBackend;
/// let backend = MemoryBackend::new();
/// let same_state = backend.clone();
/// assert_eq!(same_state.pending("nothing.here"), 0);
/// ```
#[derive(Clone, Default)]
pub struct MemoryBackend {
    state: Arc<Mutex<Shared>>,
}

impl std::fmt::Debug for MemoryBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.lock();
        f.debug_struct("MemoryBackend")
            .field("queues", &state.queues.keys().collect::<Vec<_>>())
            .field("closed", &state.closed)
            .finish()
    }
}

impl MemoryBackend {
    /// Create an empty backend with no queues declared.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, Shared> {
        // A panicking test must not poison every later assertion.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn lock_shared(state: &Arc<Mutex<Shared>>) -> MutexGuard<'_, Shared> {
        state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Number of messages waiting in `queue`.
    ///
    /// Messages that were handed to a consumer but not yet settled are *not* counted,
    /// and neither are delayed publishes whose delay has not elapsed.
    pub fn pending(&self, queue: &str) -> usize {
        self.lock().queues.get(queue).map_or(0, |q| q.pending.len())
    }

    /// Every delivery attempt on `queue` that was settled by an ack, in settle order.
    ///
    /// An ack is how *three* different outcomes end: the handler succeeded, a retry was
    /// scheduled ([`Delivery::retry`] acks the original), or a deferral was held
    /// ([`Delivery::defer`] does the same). So this counts attempts, not successes: a
    /// job that fails twice and then succeeds appears here three times, and one that
    /// exhausts its attempts and dead-letters appears once per attempt *except* the
    /// last, which is a dead letter and not an ack at all.
    ///
    /// For "did this job actually succeed", use [`MemoryBackend::succeeded`]; for the
    /// reason behind each ack, [`MemoryBackend::settled`].
    pub fn acked(&self, queue: &str) -> Vec<Envelope> {
        self.settled_by(queue, None)
    }

    /// Envelopes acked by [`Delivery::ack`] on `queue`, in ack order: the attempts whose
    /// handler returned `Ok(())`.
    ///
    /// This is what most assertions actually mean by "acked". It excludes the acks that
    /// retries and deferrals perform on the original envelope, so its length is the
    /// number of jobs that finished successfully, not the number of attempts made.
    pub fn succeeded(&self, queue: &str) -> Vec<Envelope> {
        self.settled_by(queue, Some(AckKind::Succeeded))
    }

    /// Envelopes acked on `queue` because [`Delivery::retry`] scheduled a further
    /// attempt, in retry order.
    ///
    /// One entry per failed-and-rescheduled attempt, carrying the `attempt` the handler
    /// saw; the rescheduled copy arrives separately with `attempt + 1`. Use it to assert
    /// how many times a job was retried without counting its final outcome.
    pub fn retried(&self, queue: &str) -> Vec<Envelope> {
        self.settled_by(queue, Some(AckKind::Retried))
    }

    /// Every ack on `queue` with the reason it happened, in settle order.
    ///
    /// The full record the other three helpers project from, for tests that care about
    /// the *sequence* of outcomes ("retried, retried, succeeded"), which neither a
    /// count nor a single filtered list can show.
    ///
    /// Note that the [`AckKind::Deferred`] entries here are the *originals* that a
    /// deferral acked; they are a historical record and do not shrink. That is a
    /// different thing from [`MemoryBackend::deferred`], which counts the successor
    /// envelopes sitting in hold right now and drops back to zero as the holds expire.
    pub fn settled(&self, queue: &str) -> Vec<(Envelope, AckKind)> {
        self.lock()
            .queues
            .get(queue)
            .map(|q| q.settled.clone())
            .unwrap_or_default()
    }

    /// Shared projection behind `acked` / `succeeded` / `retried`: every settled
    /// envelope, or only those with `kind`, under one lock.
    fn settled_by(&self, queue: &str, kind: Option<AckKind>) -> Vec<Envelope> {
        self.lock()
            .queues
            .get(queue)
            .map(|q| {
                q.settled
                    .iter()
                    .filter(|(_, recorded)| kind.is_none_or(|kind| *recorded == kind))
                    .map(|(envelope, _)| envelope.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Number of deferred envelopes still sitting in `queue`'s hold.
    ///
    /// These are the ones [`Backend::defer`] (or [`Delivery::defer`]) accepted but
    /// whose delay has not elapsed: they are not `pending` yet and no consumer can see
    /// them. The count drops back to zero as each hold expires or, if the backend
    /// was closed in the meantime, as each held envelope is dropped.
    ///
    /// A live count, not a history: for the attempts that *caused* a deferral, look for
    /// [`AckKind::Deferred`] in [`MemoryBackend::settled`].
    pub fn deferred(&self, queue: &str) -> usize {
        self.lock().queues.get(queue).map_or(0, |q| q.held)
    }

    /// Envelopes that were dead-lettered on `queue`, with the reason given.
    pub fn dead_letters(&self, queue: &str) -> Vec<(Envelope, String)> {
        self.lock()
            .queues
            .get(queue)
            .map(|q| q.dead.clone())
            .unwrap_or_default()
    }

    /// Names of every declared (or implicitly created) queue.
    pub fn queue_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.lock().queues.keys().cloned().collect();
        names.sort();
        names
    }

    /// Config recorded for `queue` by the first `declare` call, if any.
    pub fn queue_config(&self, queue: &str) -> Option<QueueConfig> {
        self.lock().queues.get(queue).map(|q| q.config.clone())
    }

    /// Whether [`Backend::close`] has been called.
    pub fn is_closed(&self) -> bool {
        self.lock().closed
    }

    /// Number of live consumers registered on `queue`.
    ///
    /// Internal state, exposed to this crate's tests so they can synchronise on a
    /// consumer having started (or gone) instead of sleeping.
    #[cfg(test)]
    pub(crate) fn consumer_count(&self, queue: &str) -> usize {
        self.lock()
            .queues
            .get(queue)
            .map_or(0, |q| q.consumers.len())
    }

    /// Whether [`Backend::close`] has been called, on a handle-less state pointer.
    fn is_state_closed(state: &Arc<Mutex<Shared>>) -> bool {
        Self::lock_shared(state).closed
    }

    /// Put an envelope on its queue right away, by priority, and wake any idle
    /// consumer.
    ///
    /// `hold`, when given, is the hold this envelope is leaving. Its decrement happens
    /// under the *same* lock as the insertion, so there is no window in which
    /// [`MemoryBackend::deferred`] and [`MemoryBackend::pending`] both count it.
    ///
    /// A closed backend accepts nothing, exactly like [`Backend::publish`], but the
    /// hold is still released, so the counter drops either way.
    fn enqueue_now(state: &Arc<Mutex<Shared>>, envelope: Envelope, hold: Option<HoldGuard>) {
        let mut shared = Self::lock_shared(state);
        if let Some(hold) = hold {
            hold.release(&mut shared);
        }
        if shared.closed {
            return;
        }
        let queue = shared.queue_mut(&envelope.queue);
        queue.insert_by_priority(envelope);
        queue.wake();
    }

    /// Enqueue `envelope` after `delay` (immediately when the delay is zero).
    fn schedule(state: &Arc<Mutex<Shared>>, envelope: Envelope, delay: Duration) {
        if delay.is_zero() {
            Self::enqueue_now(state, envelope, None);
            return;
        }
        // The deadline is taken now, not when the spawned task is first polled, so the
        // delay is measured from the publish call even under `tokio::time::pause()`.
        let deadline = tokio::time::Instant::now() + delay;
        let state = state.clone();
        tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            Self::enqueue_now(&state, envelope, None);
        });
    }

    /// Hold `envelope` for `delay`, then enqueue it by priority.
    ///
    /// The stand-in for a broker's hold queue: while it waits, the envelope is
    /// invisible to consumers and counted by [`MemoryBackend::deferred`]. A zero delay
    /// enqueues straight away, so the counter never moves. If the backend is closed
    /// before the hold expires the envelope is dropped, like a delayed publish.
    fn hold(state: &Arc<Mutex<Shared>>, envelope: Envelope, delay: Duration) {
        if delay.is_zero() {
            Self::enqueue_now(state, envelope, None);
            return;
        }
        // Same reasoning as `schedule`: the deadline is taken now, not when the
        // spawned task is first polled.
        let deadline = tokio::time::Instant::now() + delay;
        Self::lock_shared(state).queue_mut(&envelope.queue).held += 1;
        // The guard decrements again however the hold ends: it is handed to the
        // enqueue on expiry (one lock, no double count), and otherwise decrements on
        // drop, when the task is aborted or the runtime goes away.
        let guard = HoldGuard {
            state: state.clone(),
            queue: Some(envelope.queue.clone()),
        };
        let state = state.clone();
        tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            Self::enqueue_now(&state, envelope, Some(guard));
        });
    }
}

/// Keeps [`MemoryBackend::deferred`] honest: one live guard per envelope in hold.
struct HoldGuard {
    state: Arc<Mutex<Shared>>,
    /// The queue to decrement, taken once the decrement has happened.
    queue: Option<String>,
}

impl HoldGuard {
    /// Decrement under a lock the caller already holds, and disarm the `Drop`.
    ///
    /// This is what lets the hold end in the same critical section that enqueues the
    /// envelope, instead of one lock later.
    fn release(mut self, shared: &mut Shared) {
        if let Some(queue) = self.queue.take() {
            release_hold(shared, &queue);
        }
    }
}

impl Drop for HoldGuard {
    fn drop(&mut self) {
        if let Some(queue) = self.queue.take() {
            let mut shared = MemoryBackend::lock_shared(&self.state);
            release_hold(&mut shared, &queue);
        }
    }
}

/// One held envelope has left `queue`'s hold.
fn release_hold(shared: &mut Shared, queue: &str) {
    if let Some(queue) = shared.queues.get_mut(queue) {
        queue.held = queue.held.saturating_sub(1);
    }
}

#[async_trait]
impl Backend for MemoryBackend {
    async fn declare(&self, queues: &[QueueConfig]) -> Result<()> {
        let mut shared = self.lock();
        if shared.closed {
            return Err(Error::ShutDown);
        }
        for config in queues {
            // Idempotent: re-declaring never drops pending messages or consumers.
            shared
                .queues
                .entry(config.name.clone())
                .or_insert_with(|| QueueState::new(config.clone()));
        }
        Ok(())
    }

    async fn publish(&self, envelope: &Envelope, delay: Option<Duration>) -> Result<()> {
        if self.is_closed() {
            return Err(Error::ShutDown);
        }
        Self::schedule(&self.state, envelope.clone(), delay.unwrap_or_default());
        Ok(())
    }

    async fn defer(&self, envelope: &Envelope, delay: Duration) -> Result<()> {
        if self.is_closed() {
            return Err(Error::ShutDown);
        }
        Self::hold(&self.state, envelope.clone(), delay);
        Ok(())
    }

    async fn consume(&self, queue: &QueueConfig) -> Result<DeliveryStream> {
        let (tx, rx) = mpsc::unbounded_channel();
        let stream: DeliveryStream = Box::pin(DeliveryReceiver {
            rx,
            state: self.state.clone(),
            queue: queue.name.clone(),
        });

        let permits = if queue.prefetch == 0 {
            Semaphore::MAX_PERMITS
        } else {
            usize::from(queue.prefetch)
        };
        let semaphore = Arc::new(Semaphore::new(permits));

        let signal = {
            let mut shared = self.lock();
            if shared.closed {
                // Dropping `tx` ends the stream immediately.
                return Ok(stream);
            }
            let state = shared.queue_mut(&queue.name);
            state.consumers.push(semaphore.clone());
            state.signal.subscribe()
        };

        tokio::spawn(consumer_loop(
            self.state.clone(),
            queue.name.clone(),
            semaphore,
            tx,
            signal,
        ));
        Ok(stream)
    }

    async fn close(&self) -> Result<()> {
        let mut shared = self.lock();
        shared.closed = true;
        for queue in shared.queues.values_mut() {
            for semaphore in queue.consumers.drain(..) {
                // Wakes consumers waiting for a free prefetch slot.
                semaphore.close();
            }
            // Wakes consumers waiting for a message.
            queue.wake();
        }
        Ok(())
    }
}

/// Drops a consumer's semaphore from its queue when the consumer task ends, however
/// it ends, so `QueueState::consumers` never grows with dead consumers.
struct ConsumerGuard {
    state: Arc<Mutex<Shared>>,
    queue: String,
    semaphore: Arc<Semaphore>,
}

impl Drop for ConsumerGuard {
    fn drop(&mut self) {
        let mut shared = MemoryBackend::lock_shared(&self.state);
        if let Some(queue) = shared.queues.get_mut(&self.queue) {
            queue.consumers.retain(|s| !Arc::ptr_eq(s, &self.semaphore));
        }
    }
}

/// Pulls messages for one consumer, respecting its prefetch window.
async fn consumer_loop(
    state: Arc<Mutex<Shared>>,
    queue: String,
    semaphore: Arc<Semaphore>,
    tx: mpsc::UnboundedSender<Result<Box<dyn Delivery>>>,
    mut signal: watch::Receiver<u64>,
) {
    let _guard = ConsumerGuard {
        state: state.clone(),
        queue: queue.clone(),
        semaphore: semaphore.clone(),
    };

    loop {
        // Blocks while `prefetch` deliveries are outstanding; errors once closed.
        // A dropped stream ends the task right away, so its semaphore does not linger.
        let permit = tokio::select! {
            biased;
            () = tx.closed() => return,
            permit = semaphore.clone().acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => return,
            },
        };

        let envelope = loop {
            let next = {
                let mut shared = MemoryBackend::lock_shared(&state);
                if shared.closed {
                    return;
                }
                shared
                    .queues
                    .get_mut(&queue)
                    .and_then(|q| q.pending.pop_front())
            };
            match next {
                Some(envelope) => break envelope,
                None => {
                    // `changed()` resolves immediately if a message arrived since the
                    // last check, so no wakeup can be lost here.
                    tokio::select! {
                        biased;
                        () = tx.closed() => return,
                        changed = signal.changed() => if changed.is_err() {
                            return;
                        },
                    }
                }
            }
        };

        let delivery = MemoryDelivery {
            state: state.clone(),
            envelope: envelope.clone(),
            _permit: permit,
        };
        if tx.send(Ok(Box::new(delivery))).is_err() {
            // Consumer dropped the stream: put the message back for someone else.
            let mut shared = MemoryBackend::lock_shared(&state);
            if let Some(q) = shared.queues.get_mut(&queue) {
                q.pending.push_front(envelope);
                q.wake();
            }
            return;
        }
    }
}

/// A message handed to one consumer. Holds a prefetch permit until it is settled.
struct MemoryDelivery {
    state: Arc<Mutex<Shared>>,
    envelope: Envelope,
    _permit: OwnedSemaphorePermit,
}

impl MemoryDelivery {
    /// Record this attempt as settled by an ack, and why: the `kind` is what tells a
    /// test a success apart from the ack that a retry or deferral performs.
    fn record_ack(&self, kind: AckKind) {
        let mut shared = MemoryBackend::lock_shared(&self.state);
        shared
            .queue_mut(&self.envelope.queue)
            .settled
            .push((self.envelope.clone(), kind));
    }
}

#[async_trait]
impl Delivery for MemoryDelivery {
    fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    async fn ack(self: Box<Self>) -> Result<()> {
        self.record_ack(AckKind::Succeeded);
        Ok(())
    }

    async fn dead_letter(self: Box<Self>, reason: &str) -> Result<()> {
        let mut shared = MemoryBackend::lock_shared(&self.state);
        if shared.closed {
            return Err(Error::ShutDown);
        }
        shared
            .queue_mut(&self.envelope.queue)
            .dead
            .push((self.envelope.clone(), reason.to_owned()));
        Ok(())
    }

    async fn retry(self: Box<Self>, next: Envelope, delay: Duration) -> Result<()> {
        // A retry is a publish, so a closed backend rejects it and the original stays
        // unacked, just like `publish`.
        if MemoryBackend::is_state_closed(&self.state) {
            return Err(Error::ShutDown);
        }
        // Schedule first, ack second: never lose the message.
        MemoryBackend::schedule(&self.state, next, delay);
        self.record_ack(AckKind::Retried);
        Ok(())
    }

    async fn defer(self: Box<Self>, next: Envelope, delay: Duration) -> Result<()> {
        // A deferral is a publish too, so a closed backend rejects it and the original
        // stays unacked.
        if MemoryBackend::is_state_closed(&self.state) {
            return Err(Error::ShutDown);
        }
        // Hold first, ack second: never lose the message. Dropping `self` afterwards
        // releases the prefetch permit, exactly as `retry` does.
        MemoryBackend::hold(&self.state, next, delay);
        self.record_ack(AckKind::Deferred);
        Ok(())
    }
}

/// Adapts the consumer channel to the [`Stream`] the [`Backend`] trait returns.
struct DeliveryReceiver {
    rx: mpsc::UnboundedReceiver<Result<Box<dyn Delivery>>>,
    state: Arc<Mutex<Shared>>,
    queue: String,
}

impl Stream for DeliveryReceiver {
    type Item = Result<Box<dyn Delivery>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().rx.poll_recv(cx)
    }
}

impl Drop for DeliveryReceiver {
    /// Requeues everything that was produced for this consumer but never handed to
    /// it, the way a broker returns unacked messages when a channel closes. Without
    /// this, dropping a stream would silently swallow the messages in flight.
    fn drop(&mut self) {
        self.rx.close();
        let mut unread = Vec::new();
        while let Ok(item) = self.rx.try_recv() {
            if let Ok(delivery) = item {
                unread.push(delivery.envelope().clone());
            }
        }
        if unread.is_empty() {
            return;
        }
        let mut shared = MemoryBackend::lock_shared(&self.state);
        if shared.closed {
            return;
        }
        let queue = shared.queue_mut(&self.queue);
        // Front, in reverse, so the original order survives.
        for envelope in unread.into_iter().rev() {
            queue.pending.push_front(envelope);
        }
        queue.wake();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        job::Job,
        queue::QueueSet,
        test_support::{Greet, Ping, TestQueues},
    };
    use futures::{StreamExt, future::poll_immediate};

    fn alpha() -> QueueConfig {
        TestQueues::Alpha.config()
    }

    async fn declared() -> MemoryBackend {
        let backend = MemoryBackend::new();
        backend.declare(&[alpha()]).await.unwrap();
        backend
    }

    async fn publish(backend: &MemoryBackend, name: &str) -> Envelope {
        let envelope = Envelope::new(&Greet::new(name)).unwrap();
        backend.publish(&envelope, None).await.unwrap();
        envelope
    }

    async fn next_delivery(stream: &mut DeliveryStream) -> Box<dyn Delivery> {
        stream
            .next()
            .await
            .expect("stream ended")
            .expect("delivery error")
    }

    #[tokio::test]
    async fn declare_is_idempotent() {
        let backend = declared().await;
        publish(&backend, "a").await;

        backend.declare(&[alpha(), alpha()]).await.unwrap();
        backend.declare(&[alpha()]).await.unwrap();

        assert_eq!(backend.pending("test.alpha"), 1);
        assert_eq!(backend.queue_names(), vec!["test.alpha".to_owned()]);
        assert_eq!(backend.queue_config("test.alpha").unwrap().prefetch, 4);
    }

    #[tokio::test]
    async fn declares_every_queue_of_a_queue_set() {
        let backend = MemoryBackend::new();
        let configs: Vec<_> = TestQueues::all().iter().map(|q| q.config()).collect();
        backend.declare(&configs).await.unwrap();
        assert_eq!(
            backend.queue_names(),
            vec![
                "test.alpha".to_owned(),
                "test.beta".to_owned(),
                "test.gamma".to_owned()
            ]
        );
    }

    #[tokio::test]
    async fn publish_then_consume_is_fifo() {
        let backend = declared().await;
        for name in ["a", "b", "c"] {
            publish(&backend, name).await;
        }
        assert_eq!(backend.pending("test.alpha"), 3);

        let mut stream = backend.consume(&alpha()).await.unwrap();
        for name in ["a", "b", "c"] {
            let delivery = next_delivery(&mut stream).await;
            assert_eq!(
                delivery.envelope().decode::<Greet>().unwrap(),
                Greet::new(name)
            );
            assert_eq!(delivery.envelope().attempt, 1);
            delivery.ack().await.unwrap();
        }

        assert_eq!(backend.pending("test.alpha"), 0);
        let acked: Vec<_> = backend
            .acked("test.alpha")
            .iter()
            .map(|e| e.decode::<Greet>().unwrap().name)
            .collect();
        assert_eq!(acked, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn consumer_started_before_publish_receives_messages() {
        let backend = declared().await;
        let mut stream = backend.consume(&alpha()).await.unwrap();
        // Let the consumer task park on the empty queue.
        tokio::task::yield_now().await;

        publish(&backend, "late").await;
        let delivery = next_delivery(&mut stream).await;
        assert_eq!(
            delivery.envelope().decode::<Greet>().unwrap(),
            Greet::new("late")
        );
        delivery.ack().await.unwrap();
    }

    #[tokio::test]
    async fn prefetch_caps_outstanding_deliveries() {
        let backend = declared().await;
        let config = QueueConfig::new("test.alpha").prefetch(2);
        for name in ["a", "b", "c"] {
            publish(&backend, name).await;
        }

        let mut stream = backend.consume(&config).await.unwrap();
        let first = next_delivery(&mut stream).await;
        let second = next_delivery(&mut stream).await;
        assert_eq!(first.envelope().decode::<Greet>().unwrap(), Greet::new("a"));
        assert_eq!(
            second.envelope().decode::<Greet>().unwrap(),
            Greet::new("b")
        );

        // Third delivery is withheld: two are outstanding.
        assert!(poll_immediate(stream.next()).await.is_none());
        assert_eq!(backend.pending("test.alpha"), 1);

        first.ack().await.unwrap();
        let third = next_delivery(&mut stream).await;
        assert_eq!(third.envelope().decode::<Greet>().unwrap(), Greet::new("c"));
        assert_eq!(backend.pending("test.alpha"), 0);

        second.ack().await.unwrap();
        third.ack().await.unwrap();
        assert_eq!(backend.acked("test.alpha").len(), 3);
    }

    #[tokio::test]
    async fn zero_prefetch_means_unlimited() {
        let backend = declared().await;
        for name in ["a", "b", "c"] {
            publish(&backend, name).await;
        }
        let mut stream = backend
            .consume(&QueueConfig::new("test.alpha").prefetch(0))
            .await
            .unwrap();
        let mut held = Vec::new();
        for _ in 0..3 {
            held.push(next_delivery(&mut stream).await);
        }
        assert_eq!(held.len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_publish_is_invisible_until_the_delay_elapses() {
        let backend = declared().await;
        let envelope = Envelope::new(&Greet::new("later")).unwrap();
        backend
            .publish(&envelope, Some(Duration::from_secs(30)))
            .await
            .unwrap();

        // Sleeping (rather than `advance`) lets the paused clock actually fire the
        // scheduled timer, because the runtime parks in between.
        tokio::time::sleep(Duration::from_secs(29)).await;
        assert_eq!(backend.pending("test.alpha"), 0);

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(backend.pending("test.alpha"), 1);

        let mut stream = backend.consume(&alpha()).await.unwrap();
        let delivery = next_delivery(&mut stream).await;
        assert_eq!(delivery.envelope().job_id, envelope.job_id);
        delivery.ack().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn retry_redelivers_with_a_higher_attempt_after_the_delay() {
        let backend = declared().await;
        let original = publish(&backend, "flaky").await;
        let mut stream = backend.consume(&alpha()).await.unwrap();

        let delivery = next_delivery(&mut stream).await;
        let next = delivery.envelope().next_attempt();
        delivery.retry(next, Duration::from_secs(10)).await.unwrap();

        // The original is acked straight away, the copy is still in flight.
        assert_eq!(backend.acked("test.alpha").len(), 1);
        assert_eq!(backend.pending("test.alpha"), 0);
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(poll_immediate(stream.next()).await.is_none());

        tokio::time::advance(Duration::from_secs(6)).await;
        let redelivered = next_delivery(&mut stream).await;
        assert_eq!(redelivered.envelope().attempt, 2);
        assert_eq!(redelivered.envelope().job_id, original.job_id);
        redelivered.ack().await.unwrap();
        assert_eq!(backend.acked("test.alpha").len(), 2);
    }

    #[tokio::test]
    async fn retry_without_delay_is_immediate() {
        let backend = declared().await;
        publish(&backend, "now").await;
        let mut stream = backend.consume(&alpha()).await.unwrap();

        let delivery = next_delivery(&mut stream).await;
        let next = delivery.envelope().next_attempt();
        delivery.retry(next, Duration::ZERO).await.unwrap();

        let redelivered = next_delivery(&mut stream).await;
        assert_eq!(redelivered.envelope().attempt, 2);
        redelivered.ack().await.unwrap();
    }

    /// Publishes `name` with an explicit priority, bypassing `Envelope::new`'s zero.
    async fn publish_with_priority(backend: &MemoryBackend, name: &str, priority: u8) -> Envelope {
        let mut envelope = Envelope::new(&Greet::new(name)).unwrap();
        envelope.priority = priority;
        backend.publish(&envelope, None).await.unwrap();
        envelope
    }

    /// The names still waiting on `queue`, in the order a consumer would see them.
    fn pending_names(backend: &MemoryBackend, queue: &str) -> Vec<String> {
        backend
            .lock()
            .queues
            .get(queue)
            .map(|q| {
                q.pending
                    .iter()
                    .map(|e| e.decode::<Greet>().unwrap().name)
                    .collect()
            })
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn publish_orders_by_priority_and_keeps_each_level_fifo() {
        let backend = declared().await;
        for (name, priority) in [
            ("normal-1", 0),
            ("urgent-1", 10),
            ("normal-2", 0),
            ("urgent-2", 10),
            ("middle", 5),
            ("normal-3", 0),
        ] {
            publish_with_priority(&backend, name, priority).await;
        }

        assert_eq!(
            pending_names(&backend, "test.alpha"),
            vec![
                "urgent-1", "urgent-2", // 10, in publish order
                "middle",   // 5
                "normal-1", "normal-2", "normal-3", // 0, in publish order
            ]
        );
    }

    #[tokio::test]
    async fn a_higher_priority_publish_overtakes_the_whole_backlog() {
        let backend = declared().await;
        for name in ["a", "b", "c"] {
            publish(&backend, name).await;
        }
        publish_with_priority(&backend, "jumper", 1).await;

        let mut stream = backend.consume(&alpha()).await.unwrap();
        let first = next_delivery(&mut stream).await;
        assert_eq!(
            first.envelope().decode::<Greet>().unwrap(),
            Greet::new("jumper")
        );
        first.ack().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn defer_holds_the_message_and_then_lands_it_ahead_of_the_backlog() {
        let backend = declared().await;
        for name in ["backlog-1", "backlog-2"] {
            publish(&backend, name).await;
        }
        let held = Envelope::new(&Greet::new("held")).unwrap().deferred(10);
        backend.defer(&held, Duration::from_secs(30)).await.unwrap();

        // Invisible while it waits: not pending, but accounted for.
        assert_eq!(backend.pending("test.alpha"), 2);
        assert_eq!(backend.deferred("test.alpha"), 1);
        tokio::time::sleep(Duration::from_secs(29)).await;
        assert_eq!(backend.pending("test.alpha"), 2);
        assert_eq!(backend.deferred("test.alpha"), 1);

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(backend.deferred("test.alpha"), 0, "the hold has expired");
        assert_eq!(
            pending_names(&backend, "test.alpha"),
            vec!["held", "backlog-1", "backlog-2"],
            "a deferred job comes back in front"
        );

        let mut stream = backend.consume(&alpha()).await.unwrap();
        let first = next_delivery(&mut stream).await;
        assert_eq!(first.envelope().job_id, held.job_id);
        assert_eq!(first.envelope().deferrals, 1);
        assert_eq!(first.envelope().priority, 10);
        assert_eq!(first.envelope().attempt, 1, "a deferral is not an attempt");
        first.ack().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn deferred_counts_every_message_in_hold() {
        let backend = declared().await;
        assert_eq!(backend.deferred("test.alpha"), 0);
        assert_eq!(backend.deferred("never.declared"), 0);

        for _ in 0..3 {
            backend
                .defer(
                    &Envelope::new(&Greet::new("held")).unwrap(),
                    Duration::from_secs(5),
                )
                .await
                .unwrap();
        }
        assert_eq!(backend.deferred("test.alpha"), 3);

        tokio::time::sleep(Duration::from_secs(6)).await;
        assert_eq!(backend.deferred("test.alpha"), 0);
        assert_eq!(backend.pending("test.alpha"), 3);
    }

    /// `held + pending` for `queue`, read under one lock so the pair is a consistent
    /// snapshot rather than two observations with a gap in between.
    fn held_plus_pending(backend: &MemoryBackend, queue: &str) -> usize {
        backend
            .lock()
            .queues
            .get(queue)
            .map_or(0, |q| q.held + q.pending.len())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_hold_is_never_counted_twice_while_it_expires() {
        const HELD: usize = 64;

        let backend = declared().await;
        for i in 0..HELD {
            backend
                .defer(
                    &Envelope::new(&Greet::new(&i.to_string())).unwrap(),
                    Duration::from_millis(5),
                )
                .await
                .unwrap();
        }

        // Real time, real threads: the holds expire while this loop watches. Each
        // envelope must be either in hold or pending, never both, so the sum can only
        // ever be `HELD`.
        for _ in 0..1_000_000 {
            let total = held_plus_pending(&backend, "test.alpha");
            assert!(
                total <= HELD,
                "an envelope was counted as held and pending at once ({total} > {HELD})"
            );
            if backend.pending("test.alpha") == HELD {
                break;
            }
            tokio::task::yield_now().await;
        }

        assert_eq!(backend.pending("test.alpha"), HELD);
        assert_eq!(backend.deferred("test.alpha"), 0);
    }

    #[tokio::test]
    async fn a_zero_delay_deferral_never_enters_the_hold() {
        let backend = declared().await;
        backend
            .defer(&Envelope::new(&Greet::new("now")).unwrap(), Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(backend.deferred("test.alpha"), 0);
        assert_eq!(backend.pending("test.alpha"), 1);
    }

    #[tokio::test]
    async fn defer_after_close_reports_the_shutdown() {
        let backend = declared().await;
        backend.close().await.unwrap();

        assert!(matches!(
            backend
                .defer(
                    &Envelope::new(&Greet::new("x")).unwrap(),
                    Duration::from_secs(1)
                )
                .await,
            Err(Error::ShutDown)
        ));
        assert_eq!(backend.deferred("test.alpha"), 0);
        assert_eq!(backend.pending("test.alpha"), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_deferral_that_lands_after_close_is_dropped() {
        let backend = declared().await;
        backend
            .defer(
                &Envelope::new(&Greet::new("held")).unwrap(),
                Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert_eq!(backend.deferred("test.alpha"), 1);
        backend.close().await.unwrap();

        tokio::time::sleep(Duration::from_secs(31)).await;
        assert_eq!(backend.pending("test.alpha"), 0);
        assert_eq!(backend.deferred("test.alpha"), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn delivery_defer_acks_the_original_and_reschedules_it() {
        let backend = declared().await;
        let original = publish(&backend, "rate-limited").await;
        let mut stream = backend.consume(&alpha()).await.unwrap();

        let delivery = next_delivery(&mut stream).await;
        let next = delivery.envelope().deferred(10);
        delivery.defer(next, Duration::from_secs(30)).await.unwrap();

        // The original is acked straight away; the copy is in hold.
        assert_eq!(backend.acked("test.alpha").len(), 1);
        assert_eq!(backend.acked("test.alpha")[0].job_id, original.job_id);
        assert_eq!(backend.acked("test.alpha")[0].deferrals, 0);
        assert_eq!(backend.pending("test.alpha"), 0);
        assert_eq!(backend.deferred("test.alpha"), 1);
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(poll_immediate(stream.next()).await.is_none());

        tokio::time::advance(Duration::from_secs(21)).await;
        let redelivered = next_delivery(&mut stream).await;
        assert_eq!(redelivered.envelope().job_id, original.job_id);
        assert_eq!(redelivered.envelope().attempt, 1);
        assert_eq!(redelivered.envelope().deferrals, 1);
        assert_eq!(redelivered.envelope().priority, 10);
        assert_eq!(backend.deferred("test.alpha"), 0);
        redelivered.ack().await.unwrap();
        assert_eq!(backend.acked("test.alpha").len(), 2);
    }

    #[tokio::test]
    async fn delivery_defer_frees_a_prefetch_slot() {
        let backend = declared().await;
        for name in ["a", "b"] {
            publish(&backend, name).await;
        }
        let mut stream = backend
            .consume(&QueueConfig::new("test.alpha").prefetch(1))
            .await
            .unwrap();

        let first = next_delivery(&mut stream).await;
        assert!(poll_immediate(stream.next()).await.is_none());
        let next = first.envelope().deferred(10);
        first.defer(next, Duration::from_secs(600)).await.unwrap();

        let second = next_delivery(&mut stream).await;
        assert_eq!(
            second.envelope().decode::<Greet>().unwrap(),
            Greet::new("b")
        );
        second.ack().await.unwrap();
    }

    #[tokio::test]
    async fn delivery_defer_after_close_reports_the_shutdown() {
        let backend = declared().await;
        publish(&backend, "a").await;
        let mut stream = backend.consume(&alpha()).await.unwrap();
        let delivery = next_delivery(&mut stream).await;

        backend.close().await.unwrap();

        let next = delivery.envelope().deferred(10);
        assert!(matches!(
            delivery.defer(next, Duration::from_secs(1)).await,
            Err(Error::ShutDown)
        ));
        // A refused deferral never acks the original, and nothing is held.
        assert!(backend.acked("test.alpha").is_empty());
        assert_eq!(backend.deferred("test.alpha"), 0);
    }

    #[tokio::test]
    async fn dead_letter_records_the_reason() {
        let backend = declared().await;
        let original = publish(&backend, "doomed").await;
        let mut stream = backend.consume(&alpha()).await.unwrap();

        let delivery = next_delivery(&mut stream).await;
        delivery
            .dead_letter("max attempts (3) exhausted")
            .await
            .unwrap();

        let dead = backend.dead_letters("test.alpha");
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].0.job_id, original.job_id);
        assert_eq!(dead[0].1, "max attempts (3) exhausted");
        assert!(backend.acked("test.alpha").is_empty());
        assert_eq!(backend.pending("test.alpha"), 0);
    }

    #[tokio::test]
    async fn dead_letter_frees_a_prefetch_slot() {
        let backend = declared().await;
        for name in ["a", "b"] {
            publish(&backend, name).await;
        }
        let mut stream = backend
            .consume(&QueueConfig::new("test.alpha").prefetch(1))
            .await
            .unwrap();

        let first = next_delivery(&mut stream).await;
        assert!(poll_immediate(stream.next()).await.is_none());
        first.dead_letter("nope").await.unwrap();

        let second = next_delivery(&mut stream).await;
        assert_eq!(
            second.envelope().decode::<Greet>().unwrap(),
            Greet::new("b")
        );
        second.ack().await.unwrap();
    }

    #[tokio::test]
    async fn close_ends_all_streams() {
        let backend = declared().await;
        let mut busy = backend.consume(&alpha()).await.unwrap();
        publish(&backend, "held").await;
        // Only one consumer exists at this point, so the message lands on `busy`.
        let held = next_delivery(&mut busy).await;
        let mut idle = backend.consume(&alpha()).await.unwrap();

        backend.close().await.unwrap();

        assert!(idle.next().await.is_none());
        // Even a consumer with an outstanding delivery is released.
        assert!(busy.next().await.is_none());
        assert!(backend.is_closed());
        // Settling after close still works and is recorded.
        held.ack().await.unwrap();
        assert_eq!(backend.acked("test.alpha").len(), 1);

        assert!(matches!(
            backend
                .publish(&Envelope::new(&Greet::new("x")).unwrap(), None)
                .await,
            Err(Error::ShutDown)
        ));
        assert!(matches!(
            backend.declare(&[alpha()]).await,
            Err(Error::ShutDown)
        ));
    }

    #[tokio::test]
    async fn retry_and_dead_letter_after_close_report_the_shutdown() {
        let backend = declared().await;
        publish(&backend, "a").await;
        publish(&backend, "b").await;
        let mut stream = backend.consume(&alpha()).await.unwrap();
        let first = next_delivery(&mut stream).await;
        let second = next_delivery(&mut stream).await;

        backend.close().await.unwrap();

        let next = first.envelope().next_attempt();
        assert!(matches!(
            first.retry(next, Duration::ZERO).await,
            Err(Error::ShutDown)
        ));
        assert!(matches!(
            second.dead_letter("too late").await,
            Err(Error::ShutDown)
        ));
        // A refused retry never acks the original, and nothing is rescheduled.
        assert!(backend.acked("test.alpha").is_empty());
        assert!(backend.dead_letters("test.alpha").is_empty());
        assert_eq!(backend.pending("test.alpha"), 0);
    }

    #[tokio::test]
    async fn a_delayed_publish_that_lands_after_close_is_dropped() {
        let backend = declared().await;
        backend
            .publish(
                &Envelope::new(&Greet::new("later")).unwrap(),
                Some(Duration::from_millis(1)),
            )
            .await
            .unwrap();
        backend.close().await.unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(backend.pending("test.alpha"), 0);
    }

    #[tokio::test]
    async fn consumers_are_forgotten_when_their_streams_are_dropped() {
        let backend = declared().await;
        let streams: Vec<_> = {
            let mut v = Vec::new();
            for _ in 0..3 {
                v.push(backend.consume(&alpha()).await.unwrap());
            }
            v
        };
        assert_eq!(backend.consumer_count("test.alpha"), 3);

        drop(streams);
        // Each consumer task notices the closed channel and deregisters itself.
        for _ in 0..1_000 {
            if backend.consumer_count("test.alpha") == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(backend.consumer_count("test.alpha"), 0);
    }

    #[tokio::test]
    async fn consuming_after_close_yields_an_ended_stream() {
        let backend = declared().await;
        backend.close().await.unwrap();
        let mut stream = backend.consume(&alpha()).await.unwrap();
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn multiple_consumers_share_one_queue() {
        let backend = declared().await;
        let config = QueueConfig::new("test.alpha").prefetch(1);
        let mut left = backend.consume(&config).await.unwrap();
        let mut right = backend.consume(&config).await.unwrap();
        tokio::task::yield_now().await;

        for name in ["a", "b"] {
            publish(&backend, name).await;
        }

        let one = next_delivery(&mut left).await;
        let two = next_delivery(&mut right).await;
        let names = [
            one.envelope().decode::<Greet>().unwrap().name,
            two.envelope().decode::<Greet>().unwrap().name,
        ];
        assert!(
            names.contains(&"a".to_owned()) && names.contains(&"b".to_owned()),
            "{names:?}"
        );

        one.ack().await.unwrap();
        two.ack().await.unwrap();
        assert_eq!(backend.acked("test.alpha").len(), 2);
    }

    #[tokio::test]
    async fn dropping_a_stream_returns_undelivered_messages_to_the_queue() {
        let backend = declared().await;
        let stream = backend
            .consume(&QueueConfig::new("test.alpha").prefetch(2))
            .await
            .unwrap();
        publish(&backend, "orphaned").await;
        publish(&backend, "also-orphaned").await;

        // Wait until the consumer has taken both messages off the queue but nobody
        // has pulled them off the stream: that is the state the requeue has to cover.
        for _ in 0..1_000 {
            if backend.pending("test.alpha") == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(backend.pending("test.alpha"), 0);

        // Dropping the stream hands them straight back, in their original order.
        drop(stream);
        assert_eq!(backend.pending("test.alpha"), 2);
        let mut stream = backend.consume(&alpha()).await.unwrap();
        for name in ["orphaned", "also-orphaned"] {
            let delivery = next_delivery(&mut stream).await;
            assert_eq!(
                delivery.envelope().decode::<Greet>().unwrap(),
                Greet::new(name)
            );
            delivery.ack().await.unwrap();
        }
    }

    #[tokio::test]
    async fn queues_are_independent() {
        let backend = MemoryBackend::new();
        let configs: Vec<_> = TestQueues::all().iter().map(|q| q.config()).collect();
        backend.declare(&configs).await.unwrap();

        backend
            .publish(&Envelope::new(&Greet::new("a")).unwrap(), None)
            .await
            .unwrap();
        backend
            .publish(&Envelope::new(&Ping { seq: 1 }).unwrap(), None)
            .await
            .unwrap();

        assert_eq!(backend.pending("test.alpha"), 1);
        assert_eq!(backend.pending("test.beta"), 1);

        let mut stream = backend.consume(&TestQueues::Beta.config()).await.unwrap();
        let delivery = next_delivery(&mut stream).await;
        assert_eq!(delivery.envelope().job_type, Ping::NAME);
        delivery.ack().await.unwrap();
        assert_eq!(backend.pending("test.alpha"), 1);
    }

    #[tokio::test]
    async fn clones_share_state() {
        let backend = declared().await;
        let clone = backend.clone();
        publish(&clone, "shared").await;
        assert_eq!(backend.pending("test.alpha"), 1);
        assert!(format!("{backend:?}").contains("test.alpha"));
    }

    #[tokio::test]
    async fn delivery_is_usable_behind_a_trait_object() {
        let backend = declared().await;
        publish(&backend, "boxed").await;
        let mut stream = backend.consume(&alpha()).await.unwrap();
        let delivery: Box<dyn Delivery> = next_delivery(&mut stream).await;
        let envelope = delivery.envelope().clone();
        delivery.ack().await.unwrap();
        assert_eq!(backend.acked("test.alpha"), vec![envelope]);
    }

    #[tokio::test]
    async fn a_plain_ack_is_both_acked_and_succeeded() {
        let backend = declared().await;
        let original = publish(&backend, "fine").await;
        let mut stream = backend.consume(&alpha()).await.unwrap();

        next_delivery(&mut stream).await.ack().await.unwrap();

        assert_eq!(backend.acked("test.alpha").len(), 1);
        let succeeded = backend.succeeded("test.alpha");
        assert_eq!(succeeded.len(), 1);
        assert_eq!(succeeded[0].job_id, original.job_id);
        assert!(backend.retried("test.alpha").is_empty());
        assert_eq!(
            backend.settled("test.alpha")[0].1,
            AckKind::Succeeded,
            "an ack from a handler that returned Ok"
        );
    }

    #[tokio::test]
    async fn a_retried_attempt_is_acked_but_never_succeeded() {
        let backend = declared().await;
        let original = publish(&backend, "flaky").await;
        let mut stream = backend.consume(&alpha()).await.unwrap();

        // Two failures, then the attempts run out and the job is dead-lettered: the
        // shape a "fails N times then gives up" test asserts on.
        for _ in 0..2 {
            let delivery = next_delivery(&mut stream).await;
            let next = delivery.envelope().next_attempt();
            delivery.retry(next, Duration::ZERO).await.unwrap();
        }
        next_delivery(&mut stream)
            .await
            .dead_letter("max attempts (3) exhausted")
            .await
            .unwrap();

        // Three attempts were made, but only the first two ended in an ack, and none of
        // them succeeded.
        assert_eq!(backend.acked("test.alpha").len(), 2);
        let retried = backend.retried("test.alpha");
        assert_eq!(retried.len(), 2);
        assert_eq!(
            retried.iter().map(|e| e.attempt).collect::<Vec<_>>(),
            [1, 2]
        );
        assert!(retried.iter().all(|e| e.job_id == original.job_id));
        assert!(
            backend.succeeded("test.alpha").is_empty(),
            "the job never succeeded, so `succeeded` must stay empty"
        );
        assert_eq!(backend.dead_letters("test.alpha").len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_deferred_attempt_is_acked_but_never_succeeded() {
        let backend = declared().await;
        let original = publish(&backend, "rate-limited").await;
        let mut stream = backend.consume(&alpha()).await.unwrap();

        let delivery = next_delivery(&mut stream).await;
        let next = delivery.envelope().deferred(10);
        delivery.defer(next, Duration::from_secs(30)).await.unwrap();

        assert_eq!(backend.acked("test.alpha").len(), 1);
        assert!(backend.succeeded("test.alpha").is_empty());
        assert!(backend.retried("test.alpha").is_empty());
        let settled = backend.settled("test.alpha");
        assert_eq!(settled[0].1, AckKind::Deferred);
        assert_eq!(settled[0].0.job_id, original.job_id);
        // The live hold count and the ack record are different things: the first drops
        // back to zero when the hold expires, the second does not.
        assert_eq!(backend.deferred("test.alpha"), 1);
        tokio::time::sleep(Duration::from_secs(31)).await;
        assert_eq!(backend.deferred("test.alpha"), 0);
        assert_eq!(backend.settled("test.alpha").len(), 1);
    }

    #[tokio::test]
    async fn settled_reports_every_ack_in_order_with_its_kind() {
        let backend = declared().await;
        publish(&backend, "eventually").await;
        let mut stream = backend.consume(&alpha()).await.unwrap();

        // Fail once, then ask to be deferred, then succeed. Zero delays so the copies
        // come back without any time to advance.
        let first = next_delivery(&mut stream).await;
        let next = first.envelope().next_attempt();
        first.retry(next, Duration::ZERO).await.unwrap();

        let second = next_delivery(&mut stream).await;
        let next = second.envelope().deferred(10);
        second.defer(next, Duration::ZERO).await.unwrap();

        next_delivery(&mut stream).await.ack().await.unwrap();

        let kinds: Vec<_> = backend
            .settled("test.alpha")
            .into_iter()
            .map(|(envelope, kind)| (envelope.attempt, kind))
            .collect();
        assert_eq!(
            kinds,
            vec![
                (1, AckKind::Retried),
                (2, AckKind::Deferred),
                (2, AckKind::Succeeded),
            ],
            "a deferral does not advance the attempt, a retry does"
        );
        assert_eq!(backend.acked("test.alpha").len(), 3);
        assert_eq!(backend.succeeded("test.alpha").len(), 1);
        assert_eq!(backend.retried("test.alpha").len(), 1);
    }

    #[tokio::test]
    async fn the_new_helpers_are_empty_for_an_unknown_queue() {
        let backend = MemoryBackend::new();
        assert!(backend.succeeded("never.declared").is_empty());
        assert!(backend.retried("never.declared").is_empty());
        assert!(backend.settled("never.declared").is_empty());
    }
}
