# queuey-rabbitmq

RabbitMQ backend for [`queuey`](../..), built on [`lapin`](https://crates.io/crates/lapin) 4.x.

`RabbitMqBackend` implements `queuey_core::Backend`:

* one `lapin::Connection`;
* one publishing channel in confirm mode, shared behind a `tokio::sync::Mutex`.
  Every publish (enqueue, retry, defer, dead-letter) waits for the broker's
  confirmation. Nothing is ever *declared* on it;
* one channel for the hold queue declarations every retry, delayed enqueue and
  `defer` makes on demand. A declaration is the one thing the broker routinely
  refuses (`PRECONDITION_FAILED` closes the channel it ran on), so it is kept
  away from the publishes it would otherwise take down with it;
* one fresh channel per `consume` call, with `basic_qos(prefetch, global = false)`;
* one throwaway channel per `declare`, so a rejected declaration cannot poison
  the other channels.

## Reconnection

The connection above is a slot, not a socket. When it drops, the first operation to
notice dials a replacement, everything else queues behind that one attempt, every queue
this backend declared is re-declared on the new connection, and the consumer streams
resubscribe and keep yielding. A publish issued during the outage waits instead of
failing, and `Worker::run` keeps running across a broker restart.

By default it retries forever with a jittered exponential backoff (500ms base, doubling,
capped at 30s), so a broker that never comes back shows up as a stalled worker and a
stream of `WARN` logs rather than an error. Bound it, or turn it off:

```rust
use queuey_rabbitmq::{BackoffPolicy, RabbitMqOptions};

// Give up after ten tries; consumer streams then end and `Worker::run` returns.
let bounded = RabbitMqOptions::default()
    .reconnect_with(BackoffPolicy::default().max_attempts(Some(10)));

// Or fail on the first drop, as this backend did before 0.3.
let never = RabbitMqOptions::default().reconnect(None);
```

`BackoffPolicy` is only the built-in answer. `reconnect_with` takes any `ReconnectPolicy`,
which is one method: given an `Attempt`, return how long to wait or `None` to stop. The
attempt carries the consecutive failure count, the last error, and whether the backend is
rebuilding the connection or a consumer's subscription — so a policy can express what a
curve cannot, such as a circuit breaker, a maintenance window, giving up immediately on
`ACCESS_REFUSED`, or retrying the connection forever while abandoning a consumer whose
queue an operator deleted.

```rust
use std::time::Duration;
use queuey_rabbitmq::{Attempt, RabbitMqOptions, Rebuilding, ReconnectPolicy};

#[derive(Debug)]
struct ConnectionOnly;

impl ReconnectPolicy for ConnectionOnly {
    fn next_delay(&self, attempt: Attempt<'_>) -> Option<Duration> {
        match attempt.rebuilding {
            Rebuilding::Consumer if attempt.failures >= 3 => None,
            _ => Some(Duration::from_secs(2)),
        }
    }
}

let options = RabbitMqOptions::default().reconnect_with(ConnectionOnly);
```

The policy is consulted before *every* attempt, the first one included, so it also decides
whether to start at all (`None` straight away means never reconnect) and whether the first
try waits. `BackoffPolicy` returns zero there, because a failover is often complete by the
time the client notices.

Jobs in flight when the connection drops do **not** survive it. The broker requeues every
unacknowledged delivery, so a job whose handler was still running is delivered again on
the new connection, and the settle its first run eventually attempts fails and is counted
in `WorkerHandle::settle_failures`. That is the at-least-once contract this backend
already had; an outage is when it stops being theoretical.

Only the *first* connection is exempt: `connect` / `with_options` fail rather than retry,
so a process that cannot reach its broker at startup says so instead of hanging.

## Topology

For each logical queue `q`:

| queue | role | arguments |
|---|---|---|
| `q` | main work queue | `x-message-ttl` when `QueueConfig::message_ttl` is set, `x-max-priority` when `QueueConfig::max_priority` is `Some` |
| `q.dead` | dead-letter queue | none |
| `q.deferred.{ttl_ms}` | hold queue, one per distinct delay | `x-message-ttl = ttl_ms`, `x-dead-letter-exchange = ""`, `x-dead-letter-routing-key = q`, `x-expires = 2 * ttl_ms` |

Every wait goes through a hold queue: a retry backoff, an `enqueue_after` delay
and a deferral alike. There is no shared wait queue and no per-message
`expiration`, because RabbitMQ only expires the message at the head of a classic
queue: in a shared wait queue a five-minute retry at the head would hold back
every one-second retry behind it, and exponential backoff produces exactly that
mix. In a hold queue every message has the same TTL, so the queue drains in
publish order and a short wait is never stuck behind a long one.

Dead-lettered envelopes are published to `q.dead` with headers `x-death-reason`,
`x-original-queue` and `x-attempts`. Bodies that do not decode as an `Envelope`
are copied verbatim to `q.dead` with `x-death-reason = "malformed envelope"` and
then acked, so one poison message cannot stall a consumer.

`q` and `q.dead` are created by `declare`. Hold queues are not: their names
depend on the delays jobs actually ask for, so they are created on demand right
before each publish into them and the broker deletes them again once idle.

The dead-letter suffix and whether `q.dead` is declared are configurable:

```rust
use queuey_rabbitmq::{RabbitMqBackend, RabbitMqOptions};

let backend = RabbitMqBackend::with_options(
    "amqp://guest:guest@localhost:5672/%2f",
    RabbitMqOptions::default()
        .dead_suffix("-dlq")
        .declare_dead_letter_queues(true),
)
.await?;
```

## Hold queues

A wait is published to `q.deferred.{ttl_ms}`, whose only job is to dead-letter
its contents back onto `q` after `ttl_ms`. The delay is the queue's
`x-message-ttl`, never a per-message `expiration`, so every message in one hold
queue expires in publish order. The price is one queue per distinct delay, so
delays are rounded **up** to a granularity; they are never rounded down, so a
job is never released early.

There are two granularities, because the two kinds of wait have different
needs:

| option | applies to | default |
|---|---|---|
| `retry_granularity` | `Delivery::retry` (backoff) and `enqueue_after` | `1s` |
| `deferred_granularity` | `Delivery::defer` and `Producer::defer` | `1s` |

A backoff is a heuristic and tolerates coarse rounding; a `Retry-After` is a
contract. Exponential backoff with jitter produces a different delay on every
retry, so `retry_granularity` is the knob that bounds how many hold queues a
busy, failing queue can have at once: with a policy capped at five minutes,
`1s` allows up to 300, `10s` up to 30.

```rust
use std::time::Duration;
use queuey_rabbitmq::RabbitMqOptions;

let options = RabbitMqOptions::default()
    .deferred_suffix(".deferred")                            // default
    .retry_granularity(Duration::from_secs(10))              // default 1s
    .deferred_granularity(Duration::from_secs(1));           // default
```

A zero granularity is clamped to one millisecond rather than rejected, because
library code does not panic on configuration. It does mean up to one hold queue
per distinct millisecond, which is almost never what you want.

The hold queue is declared immediately before *every* publish into it and never
cached: an idle hold queue deletes itself one TTL after the last publish to it
(`x-expires = 2 * TTL`), and every declare resets that timer. So a delay still
in occasional use keeps its queue, and one that falls out of use is cleaned up
by the broker.

`x-expires` is deliberately **not** tunable. A hold queue's arguments are a pure
function of its name, so two processes running different builds compute identical
arguments for `q.deferred.30000`. Were the expiry a setting, a process with a
different value would be answered `PRECONDITION_FAILED` on every publish, for
ever, with no way out but deleting the queue. The granularities are safe to tune
because they only change *which* hold queue a delay lands in, never that queue's
arguments.

### Retry versus deferral

Both wait in the same hold queues; a retry and a deferral with the same rounded
delay share one. They differ in what happens once the job is back on `q`:

* A **retry** carries priority `0` and joins the back of the queue like any
  other message. Its attempt counter has been incremented.
* A **deferral** (`Backend::defer`, `Delivery::defer`) is what a
  `429 Too Many Requests` with `Retry-After: 30` calls for: the job did not
  fail, must not burn an attempt, and must come back **ahead of the backlog**.
  `q` carries `x-max-priority` from `QueueConfig::max_priority` (default
  `Some(10)`), every publish carries the envelope's `priority`, and a deferred
  envelope carries the queue's top level, so it is served before everything that
  piled up while it waited. "Ahead of the backlog" means ahead of what is still
  *on* the queue: a consumer with prefetch `N` already holds up to `N` backlog
  messages, and the returning deferral is first among what is still on the queue.

### What a hold requires

* **The queue must have been declared through this backend, in this process.**
  Otherwise the hold queue's durability and the queue it dead-letters back to
  would be guesses, and a TTL expiry into a queue that does not exist is
  discarded silently by the broker. Unlike a `mandatory` publish, nothing
  is returned and nothing is logged. Retrying, delaying or deferring onto an
  unknown queue is `Error::UnknownQueue` instead. `Producer::new` and
  `WorkerBuilder::build` declare the queue set; `Producer::new_undeclared`
  deliberately does not, so a producer built that way can `enqueue` but not
  `enqueue_after` or `defer`.
* **The delay must fit.** It is capped at `topology::MAX_DEFERRAL_MS`, about
  24.8 days. That is half of what a 32-bit millisecond TTL can express, because a hold
  queue's `x-expires` is twice its TTL. A longer delay is refused, not clamped:
  releasing a job early is the one thing a hold promises not to do. Rounding
  up to the granularity happens first, so a delay just under the cap can be
  refused too.
* **The queue name must leave room for its hold queues.** A 250-byte queue name
  is legal, but `{q}.deferred.2147483647` is not, so `declare` refuses such a
  name up front rather than letting retries fail one job at a time later.

All of these fail *before* anything is acked, so from `Delivery::retry` and
`Delivery::defer` the original message stays unacknowledged and the broker
redelivers it.

## Upgrading

**`q.retry` is gone.** Earlier versions declared a `q.retry` wait queue per work
queue and published retries into it with a per-message `expiration`. Retries
now wait in the same hold queues as deferrals, so `declare` no longer creates
`q.retry` and nothing publishes to it. No migration is needed: messages still
waiting in an existing `q.retry` expire back onto `q` on their own, because the
dead-letter routing is an argument of that queue, and workers on the old version
keep declaring it themselves, so a mixed fleet keeps working. Delete `q.retry`
once it is empty and no old worker is left. `RabbitMqOptions::retry_suffix` went
with it; `retry_granularity` is the retry tunable now. Two behavioural changes
come with the move: a delayed `enqueue_after` now needs the queue to have been
declared through the same backend (as `defer` always did), and a retry delay
past ~24.8 days is refused instead of being clamped.

**`x-max-priority` is a breaking topology change.** It is a *declaration*
argument, and RabbitMQ refuses to change the arguments of an existing queue: the
declaration comes back as `PRECONDITION_FAILED`, which closes the channel and
surfaces as an error from `declare`.

A `q` created before this feature has no `x-max-priority`, so declaring it again
with the default config **will fail**. Two options:

* drain and delete `q`, then let the backend redeclare it. Deferred jobs then
  come back ahead of the backlog; or
* set `max_priority = 0` (`QueueConfig::max_priority(0)`, or
  `#[queue(max_priority = 0)]`), which declares `q` exactly as before. Deferral
  still works; the returning job queues up FIFO with everything else.

`q.dead` and hold queues never carry `x-max-priority`, so only `q` is affected.

**`RabbitMqOptions::deferred_queue_grace` is gone.** A hold queue's `x-expires`
is now always `2 * TTL`, computed from the TTL in its name and nothing else. The
setting was unsafe by construction: two processes with different graces agreed on
the name `q.deferred.30000` but disagreed on its arguments, and the broker then
refused one of them with `PRECONDITION_FAILED` on every single deferral until
somebody deleted the queue. Drop the builder call; there is no replacement and no
broker-side migration. Existing hold queues expire on their own.

## Tests

Unit tests (topology naming, queue arguments including `x-max-priority` and the
hold queue's TTL / `x-expires` arithmetic, property and header mapping, option
defaults) need no broker and run with a plain:

```sh
cargo test -p queuey-rabbitmq
```

The integration tests in `tests/broker.rs` need a real RabbitMQ. They are not
`#[ignore]`d. Each one early-returns with a skip notice when `AMQP_URL` is
unset. To run them:

```sh
docker run --rm -p 5672:5672 rabbitmq:4-management
AMQP_URL=amqp://guest:guest@localhost:5672/%2f cargo test -p queuey-rabbitmq
```

Each test uses queue names carrying a fresh UUID and deletes them at the end
(hold queues included), so runs can share a broker.
