# Dropwizard RabbitMQ Actors Bundle
Provides actor abstraction on RabbitMQ for dropwizard based projects.

## Dependency

```
<dependency>
    <groupId>io.appform.dropwizard.actors</groupId>
    <artifactId>dropwizard-rabbitmq-actors</artifactId>
    <version>2.0.28-15</version>
</dependency>
```

## Asynchronous Retries

Set `retryConfig.retryMode` to `ASYNC` to hand failed deliveries to broker-backed
waiting queues instead of sleeping between business attempts. `SYNC` is the default.
Supply a custom scheduler by overriding `RetryStrategyFactory.createScheduler(...)`
and custom strategies by overriding `RetryStrategyFactory.createStrategy(...)`.
Overrides of `RetryStrategyFactory.create(RetryConfig)` only apply to `SYNC`; creating
an `ASYNC` actor with such a factory fails instead of silently ignoring the override.

The default TTL scheduler keeps one durable waiting queue per delay. With
`retryConfig.asyncRetryQueueScope: EXCHANGE` (the default) it is shared by every consumer bound to
the same exchange with the same queue type, named `<exchange>_RETRY_<queueType>_<delayMs>`
(prefixed with the namespace when one is set). With `QUEUE` each consumed queue gets its own,
named `<queue>_RETRY_<delayMs>` and shared only by that queue's shards. Each retry is published
through a fanout exchange of the same name with the consumed queue as routing key, and the
waiting queue dead-letters back through the consumer's own exchange, which routes it to exactly
that queue; retries never cross exchanges. The original publish delay (`x-delay`) is dropped
from retry copies so a delayed exchange does not delay them again. Waiting queues carry only the
queue type, TTL and dead-letter arguments.

When a consumer starts, it declares every delay its retry policy can request. Startup fails
if the broker refuses that topology, if the policy needs more than
`retryConfig.asyncMaxDelayLevels` (default `25`) distinct delays, or if it would take the
process over its limit of distinct waiting queues. That limit is static and shared by all
actors in the process (default `32`); raise it with `TtlRequeueRetryScheduler.setMaxRetryQueues(...)`
before actors start. A waiting queue counts while any running consumer uses it. Delays outside
the plan, such as those from custom wait strategies, are declared on first use and count toward
both limits; once either is reached, new delays are permanently rejected and follow the
configured processing-error/drop/sideline policy. Durable queues and exchanges are not deleted
on shutdown, so restarting does not bound total broker topology across configuration changes.

`retryConfig.asyncConfirmTimeout` defaults to `5 seconds`. The original delivery is
acknowledged only after the retry handoff completes. A failed transport handoff
requeues it without advancing its persisted attempt count, and the delay's topology is
declared again on its next use. Broker refusals that retrying cannot fix
(`ACCESS_REFUSED`, `PRECONDITION_FAILED`) and invalid retry requests are rejected
permanently and follow the processing-error policy instead of being requeued.
Subsequent deliveries share a cooldown of 1 second, doubling after consecutive handoff
failures up to 30 seconds. It resets after a successful handoff, or when a failure comes
more than 30 seconds after the previous cooldown ended. Cooldown waits retain the delivery
unacknowledged, do not execute business handlers, and are woken on shutdown.
Messages may be delivered more than once; business handlers must be idempotent. If the
original delivery cannot be acknowledged after its retry copy is confirmed, for example
because its channel closed, the broker redelivers it too and both copies retry independently.

A message waiting in a retry queue is overtaken by later messages, so `ASYNC` does not
preserve queue or shard ordering; keep `SYNC` where ordering matters. Retried deliveries
arrive with `redelivered=false`; read the attempt number from the `x-actor-retry-attempt`
header in `MessageMetadata`.

RabbitMQ dead-letters messages from a waiting queue at most once (the only mode for classic
queues and the default for quorum queues), and the original delivery is already
acknowledged by then, so a broker failure at that moment can lose the message. The
application user needs permission to declare, bind and publish to the `*_RETRY_*` exchanges
and queues; dead-lettering back needs write permission on the actor's exchange, which
publishers already have. Each `ASYNC` consumer opens one extra confirm channel on the producer
connection, and retry publishes are reported to observers under the `<queue>_RETRY_<delayMs>`
queue name.

### Replaying Sidelined Messages

Retry state is retained in the `x-actor-retry-queue`, `x-actor-retry-attempt`, and
`x-actor-retry-started-at` headers. Republishing a sidelined delivery with those
headers to its original queue resumes the old attempt and elapsed-time budget.
To deliberately start a fresh retry cycle, the replay publisher must remove all
three headers from a copy of the message properties before republishing. Do not
clear them during ordinary retry handoffs. Application expiry headers are separate;
resetting retry state does not extend the original application expiry.

## License
Apache-2.0

# NOTE
Package and group id has changed from `io.dropwizard.actors` to `io.appfrom.dropwizard.actors` from 1.3.12-1.
