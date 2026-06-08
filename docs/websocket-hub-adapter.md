# WebSocket Hub Adapter Contract

This document defines the v0.5 contract between `WSHub` and implementations of
`WSHubAdapter`. It describes the behavior the hub already implements and the
rules custom adapters should follow.

## Scope

`WSHub` always performs process-local fan-out first. An adapter only carries the
same `WSHubMessage` to other processes or hosts. The hub filters messages whose
`NodeID` matches its own node, so adapters may use brokers that echo a process's
own publish back to the subscriber.

A successful `hub.Publish`, `hub.PublishBatch`, or `hub.PublishFrom` means:

- the process-local publish path was invoked
- the adapter message was queued, or no adapter is configured

It does not mean the broker durably stored the message or that remote sockets
received it.

## Adapter Lifecycle

`Start(ctx, deliver)` starts the adapter receive side. The hub calls it when the
application calls `hub.Start()`, and also lazily before the first adapter
publish or topic reconciliation.

Adapters must make `Start` idempotent. After a successful start, later calls
should return `nil` and must not create duplicate broker subscriptions or
receive loops. If `Start` returns an error, the hub treats the adapter as not
started and may retry. Explicit `hub.Start()` retries immediately. Lazy starts
from publish or topic work cache the last start error for a short backoff
window, beginning at 100 ms and growing to 2 seconds.

The `deliver` callback is safe to call from adapter-owned goroutines, including
concurrently. The hub publishes delivered messages to local apps unless the
message has the hub's own `NodeID` or the hub has been closed. Ordering of
delivered messages is the adapter and broker's responsibility.

`Close` must be idempotent. It should stop receive loops, unblock operations
that observe their contexts, and release resources owned by the adapter.

## Publish Path

The hub enqueues adapter publishes onto a bounded worker queue. If the queue is
full, the publish call returns `ErrWSHubAdapterQueueFull` after local fan-out
has already been invoked. `PublishBatch` keeps processing the batch and returns
the first queue error it sees.

Each worker calls `adapter.Publish(ctx, msg)` with a context derived from the
hub lifetime and `WithWSHubAdapterPublishTimeout`. `Publish` should return when
the adapter or broker has accepted the message, or when the operation cannot be
attempted before `ctx` is done.

The hub does not retry a failed `Publish` after the message leaves its queue.
The error is reported through `WithWSHubAdapterErrorHandler`, with rate limiting
to avoid error storms. Adapters may implement their own bounded retry inside
`Publish`, but they must respect `ctx`.

## Ordering

With the default `WithWSHubAdapterWorkers(1)`, queued messages reach
`adapter.Publish` in hub queue order. This is the only ordering guarantee the
hub adds around adapter publishes.

Raising the worker count allows multiple goroutines to call `adapter.Publish`
at the same time. That can increase broker throughput, but messages may be
published out of queue order. Cross-process delivery order also depends on the
adapter and broker.

`PublishBatch` queues messages in slice order. With one adapter worker, adapter
publish calls preserve that order relative to other queued hub publishes.

## Delivery Guarantees

The hub provides best-effort cross-process delivery. It does not provide
durable storage, acknowledgements, replay, deduplication, exactly-once
delivery, or at-least-once delivery.

If a process is disconnected, the adapter queue is full, a broker publish
fails, or a dynamic topic subscription has not reached the broker yet, remote
subscribers can miss messages. Broker-specific adapters can document stronger
or weaker guarantees, but they should not imply stronger guarantees for
`WSHub` itself.

## Cancellation And Timeouts

The context passed to `Start` is canceled when the hub closes. The contexts
passed to `Publish`, `Subscribe`, and `Unsubscribe` are also bounded by
`WithWSHubAdapterPublishTimeout`.

Adapters should check those contexts before blocking broker calls and should
return promptly when cancellation is observed. They should avoid starting
background work from `Publish`, `Subscribe`, or `Unsubscribe` that can outlive
the context without its own cancellation path.

## Slow Subscribers And Backpressure

`WSHub` does not track per-socket delivery acknowledgements or retry based on
slow WebSocket subscribers. Local fan-out hands the message to the attached
`App` publish path; per-socket buffering and backpressure remain the WebSocket
runtime's responsibility.

A slow adapter or broker blocks the adapter worker handling that operation
until the operation returns or its context expires. With one adapter worker this
causes head-of-line blocking for later adapter publishes. More workers can
reduce that coupling at the cost of publish ordering.

## Topic Adapter Extension

`WSHubTopicAdapter` is optional. It lets adapters subscribe the broker receive
side only to topics that currently have local WebSocket subscribers.

Topic operations are state reconciliation, not an event log. The hub tracks the
latest desired local state:

- at least one local socket in the topic means `Subscribe`
- no local sockets in the topic means `Unsubscribe`

Rapid join and leave churn is coalesced to the latest desired state. The hub
only marks a topic as applied after the adapter operation succeeds. If a topic
operation returns an error or panics, the hub reports the error, marks the topic
dirty again, and retries after 100 ms. Topic retries continue until the
operation succeeds or the hub closes.

The default topic worker count is one. When
`WithWSHubAdapterTopicWorkers(n)` is greater than one, different topics may be
reconciled in parallel, but the same topic is never reconciled concurrently.
The hub does not guarantee cross-topic ordering.

Before calling `Subscribe` or `Unsubscribe`, the hub ensures the adapter receive
side has started. Each topic operation receives a context bounded by
`WithWSHubAdapterPublishTimeout`.

When the hub closes, pending topic retry timers are stopped and dirty topic
state is discarded. The hub relies on `adapter.Close()` to tear down broker
subscriptions; it does not issue final `Unsubscribe` calls for every applied
topic.

## Close Semantics

`hub.Close()` is idempotent. It marks the hub closed, closes adapter work
queues, waits for adapter workers to drain queued publish work, cancels the hub
context if workers do not finish in time, waits again, and then calls
`adapter.Close()`.

`WithWSHubCloseTimeout` bounds each wait phase. If workers still do not finish
after cancellation, `hub.Close()` returns `ErrWSHubCloseTimeout`; if adapter
close also returns an error, the timeout remains the returned error. When the
workers drain successfully, any `adapter.Close()` error is returned.

`hub.Close()` does not close attached `App` instances or WebSocket connections.
After close, `hub.Start()` and publish calls return `ErrWSHubClosed`, and
incoming adapter deliveries are ignored.

## Redis Adapter Notes

The Redis adapter follows this contract with Redis Pub/Sub. Redis Pub/Sub is
best-effort realtime fan-out: messages published while a process is
disconnected are not replayed. With dynamic subscriptions enabled, broker
subscription updates are asynchronous and can race with remote publishes, so a
newly subscribed local topic can miss messages until the Redis subscription is
applied.
