package gogo

import "context"

// WSHubMessage is the transport-neutral message shape used by WSHubAdapter.
type WSHubMessage struct {
	NodeID  string
	Topic   string
	Message []byte
	OpCode  OpCode
}

// WSHubAdapter bridges WSHub messages across processes or hosts.
//
// Implementations call deliver for every message received from outside the
// hub. The hub ignores messages carrying its own NodeID, so adapters can use
// brokers such as Redis Pub/Sub that echo a process's publish back to all
// subscribers, including itself.
//
// Start must be safe to call more than once. After it has successfully started
// receiving, later Start calls should return nil without creating duplicate
// receive loops. When Start returns an error, the hub treats the adapter as not
// started and may retry. The deliver callback is safe for adapters to call from
// adapter-owned goroutines, including concurrently; any ordering guarantee is
// provided by the adapter and broker.
//
// Publish should respect ctx and return when the message has been accepted by
// the adapter or broker, or when delivery cannot be attempted. The hub does not
// retry failed Publish calls after they leave its queue. Close must be
// idempotent and should stop receive loops, unblock context-aware operations,
// and release adapter-owned resources.
type WSHubAdapter interface {
	Start(ctx context.Context, deliver func(WSHubMessage)) error
	Publish(ctx context.Context, msg WSHubMessage) error
	Close() error
}

// WSHubTopicAdapter is an optional extension for adapters that can subscribe
// only to topics with local subscribers. WSHub calls these methods
// asynchronously from its adapter worker when the first local socket subscribes
// to a topic and when the last local socket leaves.
//
// Topic operations are reconciled to the latest local state, not replayed as a
// subscribe/unsubscribe event log. The hub retries failed topic operations
// until they succeed or the hub closes.
type WSHubTopicAdapter interface {
	WSHubAdapter
	Subscribe(ctx context.Context, topic string) error
	Unsubscribe(ctx context.Context, topic string) error
}
