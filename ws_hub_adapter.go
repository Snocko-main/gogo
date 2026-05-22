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
type WSHubAdapter interface {
	Start(ctx context.Context, deliver func(WSHubMessage)) error
	Publish(ctx context.Context, msg WSHubMessage) error
	Close() error
}
