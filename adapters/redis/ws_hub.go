package redis

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"

	gogo "github.com/Snocko-main/gogo"
	goredis "github.com/redis/go-redis/v9"
)

const (
	defaultChannelPrefix = "gogo:ws:"
	wireVersion          = 1
)

// Options configures a Redis-backed gogo.WSHub adapter.
type Options struct {
	// URL is parsed with redis.ParseURL. Example:
	// redis://localhost:6379/0
	URL string

	// Addr/Username/Password/DB are used when URL is empty.
	Addr     string
	Username string
	Password string
	DB       int

	// ChannelPrefix is prepended to every topic. Defaults to "gogo:ws:".
	ChannelPrefix string
}

// Adapter bridges gogo.WSHub messages through Redis Pub/Sub.
//
// It uses one Redis client and one pattern subscription. Messages are encoded
// as a tiny binary frame, while the Redis channel carries the topic:
//
//	prefix + topic
//
// Redis Pub/Sub is best-effort. If a process is disconnected, messages
// published during that window are not replayed.
type Adapter struct {
	client goredis.UniversalClient
	own    bool
	prefix string

	mu     sync.Mutex
	pubsub *goredis.PubSub
	cancel context.CancelFunc
}

// New creates a Redis-backed WSHub adapter.
func New(opt Options) (*Adapter, error) {
	prefix := opt.ChannelPrefix
	if prefix == "" {
		prefix = defaultChannelPrefix
	}

	var client goredis.UniversalClient
	if opt.URL != "" {
		parsed, parseErr := goredis.ParseURL(opt.URL)
		if parseErr != nil {
			return nil, parseErr
		}
		client = goredis.NewClient(parsed)
	} else {
		addr := opt.Addr
		if addr == "" {
			addr = "localhost:6379"
		}
		client = goredis.NewClient(&goredis.Options{
			Addr:     addr,
			Username: opt.Username,
			Password: opt.Password,
			DB:       opt.DB,
		})
	}
	return &Adapter{
		client: client,
		own:    true,
		prefix: prefix,
	}, nil
}

// NewClient wraps an existing Redis client. The adapter does not close client
// on Close.
func NewClient(client goredis.UniversalClient, channelPrefix string) (*Adapter, error) {
	if client == nil {
		return nil, errors.New("gogo/adapters/redis: nil Redis client")
	}
	if channelPrefix == "" {
		channelPrefix = defaultChannelPrefix
	}
	return &Adapter{
		client: client,
		prefix: channelPrefix,
	}, nil
}

// Start subscribes to every topic under the configured prefix.
func (a *Adapter) Start(ctx context.Context, deliver func(gogo.WSHubMessage)) error {
	if a == nil || a.client == nil {
		return errors.New("gogo/adapters/redis: nil adapter")
	}
	if deliver == nil {
		return errors.New("gogo/adapters/redis: deliver callback is nil")
	}

	a.mu.Lock()
	if a.pubsub != nil {
		a.mu.Unlock()
		return nil
	}
	subCtx, cancel := context.WithCancel(ctx)
	pubsub := a.client.PSubscribe(subCtx, a.prefix+"*")
	if _, err := pubsub.Receive(subCtx); err != nil {
		cancel()
		a.mu.Unlock()
		return err
	}
	a.pubsub = pubsub
	a.cancel = cancel
	a.mu.Unlock()

	go func() {
		ch := pubsub.Channel()
		for {
			select {
			case <-subCtx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				topic, ok := strings.CutPrefix(msg.Channel, a.prefix)
				if !ok {
					continue
				}
				hubMsg, err := decodeMessage(topic, []byte(msg.Payload))
				if err != nil {
					continue
				}
				deliver(hubMsg)
			}
		}
	}()
	return nil
}

// Publish sends msg to Redis. The Redis channel name is prefix + topic.
func (a *Adapter) Publish(ctx context.Context, msg gogo.WSHubMessage) error {
	if a == nil || a.client == nil {
		return errors.New("gogo/adapters/redis: nil adapter")
	}
	payload, err := encodeMessage(msg)
	if err != nil {
		return err
	}
	return a.client.Publish(ctx, a.prefix+msg.Topic, payload).Err()
}

// Close stops the subscription and closes the owned Redis client.
func (a *Adapter) Close() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	cancel := a.cancel
	pubsub := a.pubsub
	a.cancel = nil
	a.pubsub = nil
	a.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	var err error
	if pubsub != nil {
		err = pubsub.Close()
	}
	if a.own && a.client != nil {
		if closeErr := a.client.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}

func encodeMessage(msg gogo.WSHubMessage) ([]byte, error) {
	if len(msg.NodeID) > 0xffff {
		return nil, fmt.Errorf("gogo/adapters/redis: WSHub node id too long: %d", len(msg.NodeID))
	}
	out := make([]byte, 4+len(msg.NodeID)+len(msg.Message))
	out[0] = wireVersion
	out[1] = byte(msg.OpCode)
	binary.BigEndian.PutUint16(out[2:4], uint16(len(msg.NodeID)))
	copy(out[4:], msg.NodeID)
	copy(out[4+len(msg.NodeID):], msg.Message)
	return out, nil
}

func decodeMessage(topic string, payload []byte) (gogo.WSHubMessage, error) {
	if len(payload) < 4 {
		return gogo.WSHubMessage{}, errors.New("gogo/adapters/redis: short message")
	}
	if payload[0] != wireVersion {
		return gogo.WSHubMessage{}, fmt.Errorf("gogo/adapters/redis: unsupported wire version %d", payload[0])
	}
	nodeLen := int(binary.BigEndian.Uint16(payload[2:4]))
	if len(payload) < 4+nodeLen {
		return gogo.WSHubMessage{}, errors.New("gogo/adapters/redis: truncated node id")
	}
	msg := gogo.WSHubMessage{
		NodeID:  string(payload[4 : 4+nodeLen]),
		Topic:   topic,
		Message: cloneBytes(payload[4+nodeLen:]),
		OpCode:  gogo.OpCode(payload[1]),
	}
	return msg, nil
}

func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
