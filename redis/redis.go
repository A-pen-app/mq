package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"
)

// Publisher is all a producer needs. Run is deliberately not part of it --
// starting the subscription belongs to whoever owns the process lifecycle.
type Publisher[T Keyed] interface {
	Publish(ctx context.Context, ev T) error
}

type Service[T Keyed] interface {
	Publisher[T]
	Run(ctx context.Context)
}

// Only the connection is module state. A Hub belongs to one event type, so it
// stays with the app.
var client *goredis.Client

type Config struct {
	Addr string
}

// Initialize opens the connection; a nil config means the app does not use
// Redis. Separate from any cache pool by necessity -- SUBSCRIBE occupies a
// connection for as long as it is open.
func Initialize(ctx context.Context, c *Config) {
	if c == nil {
		return
	}
	client = goredis.NewClient(&goredis.Options{Addr: c.Addr})
}

func Finalize() {
	if client != nil {
		client.Close()
		client = nil
	}
}

// Options is per-Service: one process may broadcast several event types, each
// on its own channel.
type Options struct {
	Channel string
	// OnError reports a payload that failed to decode, which Run then skips --
	// one bad publisher must not stall delivery for everyone. Nil ignores them.
	OnError func(ctx context.Context, err error)
}

// New connects a bridge to Redis pub/sub on the configured channel. Panics when
// Initialize has not run, rather than silently degrading to single-pod delivery.
func New[T Keyed](hub *Hub[T], opts Options) Service[T] {
	if client == nil {
		panic("mq/redis: New called before Initialize")
	}

	adapter := &goRedis{rdb: client}

	return &bridge[T]{
		hub:     hub,
		client:  adapter,
		sub:     adapter,
		channel: opts.Channel,
		onError: opts.OnError,
	}
}

// NewLocal skips Redis and feeds the Hub directly, for when there is no Redis to
// reach. Not a degraded mode: a single process has no other pods to reach.
func NewLocal[T Keyed](hub *Hub[T]) Service[T] {
	return &local[T]{hub: hub}
}

type local[T Keyed] struct {
	hub *Hub[T]
}

func (l *local[T]) Publish(ctx context.Context, ev T) error {
	l.hub.Broadcast(ev.Key(), ev)
	return nil
}

// Blocks so callers can start it the same way either way.
func (l *local[T]) Run(ctx context.Context) {
	<-ctx.Done()
}
