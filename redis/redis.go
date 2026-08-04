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

// Only the connection is module state -- a Hub belongs to one event type, so it
// stays with the app.
var (
	client *goredis.Client
	// shutdown is closed by Finalize. Closing the client closes every running
	// subscription, and Run has no other way to tell that apart from losing its
	// subscription for real -- Finalize runs before the caller's context is
	// cancelled.
	shutdown chan struct{}
)

type Config struct {
	Addr string
}

// Initialize opens the connection; nil means the app does not use Redis.
// Separate from any cache pool by necessity: SUBSCRIBE occupies a connection.
func Initialize(ctx context.Context, c *Config) {
	if c == nil {
		return
	}
	client = goredis.NewClient(&goredis.Options{Addr: c.Addr})
	shutdown = make(chan struct{})
}

func Finalize() {
	if client != nil {
		close(shutdown)
		client.Close()
		client = nil
		shutdown = nil
	}
}

// Options is per-Service -- one process may broadcast several event types.
type Options struct {
	Channel string
	// OnError reports a payload Run failed to decode and then skipped. Nil
	// ignores them.
	OnError func(ctx context.Context, err error)
}

// New connects a bridge to Redis pub/sub. Panics when Initialize has not run,
// rather than silently degrading to single-pod delivery.
func New[T Keyed](hub *Hub[T], opts Options) Service[T] {
	if client == nil {
		panic("mq/redis: New called before Initialize")
	}

	adapter := &goRedis{rdb: client}

	return &bridge[T]{
		hub:      hub,
		client:   adapter,
		sub:      adapter,
		channel:  opts.Channel,
		onError:  opts.OnError,
		shutdown: shutdown,
	}
}

// NewLocal feeds the Hub directly. Not a degraded mode: a single process, or an
// event type that does not need cross-pod delivery, has no other pods to reach.
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
