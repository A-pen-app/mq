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

type Config struct {
	Addr    string
	Channel string
	// OnError receives errors from the subscription loop. A payload that fails
	// to decode is reported here and skipped -- one bad publisher must not take
	// down delivery for everyone. Nil ignores them.
	OnError func(ctx context.Context, err error)
}

// New connects a bridge to Redis pub/sub on the configured channel.
//
// A separate client from any cache pool by necessity: SUBSCRIBE occupies a
// connection for as long as it is open.
func New[T Keyed](hub *Hub[T], cfg Config) Service[T] {
	client := &goRedis{rdb: goredis.NewClient(&goredis.Options{Addr: cfg.Addr})}

	return &bridge[T]{
		hub:     hub,
		client:  client,
		sub:     client,
		channel: cfg.Channel,
		onError: cfg.OnError,
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
