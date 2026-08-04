package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

// Interfaces so both directions are testable without a live server.
type redisPublisher interface {
	Publish(ctx context.Context, channel string, payload []byte) error
}

type redisSubscriber interface {
	Subscribe(ctx context.Context, channel string) <-chan string
}

// bridge carries events between pods: every pod publishes and every pod
// subscribes, so a connection on pod A sees an event from pod B.
type bridge[T Keyed] struct {
	hub      *Hub[T]
	client   redisPublisher
	sub      redisSubscriber
	channel  string
	onError  func(ctx context.Context, err error)
	shutdown <-chan struct{}
}

// stopping reports whether the subscription ended because someone asked for it.
func (b *bridge[T]) stopping(ctx context.Context) bool {
	if ctx.Err() != nil {
		return true
	}
	select {
	case <-b.shutdown:
		return true
	default:
		return false
	}
}

// Run consumes the subscription until ctx is cancelled. One per pod.
func (b *bridge[T]) Run(ctx context.Context) {
	messages := b.sub.Subscribe(ctx, b.channel)

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messages:
			// Losing the subscription unasked means this pod silently stops
			// seeing other pods' events -- nothing crashes, so say so.
			if !ok {
				if !b.stopping(ctx) && b.onError != nil {
					b.onError(ctx, errors.New("subscription closed"))
				}
				return
			}
			if err := b.dispatch([]byte(msg)); err != nil && b.onError != nil {
				b.onError(ctx, err)
			}
		}
	}
}

// Reaches every pod including this one -- the local Hub is fed by the
// subscription, not directly, so ordering cannot diverge.
func (b *bridge[T]) Publish(ctx context.Context, ev T) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event for key %s: %w", ev.Key(), err)
	}

	if err := b.client.Publish(ctx, b.channel, payload); err != nil {
		return fmt.Errorf("publish to channel %s for key %s: %w", b.channel, ev.Key(), err)
	}
	return nil
}

// One channel carries every room, so Broadcast is a no-op when nobody here is
// watching the one this event names.
func (b *bridge[T]) dispatch(payload []byte) error {
	var ev T
	if err := json.Unmarshal(payload, &ev); err != nil {
		return fmt.Errorf("unmarshal payload %q: %w", string(payload), err)
	}

	b.hub.Broadcast(ev.Key(), ev)
	return nil
}

// goRedis adapts go-redis to the interfaces above. No branching of its own, so
// it is covered by integration checks rather than unit tests.
type goRedis struct {
	rdb *goredis.Client
}

func (g *goRedis) Publish(ctx context.Context, channel string, payload []byte) error {
	return g.rdb.Publish(ctx, channel, payload).Err()
}

// go-redis reconnects and re-subscribes internally, so the returned channel
// survives a blip; events published during the outage are missed.
func (g *goRedis) Subscribe(ctx context.Context, channel string) <-chan string {
	pubsub := g.rdb.Subscribe(ctx, channel)
	out := make(chan string)

	// Ranging below blocks until a message arrives, so cancelling ctx alone would
	// strand this goroutine -- closing the subscription is what unblocks it, and
	// go-redis does not do that on ctx.
	unregister := context.AfterFunc(ctx, func() { pubsub.Close() })

	go func() {
		defer unregister()
		defer close(out)
		defer pubsub.Close()

		for msg := range pubsub.Channel() {
			select {
			case out <- msg.Payload:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}
