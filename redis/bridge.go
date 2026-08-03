package redis

import (
	"context"
	"encoding/json"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

// Kept as interfaces so both directions are testable without a live server --
// which matters when the dev cluster runs a single pod and can never exercise
// cross-pod delivery for real.
type redisPublisher interface {
	Publish(ctx context.Context, channel string, payload []byte) error
}

type redisSubscriber interface {
	Subscribe(ctx context.Context, channel string) <-chan string
}

// bridge carries events between pods. Every pod publishes what it produces and
// every pod subscribes, so a connection held by pod A sees an event from pod B.
type bridge[T Keyed] struct {
	hub     *Hub[T]
	client  redisPublisher
	sub     redisSubscriber
	channel string
	onError func(ctx context.Context, err error)
}

// Run consumes the subscription until ctx is cancelled. One per pod.
func (b *bridge[T]) Run(ctx context.Context) {
	messages := b.sub.Subscribe(ctx, b.channel)

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}
			if err := b.dispatch([]byte(msg)); err != nil && b.onError != nil {
				b.onError(ctx, err)
			}
		}
	}
}

// Publish reaches every pod including this one -- the local Hub is fed by the
// subscription rather than directly, so local and remote delivery cannot diverge
// in ordering.
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

// All pods share one channel, so a pod sees every room's events and filters
// locally: Broadcast is a no-op when nobody here is watching that room.
func (b *bridge[T]) dispatch(payload []byte) error {
	var ev T
	if err := json.Unmarshal(payload, &ev); err != nil {
		return fmt.Errorf("unmarshal payload %q: %w", string(payload), err)
	}

	b.hub.Broadcast(ev.Key(), ev)
	return nil
}

// goRedis adapts go-redis to the two interfaces above. Thin by design -- no
// branching of its own, so it is covered by integration checks, not unit tests.
type goRedis struct {
	rdb *goredis.Client
}

func (g *goRedis) Publish(ctx context.Context, channel string, payload []byte) error {
	return g.rdb.Publish(ctx, channel, payload).Err()
}

// go-redis reconnects and re-issues the subscription internally, so the returned
// channel survives a Redis blip; events published during the outage are missed.
func (g *goRedis) Subscribe(ctx context.Context, channel string) <-chan string {
	pubsub := g.rdb.Subscribe(ctx, channel)
	out := make(chan string)

	// Ranging over pubsub.Channel() blocks until a message arrives, so a
	// cancelled context alone would leave this goroutine and the Redis
	// connection stuck on an idle channel. Closing the subscription is what
	// unblocks it. The ctx passed to Subscribe only covers the initial SUBSCRIBE
	// command; go-redis does not tear the subscription down with it.
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
