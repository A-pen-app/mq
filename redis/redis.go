package redis

import (
	"context"
	"sync"

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
//
// Guarded, because New and Run can run before Initialize: apps register routes
// from package init(), which is ahead of main().
var (
	mu     sync.RWMutex
	client *goredis.Client
	// shutdown is closed by Finalize. Closing the client closes every running
	// subscription, and Run has no other way to tell that apart from losing its
	// subscription for real -- Finalize runs before the caller's context is
	// cancelled.
	shutdown chan struct{}
	// ready is closed by Initialize, so a Run that started first can wait for the
	// connection instead of failing. Finalize replaces it with an open one.
	ready = make(chan struct{})
)

// connection reports the current state. A nil client means Initialize has not
// run, or Finalize already released it.
func connection() (*goredis.Client, <-chan struct{}, <-chan struct{}) {
	mu.RLock()
	defer mu.RUnlock()
	return client, shutdown, ready
}

type Config struct {
	Addr string
}

// Initialize opens the connection; nil means the app does not use Redis.
// Separate from any cache pool by necessity: SUBSCRIBE occupies a connection.
func Initialize(ctx context.Context, c *Config) {
	if c == nil {
		return
	}
	// A second Initialize would otherwise strand the first shutdown channel, and
	// bridges built against it would never learn that they were told to stop.
	Finalize()

	mu.Lock()
	defer mu.Unlock()
	client = goredis.NewClient(&goredis.Options{Addr: c.Addr})
	shutdown = make(chan struct{})
	close(ready)
}

func Finalize() {
	mu.Lock()
	defer mu.Unlock()
	if client != nil {
		close(shutdown)
		client.Close()
		client = nil
		shutdown = nil
		ready = make(chan struct{})
	}
}

// Options is per-Service -- one process may broadcast several event types.
type Options struct {
	Channel string
	// OnError reports a payload Run failed to decode and then skipped. Nil
	// ignores them.
	OnError func(ctx context.Context, err error)
}

// New connects a bridge to Redis pub/sub. The connection is resolved when the
// bridge is used, not here -- New may run during package init, before main has
// called Initialize.
func New[T Keyed](hub *Hub[T], opts Options) Service[T] {
	return &bridge[T]{
		hub:     hub,
		channel: opts.Channel,
		onError: opts.OnError,
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
