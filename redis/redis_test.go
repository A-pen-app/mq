package redis

import (
	"context"
	"testing"
	"time"
)

// The connection is module state, so every test here restores it rather than
// assuming what ran before.

func TestInitializeCreatesClientAndFinalizeReleasesIt(t *testing.T) {
	t.Cleanup(Finalize)

	Initialize(context.Background(), &Config{Addr: "127.0.0.1:6379"})
	if client == nil {
		t.Fatal("Initialize left the client nil")
	}

	Finalize()
	if client != nil {
		t.Fatal("Finalize left the client set")
	}
}

// A nil section means the app did not configure Redis, same as the other
// drivers -- not an error, just nothing to build.
func TestInitializeIgnoresNilConfig(t *testing.T) {
	t.Cleanup(Finalize)
	Finalize()

	Initialize(context.Background(), nil)

	if client != nil {
		t.Fatal("nil config still built a client")
	}
}

// The bug this branch fixes: apps register routes from package init(), which runs
// before main() calls Initialize, so New must not need a connection yet.
func TestNewBeforeInitializeDoesNotPanic(t *testing.T) {
	t.Cleanup(Finalize)
	Finalize()

	svc := New(NewHub[testEvent](), Options{Channel: "test:events"})

	if _, ok := svc.(*bridge[testEvent]); !ok {
		t.Fatalf("New returned %T, want *bridge[testEvent]", svc)
	}
}

func TestPublishBeforeInitializeReturnsError(t *testing.T) {
	t.Cleanup(Finalize)
	Finalize()

	err := New(NewHub[testEvent](), Options{Channel: "test:events"}).
		Publish(context.Background(), testEvent{Room: "room-1"})

	if err == nil {
		t.Fatal("Publish succeeded without a connection")
	}
}

// Run is started once per pod and must survive being started first; giving up
// would leave that pod blind to every other pod's events.
func TestRunWaitsForInitialize(t *testing.T) {
	t.Cleanup(Finalize)
	Finalize()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		New(NewHub[testEvent](), Options{Channel: "test:events"}).Run(ctx)
	}()

	select {
	case <-stopped:
		t.Fatal("Run gave up instead of waiting for Initialize")
	case <-time.After(100 * time.Millisecond):
	}

	Initialize(context.Background(), &Config{Addr: "127.0.0.1:6379"})
	cancel()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Run never proceeded after Initialize")
	}
}

func TestLocalDeliversToTheHub(t *testing.T) {
	hub := NewHub[testEvent]()
	events, unsubscribe := hub.Subscribe("room-1")
	defer unsubscribe()

	if err := NewLocal(hub).Publish(context.Background(), testEvent{Room: "room-1"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-events:
	default:
		t.Fatal("the in-process variant did not deliver to the hub")
	}
}

// Finalize closes the client, which closes the subscription. That is shutdown,
// not failure -- and the caller's context is no help, because Finalize runs
// before it is cancelled.
func TestRunStaysQuietWhenFinalizeClosesTheSubscription(t *testing.T) {
	t.Cleanup(Finalize)
	Initialize(context.Background(), &Config{Addr: "127.0.0.1:6379"})

	messages := make(chan string)
	reported := make(chan error, 1)
	b := &bridge[testEvent]{
		hub:      NewHub[testEvent](),
		sub:      &fakeRedisSubscriber{messages: messages},
		channel:  "test:events",
		onError:  func(ctx context.Context, err error) { reported <- err },
		shutdown: shutdown,
	}

	stopped := make(chan struct{})
	go func() { defer close(stopped); b.Run(context.Background()) }()

	Finalize()
	close(messages)

	<-stopped
	select {
	case err := <-reported:
		t.Fatalf("shutdown reported an error: %v", err)
	default:
	}
}

func TestNewResolvesTheModuleClient(t *testing.T) {
	t.Cleanup(Finalize)
	Initialize(context.Background(), &Config{Addr: "127.0.0.1:6379"})

	b, ok := New(NewHub[testEvent](), Options{Channel: "test:events"}).(*bridge[testEvent])
	if !ok {
		t.Fatal("New did not return a bridge")
	}
	if b.channel != "test:events" {
		t.Errorf("channel = %q, want %q", b.channel, "test:events")
	}

	pub, err := b.publisher()
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	if pub.(*goRedis).rdb != client {
		t.Error("the bridge built its own client instead of using the initialized one")
	}
}

func TestInitializeTwiceClosesTheFirstShutdown(t *testing.T) {
	t.Cleanup(Finalize)

	Initialize(context.Background(), &Config{Addr: "127.0.0.1:6379"})
	first := shutdown

	Initialize(context.Background(), &Config{Addr: "127.0.0.1:6380"})

	select {
	case <-first:
	default:
		t.Fatal("the first shutdown channel stayed open -- bridges holding it would never stop quietly")
	}
	if shutdown == first {
		t.Fatal("Initialize reused the closed channel")
	}
}
