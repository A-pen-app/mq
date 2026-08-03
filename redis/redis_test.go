package redis

import (
	"context"
	"testing"
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

// Failing at startup beats silently degrading to single-pod delivery in
// production, where the symptom would be "some users don't see new messages".
func TestNewPanicsWithoutInitialize(t *testing.T) {
	t.Cleanup(Finalize)
	Finalize()

	defer func() {
		if recover() == nil {
			t.Fatal("New did not panic without Initialize")
		}
	}()

	New(NewHub[testEvent](), Options{Channel: "test:events"})
}

func TestNewUsesTheInitializedClient(t *testing.T) {
	t.Cleanup(Finalize)

	Initialize(context.Background(), &Config{Addr: "127.0.0.1:6379"})

	svc := New(NewHub[testEvent](), Options{Channel: "test:events"})
	b, ok := svc.(*bridge[testEvent])
	if !ok {
		t.Fatalf("New returned %T, want *bridge[testEvent]", svc)
	}
	if b.channel != "test:events" {
		t.Errorf("channel = %q, want %q", b.channel, "test:events")
	}
	if b.client.(*goRedis).rdb != client {
		t.Error("New built its own client instead of using the initialized one")
	}
}
