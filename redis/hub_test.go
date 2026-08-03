package redis

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type testEvent struct {
	Room string `json:"room"`
	Body string `json:"body"`
}

func (e testEvent) Key() string { return e.Room }

func TestHubBroadcastReachesSubscriber(t *testing.T) {
	h := NewHub[testEvent]()

	events, unsubscribe := h.Subscribe("room-1")
	defer unsubscribe()

	h.Broadcast("room-1", testEvent{Room: "room-1", Body: "hello"})

	select {
	case got := <-events:
		if got.Body != "hello" {
			t.Errorf("Body = %q, want %q", got.Body, "hello")
		}
	default:
		t.Fatal("subscriber received nothing")
	}
}

// A connection that stops draining its channel must not stall delivery for
// everyone else in the room. Its events are dropped instead.
func TestHubBroadcastDoesNotBlockOnSlowSubscriber(t *testing.T) {
	h := NewHub[testEvent]()

	// Subscribed and never read from, so the buffer fills and stays full. Not
	// unsubscribed either: that takes the write lock, which would deadlock
	// against a Broadcast still blocked under the read lock -- exactly the
	// failure this test is here to catch.
	h.Subscribe("room-1")

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range subscriberBuffer + 5 {
			h.Broadcast("room-1", testEvent{Room: "room-1"})
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Broadcast blocked on a subscriber that stopped reading")
	}
}

// Handlers defer their unsubscribe. If it did not detach, closed connections
// would accumulate for the lifetime of the pod.
func TestHubUnsubscribeStopsDelivery(t *testing.T) {
	h := NewHub[testEvent]()
	events, unsubscribe := h.Subscribe("room-1")

	unsubscribe()
	h.Broadcast("room-1", testEvent{Room: "room-1"})

	select {
	case ev, open := <-events:
		if open {
			t.Fatalf("received %+v after unsubscribe", ev)
		}
	default:
		t.Fatal("channel still open after unsubscribe")
	}
}

// Leaking an event into the wrong room is a privacy bug.
func TestHubBroadcastIsScopedToOneRoom(t *testing.T) {
	h := NewHub[testEvent]()

	other, unsubscribe := h.Subscribe("room-2")
	defer unsubscribe()

	h.Broadcast("room-1", testEvent{Room: "room-1"})

	select {
	case ev := <-other:
		t.Fatalf("room-2 subscriber received %+v meant for room-1", ev)
	default:
	}
}

// One Hub is shared by every connection on the pod, so these genuinely race.
// Meaningful only under -race.
func TestHubHandlesConcurrentAccess(t *testing.T) {
	h := NewHub[testEvent]()

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			room := fmt.Sprintf("room-%d", i%5)
			events, unsubscribe := h.Subscribe(room)
			defer unsubscribe()

			h.Broadcast(room, testEvent{Room: room})
			select {
			case <-events:
			default:
			}
		}()
	}
	wg.Wait()
}
