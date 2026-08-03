package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

type fakeRedisPublisher struct {
	channel string
	payload []byte
}

func (f *fakeRedisPublisher) Publish(ctx context.Context, channel string, payload []byte) error {
	f.channel = channel
	f.payload = payload
	return nil
}

type fakeRedisSubscriber struct {
	messages chan string
}

func (f *fakeRedisSubscriber) Subscribe(ctx context.Context, channel string) <-chan string {
	return f.messages
}

// The cross-pod contract: what the sending pod publishes has to decode back into
// the same event on the receiving pod, and land in the room it names.
func TestBridgePublishRoundTripsThroughDispatch(t *testing.T) {
	sender := &fakeRedisPublisher{}
	sending := &bridge[testEvent]{client: sender, channel: "test:events"}

	err := sending.Publish(context.Background(), testEvent{Room: "room-7", Body: "hi"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if sender.channel != "test:events" {
		t.Errorf("published to %q, want %q", sender.channel, "test:events")
	}

	// Now play the receiving pod: a different Hub, fed only by the bytes that
	// went over the wire.
	h := NewHub[testEvent]()
	events, unsubscribe := h.Subscribe("room-7")
	defer unsubscribe()

	if err := (&bridge[testEvent]{hub: h}).dispatch(sender.payload); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	select {
	case got := <-events:
		if got.Body != "hi" {
			t.Errorf("Body = %q, want %q", got.Body, "hi")
		}
	default:
		t.Fatal("round trip lost the event")
	}
}

func TestBridgeDispatchDeliversToNamedRoom(t *testing.T) {
	h := NewHub[testEvent]()
	events, unsubscribe := h.Subscribe("room-1")
	defer unsubscribe()

	payload, err := json.Marshal(testEvent{Room: "room-1"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if err := (&bridge[testEvent]{hub: h}).dispatch(payload); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	select {
	case <-events:
	default:
		t.Fatal("dispatch delivered nothing to room-1")
	}
}

// One pod publishing junk must not take down every other pod's subscription
// loop, so dispatch reports the error rather than panicking.
func TestBridgeDispatchRejectsMalformedPayload(t *testing.T) {
	h := NewHub[testEvent]()
	events, unsubscribe := h.Subscribe("room-1")
	defer unsubscribe()

	if err := (&bridge[testEvent]{hub: h}).dispatch([]byte("{not json")); err == nil {
		t.Fatal("dispatch accepted a malformed payload")
	}

	select {
	case ev := <-events:
		t.Fatalf("malformed payload still delivered %+v", ev)
	default:
	}
}

func runningBridge(t *testing.T, h *Hub[testEvent]) (chan string, context.CancelFunc, <-chan struct{}) {
	t.Helper()

	messages := make(chan string, 4)
	b := &bridge[testEvent]{
		hub:     h,
		sub:     &fakeRedisSubscriber{messages: messages},
		channel: "test:events",
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		b.Run(ctx)
	}()

	return messages, cancel, stopped
}

func TestBridgeRunDeliversSubscribedEvents(t *testing.T) {
	h := NewHub[testEvent]()
	events, unsubscribe := h.Subscribe("room-3")
	defer unsubscribe()

	messages, _, _ := runningBridge(t, h)

	payload, err := json.Marshal(testEvent{Room: "room-3"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	messages <- string(payload)

	select {
	case got := <-events:
		if got.Room != "room-3" {
			t.Errorf("Room = %q, want %q", got.Room, "room-3")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run never delivered the subscribed event")
	}
}

// Run owns a goroutine for the pod's lifetime. Ignoring cancellation would keep
// the Redis connection open past shutdown.
func TestBridgeRunStopsOnContextCancel(t *testing.T) {
	_, cancel, stopped := runningBridge(t, NewHub[testEvent]())

	cancel()

	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Run kept going after its context was cancelled")
	}
}

// A payload that fails to decode has to reach the caller. Swallowing it would
// leave cross-pod delivery broken with nothing to show for it.
func TestBridgeRunReportsMalformedPayload(t *testing.T) {
	messages := make(chan string, 1)
	reported := make(chan error, 1)

	b := &bridge[testEvent]{
		hub:     NewHub[testEvent](),
		sub:     &fakeRedisSubscriber{messages: messages},
		channel: "test:events",
		onError: func(ctx context.Context, err error) { reported <- err },
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Run(ctx)

	messages <- "{not json"

	select {
	case err := <-reported:
		if err == nil {
			t.Fatal("onError got a nil error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run never reported the malformed payload")
	}
}
