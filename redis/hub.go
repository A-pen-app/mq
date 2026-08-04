// Package redis fans events out to long-lived connections held by this pod, and
// carries them between pods over Redis pub/sub.
//
// It deliberately does not implement MQ: the other drivers are work queues (one
// message, one consumer), this is a broadcast (every pod, dropped if nobody is
// listening). Swapping one for the other compiles and then misbehaves.
package redis

import "sync"

// How far one connection may fall behind before Broadcast drops its events.
const subscriberBuffer = 16

// Keyed is how an event names the room it belongs to.
type Keyed interface {
	Key() string
}

// Hub delivers events to the subscribers held by THIS pod. Cross-pod delivery is
// the bridge's job.
type Hub[T Keyed] struct {
	mu   sync.RWMutex
	subs map[string]map[chan T]struct{}
}

func NewHub[T Keyed]() *Hub[T] {
	return &Hub[T]{subs: make(map[string]map[chan T]struct{})}
}

// Subscribe returns the channel to read from and the function that detaches it.
// Callers must call that function or the subscriber leaks.
func (h *Hub[T]) Subscribe(key string) (<-chan T, func()) {
	ch := make(chan T, subscriberBuffer)

	h.mu.Lock()
	if h.subs[key] == nil {
		h.subs[key] = make(map[chan T]struct{})
	}
	h.subs[key][ch] = struct{}{}
	h.mu.Unlock()

	return ch, func() { h.unsubscribe(key, ch) }
}

// Best-effort: a subscriber whose buffer is full loses this event rather than
// stalling everyone else.
func (h *Hub[T]) Broadcast(key string, ev T) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for ch := range h.subs[key] {
		select {
		case ch <- ev:
		default:
		}
	}
}

func (h *Hub[T]) SubscriberCount(key string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return len(h.subs[key])
}

// Closing ch is what ends the reader's loop. The membership check makes a repeat
// call a no-op instead of a close-of-closed-channel panic.
func (h *Hub[T]) unsubscribe(key string, ch chan T) {
	h.mu.Lock()
	defer h.mu.Unlock()

	subs, ok := h.subs[key]
	if !ok {
		return
	}
	if _, ok := subs[ch]; !ok {
		return
	}

	delete(subs, ch)
	if len(subs) == 0 {
		delete(h.subs, key)
	}
	close(ch)
}
