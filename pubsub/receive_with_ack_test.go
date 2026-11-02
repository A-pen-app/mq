package pubsub

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/A-pen-app/mq/v2/models"
)

// TestReceiveWithAckMessageID runs against the in-process pstest server. Each
// message carries the ID its publish returned, so the same bytes published
// twice are two IDs: the ID tells a redelivery apart, not a duplicate publish.
func TestReceiveWithAckMessageID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv := pstest.NewServer()
	t.Cleanup(func() { srv.Close() })
	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	cl, err := pubsub.NewClient(ctx, "test-project", option.WithGRPCConn(conn))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })

	topic, err := cl.CreateTopic(ctx, "rtdn")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cl.CreateSubscription(ctx, "rtdn.test", pubsub.SubscriptionConfig{Topic: topic}); err != nil {
		t.Fatal(err)
	}

	oldClient, oldConfig := client, c
	client, c = cl, &Config{ProjectID: "test-project", Topics: map[string]TopicConfig{"rtdn": {SubscriptionID: "rtdn.test"}}}
	t.Cleanup(func() { client, c = oldClient, oldConfig })

	var published []string
	for i := 0; i < 2; i++ {
		id, err := topic.Publish(ctx, &pubsub.Message{Data: []byte(`{"same":"bytes"}`)}).Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		published = append(published, id)
	}
	topic.Stop()
	if published[0] == published[1] {
		t.Fatalf("two publishes got one ID %q", published[0])
	}

	msgCh, errCh, err := (&Store{}).ReceiveWithAck(ctx, "rtdn")
	if err != nil {
		t.Fatal(err)
	}
	next := func() *models.Message {
		t.Helper()
		select {
		case m, ok := <-msgCh:
			if !ok {
				t.Fatal("msgCh closed")
			}
			if m.ID == "" {
				t.Fatal("message without an ID")
			}
			return m
		case err, ok := <-errCh:
			if ok {
				t.Fatalf("receive: %v", err)
			}
			t.Fatal("errCh closed")
		case <-ctx.Done():
			t.Fatal("no message")
		}
		return nil
	}

	// Not nacked: pstest reuses the message ID as the ack ID, and a redelivery
	// can race the client's pending nack for it, so a redelivery test would
	// flake (7 of 20 runs with -race).
	received := map[string]bool{}
	for len(received) < len(published) {
		m := next()
		received[m.ID] = true
		m.Ack()
	}
	for _, id := range published {
		if !received[id] {
			t.Fatalf("published %q, received %v", id, received)
		}
	}
}
