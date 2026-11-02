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
)

// TestReceiveWithAckMessageID runs against the in-process pstest server. A
// message carries the ID its publish returned, and a nacked message comes back
// with the same ID.
func TestReceiveWithAckMessageID(t *testing.T) {
	// pstest redelivers a nacked message about 10s later.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	published, err := topic.Publish(ctx, &pubsub.Message{Data: []byte(`{}`)}).Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	topic.Stop()

	msgCh, errCh, err := (&Store{}).ReceiveWithAck(ctx, "rtdn")
	if err != nil {
		t.Fatal(err)
	}
	for delivery := 1; delivery <= 2; delivery++ {
		select {
		case m, ok := <-msgCh:
			if !ok {
				t.Fatal("msgCh closed")
			}
			if m.ID != published {
				t.Fatalf("delivery %d has ID %q, published %q", delivery, m.ID, published)
			}
			if delivery == 1 {
				m.Nack()
			} else {
				m.Ack()
			}
		case err, ok := <-errCh:
			if ok {
				t.Fatalf("receive: %v", err)
			}
			t.Fatal("errCh closed")
		case <-ctx.Done():
			t.Fatalf("delivery %d did not come", delivery)
		}
	}
}
