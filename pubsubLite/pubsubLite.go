package pubsubLite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite/pscompat"
	"github.com/A-pen-app/mq/models"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

var (
	receiveTimeout = 24 * 60 * 60 * time.Second
	publisher      = make(map[string]*pscompat.PublisherClient)
	c              *Config
)

type Config struct {
	ProjectID    string
	RegionOrZone string
	Topics       map[string]string
}

type Store struct{}

// FIXME: Reservation, topic, and subscription health check and initialization is to be added in the future.
func Initialize(ctx context.Context, config *Config) {
	if config == nil {
		return
	}

	for topic := range config.Topics {
		topicPath := fmt.Sprintf("projects/%s/locations/%s/topics/%s", config.ProjectID, config.RegionOrZone, topic)

		// Create the publisher client.
		p, err := pscompat.NewPublisherClient(ctx, topicPath)
		if err != nil {
			continue
		}
		publisher[topic] = p
	}
	c = config
}

func (ps *Store) SendWithContext(ctx context.Context, topic string, data interface{}, opts ...models.GetMQOption) error {
	p := publisher[topic]
	if p == nil {
		return errors.New("publisher not found")
	}
	options := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("pubsub"),
			semconv.MessagingDestinationKey.String(topic),
			semconv.MessagingDestinationKindTopic,
		),
	}
	_, span := otel.Tracer("publisher:"+topic).Start(ctx, "pubsub.sendwithcontext", options...)
	defer span.End()

	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	msg := pubsub.Message{
		Data:       payload,
		Attributes: make(map[string]string),
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(msg.Attributes))

	msgID, err := p.Publish(ctx, &msg).Get(ctx)
	if err != nil {
		return err
	}
	span.SetAttributes(semconv.MessagingMessageIDKey.String(msgID))
	return nil

}

func (ps *Store) Send(topic string, data interface{}, opts ...models.GetMQOption) error {
	p := publisher[topic]
	if p == nil {
		return errors.New("publisher not found")
	}

	// Collect any messages that need to be republished with a new publisher
	// client.
	var toRepublish []*pubsub.Message
	var toRepublishMu sync.Mutex

	ctx := context.Background()
	// Publish messages. Messages are automatically batched.
	g := new(errgroup.Group)

	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}

	msg := &pubsub.Message{
		Data: payload,
	}
	result := p.Publish(ctx, msg)

	// FIXME: Resend mechanism to be added
	g.Go(func() error {
		// Get blocks until the result is ready.
		id, err := result.Get(ctx)
		if err != nil {
			// NOTE: A failed PublishResult indicates that the publisher client
			// encountered a fatal error and has permanently terminated. After the
			// fatal error has been resolved, a new publisher client instance must
			// be created to republish failed messages.
			toRepublishMu.Lock()
			toRepublish = append(toRepublish, msg)
			toRepublishMu.Unlock()
			return err
		}

		// Metadata decoded from the id contains the partition and offset.
		_, err = pscompat.ParseMessageMetadata(id)
		if err != nil {
			fmt.Printf("Failed to parse message metadata %q: %v\n", id, err)
			return err
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}

	// Print the error that caused the publisher client to terminate (if any),
	// which may contain more context than PublishResults.
	if err := p.Error(); err != nil {
		return err
	}

	return nil
}

func (ps *Store) ReceiveWithContext(ctx context.Context, topic string) (<-chan []byte, error) {
	subID, exist := c.Topics[topic]
	if !exist || len(subID) == 0 {
		return nil, errors.New("topic or subscription not found")
	}
	subscriptionPath := fmt.Sprintf("projects/%s/locations/%s/subscriptions/%s", c.ProjectID, c.RegionOrZone, subID)
	settings := pscompat.ReceiveSettings{
		MaxOutstandingBytes:    10 * 1024 * 1024,
		MaxOutstandingMessages: 1000,
	}
	s, err := pscompat.NewSubscriberClientWithSettings(
		ctx,
		subscriptionPath,
		settings,
	)
	if err != nil {
		return nil, err
	}
	byteCh := make(chan []byte)
	errCh := make(chan error)
	go func(ctx context.Context, s *pscompat.SubscriberClient, ch chan []byte, errCh chan error) {
		if err := s.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			if msg.Attributes != nil {
				propagator := otel.GetTextMapPropagator()
				ctx = propagator.Extract(ctx, propagation.MapCarrier(msg.Attributes))
			}
			options := []trace.SpanStartOption{
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					semconv.MessagingSystemKey.String("pubsub"),
					semconv.MessagingDestinationKey.String(subID),
					semconv.MessagingDestinationKindTopic,
					semconv.MessagingMessageIDKey.String(msg.ID),
				),
			}
			_, span := otel.Tracer("subscriber:"+topic).Start(ctx, "pubsub.receivewithcontext", options...)
			defer span.End()

			byteCh <- msg.Data
			msg.Ack()
		}); err != nil {
			errCh <- err
		}
	}(ctx, s, byteCh, errCh)

	return byteCh, nil

}

func (ps *Store) Receive(topic string) (<-chan []byte, error) {
	ctx := context.Background()

	subID, exist := c.Topics[topic]
	if !exist || len(subID) == 0 {
		return nil, errors.New("topic or subscription not found")
	}

	subscriptionPath := fmt.Sprintf("projects/%s/locations/%s/subscriptions/%s", c.ProjectID, c.RegionOrZone, subID)

	// Configure flow control settings. These settings apply per partition.
	// The message stream is paused based on the maximum size or number of
	// messages that the subscriber has already received, whichever condition is
	// met first.
	settings := pscompat.ReceiveSettings{
		// 10 MiB. Must be greater than the allowed size of the largest message
		// (1 MiB).
		MaxOutstandingBytes: 10 * 1024 * 1024,
		// 1,000 outstanding messages. Must be > 0.
		MaxOutstandingMessages: 1000,
	}

	// Create the subscriber client.
	s, err := pscompat.NewSubscriberClientWithSettings(
		ctx,
		subscriptionPath,
		settings,
	)
	if err != nil {
		return nil, err
	}

	byteCh := make(chan []byte)
	errCh := make(chan error)
	go func(ctx context.Context, s *pscompat.SubscriberClient, ch chan []byte, errCh chan error) {
		for {
			ctx, cancel := context.WithTimeout(ctx, receiveTimeout)

			// Receive blocks until the context is cancelled or an error occurs.
			if err = s.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
				// Metadata decoded from the message ID contains the partition and offset.
				_, err = pscompat.ParseMessageMetadata(msg.ID)
				if err != nil {
					errCh <- err
					return
				}
				byteCh <- msg.Data
				msg.Ack()
			}); err != nil {
				errCh <- err
			}

			cancel()
		}
	}(ctx, s, byteCh, errCh)

	// logging.Info(ctx, fmt.Sprintf("Received %d messages\n", receiveCount))
	return byteCh, nil
}

// Finalize ...
func Finalize() {
	// Ensure the publisher will be shut down.
	for _, v := range publisher {
		v.Stop()
	}
}
