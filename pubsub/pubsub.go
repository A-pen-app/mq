package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/A-pen-app/mq/v2/models"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

var (
	receiveTimeout = 24 * 60 * 60 * time.Second
	publisher      = make(map[string]*pubsub.Topic)
	client         *pubsub.Client
	c              *Config
)

type TopicConfig struct {
	SubscriptionID string
}

type Config struct {
	ProjectID string
	Topics    map[string]TopicConfig
}

type Store struct{}

func Initialize(ctx context.Context, config *Config) {
	if config == nil {
		return
	}

	newClient, err := pubsub.NewClient(ctx, config.ProjectID)
	if err != nil {
		return
	}

	for topic := range config.Topics {
		t := newClient.Topic(topic)
		publisher[topic] = t
	}
	c = config
	client = newClient
}

func (ps *Store) SendWithContext(ctx context.Context, topic string, data interface{}, opts ...models.GetMQOption) error {
	p := publisher[topic]
	if p == nil {
		return errors.New("publisher not found")
	}

	opt := &models.MQOption{}
	for _, f := range opts {
		if err := f(opt); err != nil {
			return err
		}
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

	attributes := map[string]string{}
	if opt.Attributes != nil {
		attributes = map[string]string{
			"receiver":               string(opt.Attributes.Receiver),
			"deployment_environment": string(opt.Attributes.DeploymentEnvironment),
		}
	}

	msg := pubsub.Message{
		Data:       payload,
		Attributes: attributes,
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

	opt := &models.MQOption{}
	for _, f := range opts {
		if err := f(opt); err != nil {
			return err
		}
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

	attributes := map[string]string{}
	if opt.Attributes != nil {
		attributes = map[string]string{
			"receiver":               string(opt.Attributes.Receiver),
			"deployment_environment": string(opt.Attributes.DeploymentEnvironment),
		}
	}

	msg := &pubsub.Message{
		Data:       payload,
		Attributes: attributes,
	}
	result := p.Publish(ctx, msg)

	// FIXME: Resend mechanism to be added
	g.Go(func() error {
		// Get blocks until the result is ready.
		_, err := result.Get(ctx)
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

		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (ps *Store) ReceiveWithContext(ctx context.Context, topic string) (<-chan []byte, error) {
	tc, exist := c.Topics[topic]
	if !exist || len(tc.SubscriptionID) == 0 {
		return nil, errors.New("topic or subscription not found")
	}
	settings := pubsub.ReceiveSettings{
		MaxOutstandingBytes:    10 * 1024 * 1024,
		MaxOutstandingMessages: 1000,
	}

	// bind the subscription.
	sub := client.Subscription(tc.SubscriptionID)
	exists, err := sub.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("check subscriptionID %s existence failed: %w", tc.SubscriptionID, err)
	} else if !exists {
		return nil, fmt.Errorf("subscriptionID %s not found", tc.SubscriptionID)
	}
	sub.ReceiveSettings = settings

	byteCh := make(chan []byte)
	errCh := make(chan error)
	go func(ctx context.Context, s *pubsub.Subscription, ch chan []byte, errCh chan error) {
		if err := s.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			if msg.Attributes != nil {
				propagator := otel.GetTextMapPropagator()
				ctx = propagator.Extract(ctx, propagation.MapCarrier(msg.Attributes))
			}

			options := []trace.SpanStartOption{
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					semconv.MessagingSystemKey.String("pubsub"),
					semconv.MessagingDestinationKey.String(tc.SubscriptionID),
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
	}(ctx, sub, byteCh, errCh)

	return byteCh, nil
}

// ReceiveWithAck receives messages with manual Ack/Nack control
func (ps *Store) ReceiveWithAck(ctx context.Context, topic string) (<-chan *models.Message, <-chan error, error) {
	tc, exist := c.Topics[topic]
	if !exist || len(tc.SubscriptionID) == 0 {
		return nil, nil, errors.New("topic or subscription not found")
	}
	settings := pubsub.ReceiveSettings{
		MaxOutstandingBytes:    10 * 1024 * 1024,
		MaxOutstandingMessages: 1000,
	}

	// bind the subscription.
	sub := client.Subscription(tc.SubscriptionID)
	exists, err := sub.Exists(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("check subscriptionID %s existence failed: %w", tc.SubscriptionID, err)
	} else if !exists {
		return nil, nil, fmt.Errorf("subscriptionID %s not found", tc.SubscriptionID)
	}
	sub.ReceiveSettings = settings

	msgCh := make(chan *models.Message)
	errCh := make(chan error)
	go func(ctx context.Context, s *pubsub.Subscription, ch chan *models.Message, errCh chan error) {
		defer close(ch)
		defer close(errCh)
		if err := s.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			if msg.Attributes != nil {
				propagator := otel.GetTextMapPropagator()
				ctx = propagator.Extract(ctx, propagation.MapCarrier(msg.Attributes))
			}

			options := []trace.SpanStartOption{
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					semconv.MessagingSystemKey.String("pubsub"),
					semconv.MessagingDestinationKey.String(tc.SubscriptionID),
					semconv.MessagingDestinationKindTopic,
					semconv.MessagingMessageIDKey.String(msg.ID),
				),
			}
			_, span := otel.Tracer("subscriber:"+topic).Start(ctx, "pubsub.receivewithack", options...)
			defer span.End()

			wrappedMsg := &models.Message{
				Data:     msg.Data,
				AckFunc:  func() { msg.Ack() },
				NackFunc: func() { msg.Nack() },
			}
			ch <- wrappedMsg

		}); err != nil {
			errCh <- err
		}
	}(ctx, sub, msgCh, errCh)

	return msgCh, errCh, nil
}

func (ps *Store) Receive(topic string) (<-chan []byte, error) {
	ctx := context.Background()

	tc, exist := c.Topics[topic]
	if !exist || len(tc.SubscriptionID) == 0 {
		return nil, errors.New("topic or subscription not found")
	}

	// The message stream is paused based on the maximum size or number of
	// messages that the subscriber has already received, whichever condition is
	// met first.
	settings := pubsub.ReceiveSettings{
		// 10 MiB. Must be greater than the allowed size of the largest message
		// (1 MiB).
		MaxOutstandingBytes: 10 * 1024 * 1024,
		// 1,000 outstanding messages. Must be > 0.
		MaxOutstandingMessages: 1000,
	}

	// bind the subscription.
	sub := client.Subscription(tc.SubscriptionID)
	exists, err := sub.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("check subscriptionID %s existence failed: %w", tc.SubscriptionID, err)
	} else if !exists {
		return nil, fmt.Errorf("subscriptionID %s not found", tc.SubscriptionID)
	}
	sub.ReceiveSettings = settings

	byteCh := make(chan []byte)
	errCh := make(chan error)
	go func(ctx context.Context, s *pubsub.Subscription, ch chan []byte, errCh chan error) {
		for {
			ctx, cancel := context.WithTimeout(ctx, receiveTimeout)

			// Receive blocks until the context is cancelled or an error occurs.
			if err := s.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
				byteCh <- msg.Data
				msg.Ack()
			}); err != nil {
				errCh <- err
			}

			cancel()
		}
	}(ctx, sub, byteCh, errCh)

	return byteCh, nil
}

// Finalize ...
func Finalize() {
	// Ensure the publisher will be shut down.
	for _, v := range publisher {
		v.Stop()
	}
	client.Close()
}
