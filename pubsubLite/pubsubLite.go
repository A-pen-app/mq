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
	"github.com/A-pen-app/mq/config"
	"golang.org/x/sync/errgroup"
)

var (
	ctx                context.Context
	projectID          = "490242039522"
	zone               = "asia-east1"
	ActionSubscription = config.GetString("SUBSCRIPTION_ACTION")
	ActionTopic        = config.GetString("TOPIC_ACTION")
	receiveTimeout     = 24 * 60 * 60 * time.Second
	isProduction       = config.GetBool("PRODUCTION_ENVIRONMENT")
	publisher          = make(map[string]*pscompat.PublisherClient)
	topicToSub         = make(map[string]string)
)

type PubSubLiteStore struct {
}

func init() {
	topicToSub[ActionTopic] = ActionSubscription
}

// FIXME: Reservation, topic, and subscription health check and initialization is to be added in the future.
func Initialize(ictx context.Context) {
	ctx = ictx

	topicPath := fmt.Sprintf("projects/%s/locations/%s/topics/%s", projectID, zone, ActionTopic)

	// Create the publisher client.
	p, err := pscompat.NewPublisherClient(ctx, topicPath)
	if err != nil {
		return
	}
	publisher[ActionTopic] = p

	/*
		ch, err := (&PubSubLiteStore{}).Receive(ActionTopic)
		if err != nil {
			logging.Error(ctx, err.Error())
			return
		}
		for {
			msg := <-ch
			logging.Debug(ctx, "msg: %s", string(msg))
		}
		(&PubSubLiteStore{}).Send(ActionTopic, "DEBUG: test message")
	*/
}

func (ps *PubSubLiteStore) Send(topic string, data interface{}) error {
	p := publisher[topic]

	// Collect any messages that need to be republished with a new publisher
	// client.
	var toRepublish []*pubsub.Message
	var toRepublishMu sync.Mutex

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

func (ps *PubSubLiteStore) Receive(topic string) (<-chan []byte, error) {
	var subID string
	if v, exist := topicToSub[topic]; exist {
		subID = v
	} else {
		return nil, errors.New("The provided topic does not exist.")
	}
	subscriptionPath := fmt.Sprintf("projects/%s/locations/%s/subscriptions/%s", projectID, zone, subID)

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
	go func(ctx context.Context, s *pscompat.SubscriberClient, ch chan<- []byte) {
		for {
			ctx, cancel := context.WithTimeout(ctx, receiveTimeout)

			// Receive blocks until the context is cancelled or an error occurs.
			if err = s.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
				// Metadata decoded from the message ID contains the partition and offset.
				_, err = pscompat.ParseMessageMetadata(msg.ID)
				if err != nil {
					return
				}

				byteCh <- msg.Data
				msg.Ack()
			}); err != nil {
				cancel()
				return
			}

			cancel()
		}
	}(ctx, s, byteCh)

	// logging.Info(ctx, fmt.Sprintf("Received %d messages\n", receiveCount))
	return byteCh, nil
}

// Finalize ...
func Finalize() {
	// Nothing to be done
	// Ensure the publisher will be shut down.
	for _, v := range publisher {
		defer v.Stop()
	}
}
