package mq

import (
	"context"

	"github.com/A-pen-app/mq/v2/models"
	"github.com/A-pen-app/mq/v2/pubsub"
	"github.com/A-pen-app/mq/v2/pubsubLite"
	"github.com/A-pen-app/mq/v2/rabbitmq"
)

type MQ interface {
	// send some data to a topic
	Send(topic string, data interface{}, options ...models.GetMQOption) error
	SendWithContext(ctx context.Context, topic string, data interface{}, options ...models.GetMQOption) error

	// or, pass messages back to client
	Receive(topic string) (<-chan []byte, error)
	ReceiveWithContext(ctx context.Context, topic string) (<-chan []byte, error)
}

type Config struct {
	Pubsub    *pubsubLite.Config
	Rabbitmq  *rabbitmq.Config
	NewPubsub *pubsub.Config
}

// Initialize ...
func Initialize(ctx context.Context, config *Config) {
	if config == nil {
		return
	}
	rabbitmq.Initialize(ctx, config.Rabbitmq)
	pubsubLite.Initialize(ctx, config.Pubsub)
	pubsub.Initialize(ctx, config.NewPubsub)
}

func GetPubsub() MQ {
	return &pubsubLite.Store{}
}

func GetNewPubsub() MQ {
	return &pubsub.Store{}
}

func GetRabbitmq() MQ {
	return &rabbitmq.Store{}
}

// Finalize ...
func Finalize() {
	rabbitmq.Finalize()
	pubsubLite.Finalize()
	pubsub.Finalize()
}
