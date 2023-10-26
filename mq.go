package mq

import (
	"context"

	"github.com/A-pen-app/mq/pubsubLite"
	"github.com/A-pen-app/mq/rabbitmq"
)

type MQ interface {
	// send some data to a topic
	Send(topic string, data interface{}) error
	SendWithContext(ctx context.Context, topic string, data interface{}) error

	// or, pass messages back to client
	Receive(topic string) (<-chan []byte, error)
}

type Config struct {
	Pubsub   *pubsubLite.Config
	Rabbitmq *rabbitmq.Config
}

// Initialize ...
func Initialize(ctx context.Context, config *Config) {
	if config == nil {
		return
	}
	rabbitmq.Initialize(ctx, config.Rabbitmq)
	pubsubLite.Initialize(ctx, config.Pubsub)
}

func GetPubsub() MQ {
	return &pubsubLite.Store{}
}

func GetRabbitmq() MQ {
	return &rabbitmq.Store{}
}

// Finalize ...
func Finalize() {
	rabbitmq.Finalize()
	pubsubLite.Finalize()
}
