package mq

import (
	"context"

	"github.com/A-pen-app/mq/pubsubLite"
	"github.com/A-pen-app/mq/rabbitmq"
)

type MQ interface {
	// send some data to a topic
	Send(topic string, data interface{}) error

	// or, pass messages back to client
	Receive(topic string) (<-chan []byte, error)
}

var (
	ctx context.Context
)

func init() {
}

// Initialize ...
func Initialize(ictx context.Context) {
	ctx = ictx
	rabbitmq.Initialize(ctx)
	pubsubLite.Initialize(ctx)
}

// Finalize ...
func Finalize() {
	rabbitmq.Finalize()
	pubsubLite.Finalize()
}
