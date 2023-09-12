package rabbitmq

import (
	"time"

	"github.com/A-pen-app/mq/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbitmqClient struct {
	mqurl   string
	conn    *amqp.Connection
	channel *amqp.Channel
	IsReady bool
}

func NewClient() *rabbitmqClient {
	r := rabbitmqClient{
		mqurl: config.RabbitmqConnURL,
	}
	return &r
}

func (r *rabbitmqClient) connect(retryDelay time.Duration) {
	go func() {
		for {
			conn, err := amqp.Dial(r.mqurl)
			if err != nil {
				time.Sleep(retryDelay)
				continue
			}
			r.conn = conn

			for {
				ch, err := r.conn.Channel()
				if err != nil {
					time.Sleep(retryDelay)
					continue
				}
				r.channel = ch
				r.IsReady = true
				<-r.conn.NotifyClose(make(chan *amqp.Error))
				r.IsReady = false
				break
			}
		}
	}()
}
