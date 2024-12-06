package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/A-pen-app/mq/models"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Store struct {
}

var (
	ctx    context.Context
	client *rabbitmqClient
	maxTry = 5
)

const (
	TopicNotif = "notif"
	TopicMail  = "mail"
)

var projectName string

type Config struct {
	ProjectName     string
	RabbitmqConnURL string
}

// Initialize ...
func Initialize(c context.Context, config *Config) {
	ctx = c
	if config == nil {
		return
	}

	projectName = config.ProjectName
	client = NewClient(config.RabbitmqConnURL)
	client.connect(time.Second)
}

func clearExchanges() error {
	ch, err := client.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	// Delete the exchange
	err = ch.ExchangeDelete(
		TopicNotif, // Exchange name
		false,      // IfUnused
		false,      // NoWait
	)
	if err != nil {
		return err
	}

	return nil
}

func (p *Store) SendWithContext(ctx context.Context, topic string, data interface{}, opts ...models.GetMQOption) error {
	// FIXME propagate context through rabbitmq messages
	return p.Send(topic, data, opts...)
}

func (p *Store) Send(exchange string, message interface{}, opts ...models.GetMQOption) error {
	switch exchange {
	case TopicNotif:
		tries := 0
		for {
			if client.IsReady == false {
				tries++
				if tries < maxTry {
					time.Sleep(time.Second)
					continue
				}

				return errors.New("RabbitMQ reconnection has exceeded max connection max tries, sending message failed.")
			}
			break
		}

		ex := string(exchange)

		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		body, err := json.Marshal(message)
		if err != nil {
			return err
		}

		ch, err := client.conn.Channel()
		if err != nil {
			return err
		}
		defer ch.Close()

		err = ch.ExchangeDeclare(
			ex,       // name
			"direct", // type
			true,     // durable
			false,    // auto-deleted
			false,    // internal
			false,    // no-wait
			nil,      // arguments
		)
		if err != nil {
			return err
		}

		err = ch.PublishWithContext(ctx,
			ex,          // exchange
			projectName, // routing key
			false,       // mandatory
			false,       // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        body,
			})
		if err != nil {
			return err
		}

	case TopicMail:
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		exchange := "mail"

		body, err := json.Marshal(message)
		if err != nil {
			return err
		}

		ch, err := client.conn.Channel()
		if err != nil {
			return err
		}
		defer ch.Close()

		err = ch.ExchangeDeclare(
			exchange, // name
			"fanout", // type
			true,     // durable
			false,    // auto-deleted
			false,    // internal
			false,    // no-wait
			nil,      // arguments
		)
		if err != nil {
			return err
		}

		err = ch.PublishWithContext(ctx,
			exchange, // exchange
			"",
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        body,
			})
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Store) ReceiveWithContext(ctx context.Context, topic string) (<-chan []byte, error) {
	// FIXME propagate context through rabbitmq messages
	return p.Receive(topic)
}

func (p *Store) Receive(topic string) (<-chan []byte, error) {
	s := strings.Split(topic, "-")
	if len(s) != 2 {
		return nil, errors.New("RabbitMQ Topic format incorrect: Should be of form 'Topic-Project', eg: notif-apen")
	}
	exchange, routing := s[0], s[1]

	for {
		if client.IsReady == false {
			time.Sleep(time.Second)
			continue
		}
		break
	}

	ch, err := client.conn.Channel()
	if err != nil {
		return nil, err
	}
	// defer ch.Close()

	err = ch.ExchangeDeclare(
		exchange, // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		fmt.Sprintf("%s-%s", exchange, routing), // name
		false,                                   // durable
		false,                                   // delete when unused
		false,                                   // exclusive
		false,                                   // no-wait
		nil,                                     // arguments
	)
	if err != nil {
		return nil, err
	}

	err = ch.QueueBind(
		q.Name,   // queue name
		routing,  // routing key
		exchange, // exchange
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return nil, err
	}

	byteCh := make(chan []byte)
	go func(dch chan<- []byte, src <-chan amqp.Delivery) {
		defer ch.Close()

		for d := range src {
			dch <- d.Body
		}
	}(byteCh, msgs)

	return byteCh, nil
}

// Finalize ...
func Finalize() {
	if client != nil {
		client.conn.Close()
	}
}
