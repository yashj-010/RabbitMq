package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	body, err := json.Marshal(val)
	if err != nil {
		return err
	}

	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
}

// SimpleQueueType represents whether a queue is durable or transient.
type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

// DeclareAndBind creates a channel, declares a queue with the appropriate
// durability/exclusivity/auto-delete properties for the given queueType,
// binds it to the given exchange with the given routing key, and returns
// the channel and queue.
func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	durable := queueType == SimpleQueueDurable
	autoDelete := queueType == SimpleQueueTransient
	exclusive := queueType == SimpleQueueTransient

	queue, err := ch.QueueDeclare(
		queueName,
		durable,
		autoDelete,
		exclusive,
		false, // noWait
		nil,   // args
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(
		queue.Name,
		key,
		exchange,
		false, // noWait
		nil,   // args
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return ch, queue, nil
}