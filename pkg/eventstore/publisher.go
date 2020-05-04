package eventstore

import (
	"github.com/ThreeDotsLabs/watermill/message"
)

type Publisher struct {
}

func NewPublisher() (*Publisher, error) {
	return &Publisher{}, nil
}

func (p *Publisher) Publish(
	topic string,
	messages ...*message.Message,
) error {
	return nil
}

type TestEvent struct{}

func (p *Publisher) Close() error {
	return nil
}
