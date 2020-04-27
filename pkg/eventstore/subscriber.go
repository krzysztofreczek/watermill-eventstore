package eventstore

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
)

type Subscriber struct {
}

func NewSubscriber() (*Subscriber, error) {
	return &Subscriber{}, nil
}

func (s *Subscriber) Subscribe(
	ctx context.Context,
	topic string,
) (o <-chan *message.Message, err error) {
	return nil, nil
}

func (s *Subscriber) Close() error {
	return nil
}

func (s *Subscriber) SubscribeInitialize(
	topic string,
) error {
	return nil
}
