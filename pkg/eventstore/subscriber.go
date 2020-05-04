package eventstore

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/jdextraze/go-gesclient/client"
)

type Subscriber struct {
	c   client.Connection
	sub client.EventStoreSubscription
	out chan *message.Message
}

func NewSubscriber(
	c client.Connection,
) (*Subscriber, error) {
	return &Subscriber{
		c:   c,
		out: make(chan *message.Message),
	}, nil
}

func (s *Subscriber) Subscribe(
	ctx context.Context,
	topic string,
) (<-chan *message.Message, error) {
	task, err := s.c.SubscribeToStreamAsync(topic, true, s.handleEventAppeared, s.handleSubscriptionDropped, nil)
	if err != nil {
		// TODO!!! wrap it
		return nil, err
	}

	err = task.Error()
	if err != nil {
		// TODO!!! wrap it
		return nil, err
	}

	s.sub = task.Result().(client.EventStoreSubscription)

	return nil, nil
}

func (s *Subscriber) Close() error {
	// TODO!!! wrap it
	return s.sub.Close()
}

func (s *Subscriber) SubscribeInitialize(
	topic string,
) error {
	return nil
}

func (s *Subscriber) handleEventAppeared(_ client.EventStoreSubscription, e *client.ResolvedEvent) error {
	s.out <- &message.Message{
		UUID:    e.Event().EventId().String(),
		Payload: e.Event().Data(),
	}
	return nil
}

func (s *Subscriber) handleSubscriptionDropped(_ client.EventStoreSubscription, r client.SubscriptionDropReason, err error) error {
	return nil
}
