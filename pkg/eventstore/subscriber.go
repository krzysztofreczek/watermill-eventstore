package eventstore

import (
	"context"
	"encoding/json"
	"log"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/jdextraze/go-gesclient/client"
)

type Subscriber struct {
	c   client.Connection
	sub client.PersistentSubscription
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
	return s.out, nil
}

func (s *Subscriber) Close() error {
	if s.out != nil {
		close(s.out)
	}

	if s.sub == nil {
		return nil
	}

	// return s.sub.Close()

	return nil
}

func (s *Subscriber) SubscribeInitialize(
	topic string,
) error {
	// TODO!!! handle group name
	task, err := s.c.CreatePersistentSubscriptionAsync(topic, "groupName", client.DefaultPersistentSubscriptionSettings, nil)
	if err != nil {
		// TODO!!! wrap it
		return err
	}

	res := task.Result()
	if res != nil {
		createResult := res.(*client.PersistentSubscriptionCreateResult)
		log.Printf("persistent subscription created with status: %+v\n", createResult)
	}

	task, err = s.c.ConnectToPersistentSubscriptionAsync(topic, "groupName", s.handleEventAppeared, s.handleSubscriptionDropped, nil, 10, true)
	if err != nil {
		// TODO!!! wrap it
		return err
	}

	sub := task.Result().(client.PersistentSubscription)
	log.Printf("SubscribeToStream result: %+v", sub)

	s.sub = sub

	return nil
}

func (s *Subscriber) handleEventAppeared(_ client.PersistentSubscription, e *client.ResolvedEvent) error {
	m := message.NewMessage(
		e.Event().EventId().String(),
		e.Event().Data(),
	)

	var metadata map[string]string
	err := json.Unmarshal(e.Event().Metadata(), &metadata)
	if err != nil {
		// TODO!!! wrap it
		return err
	}
	m.Metadata = metadata

	s.out <- m
	// TODO!!! remove it
	log.Printf("received event of UUID %s\n", e.Event().EventId().String())
	return nil
}

func (s *Subscriber) handleSubscriptionDropped(_ client.PersistentSubscription, r client.SubscriptionDropReason, err error) error {
	return nil
}
