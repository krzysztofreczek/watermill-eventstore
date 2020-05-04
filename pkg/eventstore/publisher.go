package eventstore

import (
	"log"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/jdextraze/go-gesclient/client"
	uuid "github.com/satori/go.uuid"
)

type Publisher struct {
	c client.Connection
}

func NewPublisher(
	c client.Connection,
) (*Publisher, error) {
	return &Publisher{
		c: c,
	}, nil
}

func (p *Publisher) Publish(
	topic string,
	messages ...*message.Message,
) error {
	for _, m := range messages {
		messagePayload := m.Payload
		messageUUID, err := uuid.FromString(m.UUID)
		if err != nil {

		}

		// TODO!!! what about topic
		evt := client.NewEventData(messageUUID, topic, false, messagePayload, nil)

		task, err := p.c.AppendToStreamAsync(topic, client.ExpectedVersion_Any, []*client.EventData{evt}, nil)
		if err != nil {
			// TODO!!! wrap it
			return err
		}

		err = task.Error()
		if err != nil {
			// TODO!!! wrap it
			return err
		}

		result := task.Result().(*client.WriteResult)
		log.Printf("<- %+v", result)
	}

	return nil
}

type TestEvent struct{}

func (p *Publisher) Close() error {
	return nil
}
