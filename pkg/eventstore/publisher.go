package eventstore

import (
	"encoding/json"
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
	// TODO!!! handle nil conn
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

		// TODO!!! think of better serialization
		messageMetadata, err := json.Marshal(m.Metadata)
		if err != nil {
			// TODO!!! wrap it
			return err
		}

		messageUUID, err := uuid.FromString(m.UUID)
		if err != nil {
			// TODO!!! wrap it
			return err
		}

		// TODO!!! what about type
		evt := client.NewEventData(messageUUID, "event-type", false, messagePayload, messageMetadata)

		// TODO!!! what about credentials
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

		_ = task.Result().(*client.WriteResult)
		// TODO!!! remove it
		log.Printf("sent event of UUID %s\n", messageUUID)
	}

	return nil
}

type TestEvent struct{}

func (p *Publisher) Close() error {
	return nil
}
