package eventstore_test

import (
	"flag"
	"log"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-eventstore/pkg/eventstore"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/jdextraze/go-gesclient/client"
	"github.com/jdextraze/go-gesclient/flags"
	"github.com/stretchr/testify/require"
)

var (
	logger = watermill.NewStdLogger(true, true)
)

func newPubSub(
	t *testing.T,
) (message.Publisher, message.Subscriber) {
	c, err := newEventStoreConnection()
	require.NoError(t, err)

	publisher, err := eventstore.NewPublisher(c)
	require.NoError(t, err)

	subscriber, err := eventstore.NewSubscriber(c)
	require.NoError(t, err)

	return publisher, subscriber
}

func newPubSubWithConsumerGroup(
	t *testing.T,
	consumerGroup string,
) (message.Publisher, message.Subscriber) {
	c, err := newEventStoreConnection()
	require.NoError(t, err)

	publisher, err := eventstore.NewPublisher(c)
	require.NoError(t, err)

	subscriber, err := eventstore.NewSubscriber(c)
	require.NoError(t, err)

	return publisher, subscriber
}

func newEventStoreConnection() (client.Connection, error) {
	// TODO!!! initialize it proprely
	fs := &flag.FlagSet{}
	flags.Init(fs)

	c, err := flags.CreateConnection("test-connection")
	if err != nil {
		// TODO!!! wrap it
		return nil, err
	}

	c.Connected().Add(func(evt client.Event) error { log.Printf("Connected: %+v", evt); return nil })
	c.Disconnected().Add(func(evt client.Event) error { log.Printf("Disconnected: %+v", evt); return nil })
	c.Reconnecting().Add(func(evt client.Event) error { log.Printf("Reconnecting: %+v", evt); return nil })
	c.Closed().Add(func(evt client.Event) error { log.Fatalf("Connection closed: %+v", evt); return nil })
	c.ErrorOccurred().Add(func(evt client.Event) error { log.Printf("Error: %+v", evt); return nil })
	c.AuthenticationFailed().Add(func(evt client.Event) error { log.Printf("Auth failed: %+v", evt); return nil })

	err = c.ConnectAsync().Wait()
	if err != nil {
		// TODO!!! wrap it
		return nil, err
	}

	return c, nil
}

func TestEventStorePublishSubscribe(
	t *testing.T,
) {
	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: true,
		GuaranteedOrder:     true,
		Persistent:          true,
	}

	tests.TestPublishSubscribe(
		t,
		tests.TestContext{
			TestID:   "TestPublishSubscribe",
			Features: features,
		},
		newPubSub,
	)

	// tests.TestConcurrentSubscribe(
	// 	t,
	// 	tests.TestContext{
	// 		TestID:   "TestConcurrentSubscribe",
	// 		Features: features,
	// 	},
	// 	newPubSub,
	// )

	// tests.TestPubSub(
	// 	t,
	// 	features,
	// 	newPubSub,
	// 	newPubSubWithConsumerGroup,
	// )
}
