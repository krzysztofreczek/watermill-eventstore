package eventstore_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-eventstore/pkg/eventstore"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/stretchr/testify/require"
)

var (
	logger = watermill.NewStdLogger(true, true)
)

func newPubSub(
	t *testing.T,
) (message.Publisher, message.Subscriber) {
	publisher, err := eventstore.NewPublisher()
	require.NoError(t, err)

	subscriber, err := eventstore.NewSubscriber()
	require.NoError(t, err)

	return publisher, subscriber
}

func newPubSubWithConsumerGroup(
	t *testing.T,
	consumerGroup string,
) (message.Publisher, message.Subscriber) {
	publisher, err := eventstore.NewPublisher()
	require.NoError(t, err)

	subscriber, err := eventstore.NewSubscriber()
	require.NoError(t, err)

	return publisher, subscriber
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

	tests.TestPubSub(
		t,
		features,
		newPubSub,
		newPubSubWithConsumerGroup,
	)
}
