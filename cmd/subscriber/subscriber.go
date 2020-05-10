package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/ThreeDotsLabs/watermill-eventstore/pkg/eventstore"
	"github.com/jdextraze/go-gesclient/client"
	"github.com/jdextraze/go-gesclient/flags"
)

const topic = "test-topic"

func main() {
	flags.Init(flag.CommandLine)

	c, err := flags.CreateConnection("test-connection-sub")
	if err != nil {
		panic(err)
	}

	c.Connected().Add(func(evt client.Event) error { log.Printf("Connected: %+v", evt); return nil })
	c.Disconnected().Add(func(evt client.Event) error { log.Printf("Disconnected: %+v", evt); return nil })
	c.Reconnecting().Add(func(evt client.Event) error { log.Printf("Reconnecting: %+v", evt); return nil })
	c.Closed().Add(func(evt client.Event) error { log.Fatalf("Connection closed: %+v", evt); return nil })
	c.ErrorOccurred().Add(func(evt client.Event) error { log.Printf("Error: %+v", evt); return nil })
	c.AuthenticationFailed().Add(func(evt client.Event) error { log.Printf("Auth failed: %+v", evt); return nil })

	err = c.ConnectAsync().Wait()
	if err != nil {
		panic(err)
	}

	for i := 0; i < 3; i++ {
		go runSubscriberAsync(i, c)
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch
}

func runSubscriberAsync(i int, c client.Connection) {
	subscriber, err := eventstore.NewSubscriber(c)
	if err != nil {
		panic(err)
	}

	defer func() {
		err = subscriber.Close()
		if err != nil {
			panic(err)
		}
	}()

	err = subscriber.SubscribeInitialize(topic)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	m, err := subscriber.Subscribe(ctx, topic)
	if err != nil {
		panic(err)
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)

	ticker := time.NewTicker(100 * time.Millisecond)

	for {
		select {
		case <-ch:
			c.Close()
			time.Sleep(10 * time.Millisecond)
			return
		case message := <-m:
			println(i, ": message received: ", message.UUID)
		case <-ticker.C:
		}
	}
}
