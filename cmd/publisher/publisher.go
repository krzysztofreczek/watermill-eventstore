package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/ThreeDotsLabs/watermill-eventstore/pkg/eventstore"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/jdextraze/go-gesclient/client"
	"github.com/jdextraze/go-gesclient/flags"
	uuid "github.com/satori/go.uuid"
)

const topic = "test-topic"

func main() {
	flags.Init(flag.CommandLine)

	c, err := flags.CreateConnection("test-connection-pub")
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

	publisher, err := eventstore.NewPublisher(c)
	if err != nil {
		panic(err)
	}

	defer func() {
		err = publisher.Close()
		if err != nil {
			panic(err)
		}
	}()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)

	ticker := time.NewTicker(100 * time.Millisecond)

	for {
		select {
		case <-ch:
			c.Close()
			time.Sleep(10 * time.Millisecond)
			return
		case <-ticker.C:
		}

		id, err := uuid.NewV4()
		if err != nil {
			panic(err)
		}

		payloadString := "id:" + id.String()
		payload := []byte(payloadString)

		m := message.NewMessage(id.String(), payload)
		err = publisher.Publish(topic, m)
		if err != nil {
			panic(err)
		}

		time.Sleep(1000 * time.Millisecond)
	}
}
