package main

import (
	"adaptivesystemscourse/internal/shared"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const NumberOfPublishers = 1000
const PublishInterval = 100 // milliseconds
const StdDevPublishInterval = 10

func main() {
	wg := sync.WaitGroup{}

	//log.Println(" [*] Starting publisher[", id, "]")
	fmt.Printf("#Publishers: [%v]   Publish Interval: [%v] ms Rate: [%v] msg/s\n", NumberOfPublishers, PublishInterval, float64(NumberOfPublishers)*1.0/PublishInterval*1000.0)

	for i := 0; i < NumberOfPublishers; i++ {
		wg.Add(1)
		go StartPublisher(i, &wg)
	}
	wg.Wait()
}

func StartPublisher(id int, wg *sync.WaitGroup) {
	defer wg.Done()

	// 1. Connect to RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// 2. Open a channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open channel: %v", err)
	}
	defer ch.Close()

	// 3. Declare the same queue used by the subscriber-prefetch
	q, err := ch.QueueDeclare(
		"test-queue",
		true, // durable (match consumer version)
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	// 4. Publish messages periodically
	rng := rand.New(rand.NewSource(42))
	for i := 0; ; i++ {
		body := fmt.Sprintf("Message %d", i)

		err = ch.Publish(
			"",     // exchange (default)
			q.Name, // routing key = queue name
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			},
		)

		if err != nil {
			log.Printf("Publish error: %v", err)
			continue
		}

		//log.Printf("Sent: %s", body)
		t := time.Duration(shared.SampleNormal(rng, PublishInterval, StdDevPublishInterval))
		time.Sleep(t * time.Millisecond)
	}
}
