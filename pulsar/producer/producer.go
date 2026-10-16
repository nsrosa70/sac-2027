package main

import (
	"adaptivesystemscourse/internal/shared"
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	serviceURL := flag.String("url", "pulsar://localhost:6650", "Pulsar service URL")
	topic := flag.String("topic", "persistent://public/default/demo", "topic to publish to")
	//count := flag.Int("count", 10, "number of messages to publish")
	//interval := flag.Duration("interval", time.Second, "delay between messages")
	flag.Parse()
	rng := rand.New(rand.NewSource(42))
	nProducers := 1000
	mean := 100.0 // milliseconds
	std := 10.0   // millisecond

	for i := 0; i < nProducers; i++ {
		go func() {
			client, err := pulsar.NewClient(pulsar.ClientOptions{URL: *serviceURL})
			if err != nil {
				log.Fatalf("create client: %v", err)
			}
			defer client.Close()

			producer, err := client.CreateProducer(pulsar.ProducerOptions{Topic: *topic})
			if err != nil {
				log.Fatalf("create producer: %v", err)
			}
			defer producer.Close()

			//fmt.Printf("Producer %v created at %v msg/s\n", i, 1000/mean)
			ctx := context.Background()
			for {

				payload := fmt.Sprintf("fixed-message-payload")
				//messageID, err := producer.Send(ctx, &pulsar.ProducerMessage{
				_, err := producer.Send(ctx, &pulsar.ProducerMessage{
					Payload: []byte(payload),
					//EventTime: time.Now(),
					Properties: map[string]string{
						"sent-at-ns": time.Now().UTC().Format(time.RFC3339Nano),
					},
				})
				if err != nil {
					log.Fatalf("send %q: %v", payload, err)
				}

				// configure sleep time
				t := time.Duration(shared.SampleNormal(rng, mean, std))
				time.Sleep(t * time.Millisecond)
			}
		}()
	}
	time.Sleep(5 * time.Second)
	fmt.Printf("Pulsar producers: %d\n  Total rate: %v msg/s", nProducers, float64(nProducers)*1.0/mean*1000)
	fmt.Scanln()
}
