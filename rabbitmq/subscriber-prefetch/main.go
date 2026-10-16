package main

import (
	basicpid "adaptivesystemscourse/internal/control/pid"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const MonitorInterval = time.Duration(5) * time.Second // seconds
const PrefetchCount = 5
const GoalRate = 3000
const SampleSize = 10
const BacklogGoal = 5000
const Kp = 5.97e-5
const Ki = 8.78e-6
const Kd = 0.0
const Direction = -1.0
const DeltaMinPC = 1
const DeltaMaxPC = 15
const ProcessingTime = 100 * time.Millisecond

func main() {
	//OpenLoop()
	ClosedLoop()
}

func ClosedLoop() {
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

	// 3. Declare a queue (idempotent)
	q, err := ch.QueueDeclare(
		"test-queue", // name
		false,        // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	// 4. Start consuming
	msgs, err := ch.Consume(
		q.Name,
		"",    // consumer tag
		false, // auto-ack
		false, // exclusive
		false, // no-local (not supported)
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		log.Fatalf("Failed to register consumer: %v", err)
	}

	log.Println(" [*] Waiting for messages...")

	// 5. Receive messages
	forever := make(chan struct{})

	// Configure prefetch count
	err = ch.Qos(
		PrefetchCount, // prefetch count
		0,             // prefetch size (almost always 0)
		true,          // global flag
	)
	if err != nil {
		log.Fatalf("QoS error: %v", err)
	}

	// start Monitor
	ticker := time.NewTicker(MonitorInterval)
	nMsgs := 0

	// Bang-Bang
	//controller := basic.Controller{}
	//controller.Initialise(1, 1, 100000)

	// HPA
	//controller := hpa.Controller{}
	//controller.Initialise(1, 1, 1000, PrefetchCount)

	// PID
	controller := basicpid.Controller{}
	//controller.Initialise(1, 1, 1000, 0.000305, 0.000065, 0.0)
	controller.Initialise(Direction, DeltaMinPC, DeltaMaxPC, Kp, Ki, Kd)
	u := float64(PrefetchCount)

	client := &http.Client{
		Timeout: 5 * time.Second,
	}
	for {
		select {
		case <-ticker.C:
			r := float64(nMsgs) / float64(MonitorInterval.Seconds())
			backlog, err := GetRabbitMQBacklog(
				context.Background(),
				client,
				"http://localhost:15672", // Management HTTP API
				"/",
				"test-queue",
				"guest",
				"guest",
			)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%v;%v;%v;[Rate=%v] \n", int(math.Round(u)), BacklogGoal, backlog.Ready+backlog.Unacknowledged, r)
			u = controller.Update(BacklogGoal, float64(backlog.Total+backlog.Unacknowledged))
			//u = myController(float64(BacklogGoal), float64(backlog.Total+backlog.Unacknowledged), u)
			//fmt.Printf("%v;%v;%v \n", int(u), r, nMsgs)

			err = ch.Qos(
				int(math.Round(u)), // prefetch count
				0,                  // prefetch size (almost always 0)
				true,               // global flag
			)
			if err != nil {
				log.Fatalf("QoS error: %v", err)
			}
			nMsgs = 0
			ticker.Reset(MonitorInterval)
		default:
			d := <-msgs
			d.Ack(false)
			//log.Printf("Received: %s", msg.Body)
			nMsgs++
		}
	}
	<-forever
}

func OpenLoop() {
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

	// 3. Declare a queue (idempotent)
	q, err := ch.QueueDeclare(
		"test-queue", // name
		false,        // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	// 4. Start consuming
	msgs, err := ch.Consume(
		q.Name,
		"",    // consumer tag
		false, // auto-ack
		false, // exclusive
		false, // no-local (not supported)
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		log.Fatalf("Failed to register consumer: %v", err)
	}

	log.Println(" [*] Waiting for messages...")

	// 5. Receive messages
	forever := make(chan struct{})

	// Configure prefetch count
	err = ch.Qos(
		PrefetchCount, // prefetch count
		0,             // prefetch size (almost always 0)
		true,          // global flag
	)
	if err != nil {
		log.Fatalf("QoS error: %v", err)
	}

	// start Monitor
	ticker := time.NewTicker(MonitorInterval)
	nMsgs := 0

	client := &http.Client{
		Timeout: 5 * time.Second,
	}
	pc := PrefetchCount
	nSamples := 0
	previousBacklog := int64(0)
	for {
		select {
		case <-ticker.C:
			r := float64(nMsgs) / float64(MonitorInterval.Seconds())
			nSamples++
			nMsgs = 0
			backlog, err := GetRabbitMQBacklog(
				context.Background(),
				client,
				"http://localhost:15672", // Management HTTP API
				"/",
				"test-queue",
				"guest",
				"guest",
			)
			if err != nil {
				log.Fatal(err)
			}

			//fmt.Printf("%v; %v;%v \n", time.Now(), pc, backlog.Ready-(previousBacklog+backlog.Unacknowledged))
			fmt.Printf("%v; %v;%v \n", pc, backlog.Ready-(previousBacklog+backlog.Unacknowledged), r)
			previousBacklog = backlog.Ready + backlog.Unacknowledged

			if nSamples >= SampleSize {
				nSamples = 0
				pc++
				err = ch.Qos(
					int(pc), // prefetch count
					0,       // prefetch size (almost always 0)
					true,    // global flag
				)
				if err != nil {
					log.Fatalf("QoS error: %v", err)
				}
			}
			ticker.Reset(MonitorInterval)
		default:
			d := <-msgs
			d.Ack(false)
			//log.Printf("Received: %s", msg.Body)
			nMsgs++
		}
	}
	<-forever
}

type QueueBacklog struct {
	Ready          int64
	Unacknowledged int64
	Total          int64
	Consumers      int
}

type queueAPIResponse struct {
	Messages               int64 `json:"messages"`
	MessagesReady          int64 `json:"messages_ready"`
	MessagesUnacknowledged int64 `json:"messages_unacknowledged"`
	Consumers              int   `json:"consumers"`
}

// GetRabbitMQBacklog obtains the current backlog of a RabbitMQ queue.
//
// managementURL example: "http://localhost:15672"
// vhost example:        "/"
// queueName example:    "my-queue"
func GetRabbitMQBacklog(
	ctx context.Context,
	httpClient *http.Client,
	managementURL string,
	vhost string,
	queueName string,
	username string,
	password string,
) (QueueBacklog, error) {
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 5 * time.Second,
		}
	}

	endpoint := fmt.Sprintf(
		"%s/api/queues/%s/%s",
		strings.TrimRight(managementURL, "/"),
		url.PathEscape(vhost),
		url.PathEscape(queueName),
	)

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		endpoint,
		nil,
	)
	if err != nil {
		return QueueBacklog{}, fmt.Errorf(
			"create RabbitMQ request: %w",
			err,
		)
	}

	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(username, password)

	resp, err := httpClient.Do(req)
	if err != nil {
		return QueueBacklog{}, fmt.Errorf(
			"request RabbitMQ queue metrics: %w",
			err,
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

		return QueueBacklog{}, fmt.Errorf(
			"RabbitMQ Management API returned %s: %s",
			resp.Status,
			strings.TrimSpace(string(body)),
		)
	}

	var result queueAPIResponse

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return QueueBacklog{}, fmt.Errorf(
			"decode RabbitMQ response: %w",
			err,
		)
	}

	return QueueBacklog{
		Ready:          result.MessagesReady,
		Unacknowledged: result.MessagesUnacknowledged,
		Total:          result.Messages,
		Consumers:      result.Consumers,
	}, nil
}

func myController(goal, measured, dr float64) float64 {
	r := dr
	if measured <= goal {
		r -= 1
	}
	if measured > goal {
		r += 1
	}

	if r < DeltaMinPC {
		r = DeltaMinPC
	}
	if r > DeltaMaxPC {
		r = DeltaMaxPC
	}
	return r
}
