package main

import (
	basicpid "adaptivesystemscourse/internal/control/pid"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	amqp "github.com/Azure/go-amqp"
)

const MonitorInterval = time.Duration(5) * time.Second // seconds

const Kp = 0.000200 // 30/09/2026 - Aggressive
const Ki = 0.000020 // 30/09/2026 - Agressive
const Kd = 0.0
const Direction = 1.0
const MinLC = 1
const MaxLC = 63

const SampleSize = 150
const RateGoal = 4600 // msg/s
const InitialLinkedCredit = 5

const QueueName = "test-queue"

var currentReceiver = int32(InitialLinkedCredit)
var receivers = [MaxLC]amqp.Receiver{}
var ratesGoal = [6]int32{4600, 9500, 2600, 8110, 6680, 4600}

const Native = 1
const PI = 2
const ControllerType = Native
const SamplesPerLevel = 150

func main() {
	//RunOpenLoop()
	RunClosedLoop()
	//RunClosedLoopMulti()
}

func RunOpenLoop() {

	// create context
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// create queue
	queueAddress := "/queues/" + url.PathEscape(QueueName)
	if err := EnsureQueue(
		ctx,
		"http://localhost:15672",
		"guest",
		"guest",
		"/",
		QueueName,
	); err != nil {
		log.Fatalf("declare queue: %v", err)
	}

	// create connection
	connection, err := amqp.Dial(
		ctx,
		"amqp://localhost:5672",
		&amqp.ConnOptions{
			SASLType: amqp.SASLTypePlain("guest", "guest"),
		},
	)
	if err != nil {
		log.Fatalf("connect to RabbitMQ: %v", err)
	}
	defer connection.Close()

	// create pool of receivers
	receivers := [MaxLC]amqp.Receiver{}
	for i := 0; i < MaxLC; i++ {
		// create session
		session, err := connection.NewSession(ctx, nil)
		if err != nil {
			log.Fatalf("create session: %v", err)
		}
		defer session.Close(context.Background())

		temp, err := session.NewReceiver(
			ctx,
			queueAddress,
			&amqp.ReceiverOptions{
				Credit: int32(i + 1),
			},
		)
		if err != nil {
			log.Fatalf("create receiver: %v", err)
		}
		receivers[i] = *temp
		defer receivers[i].Close(context.Background())
	}

	log.Printf("Waiting for messages...\n")

	// initialisations
	ticker := time.NewTicker(MonitorInterval)
	nMsgs := 0
	nSamples := 0
	u := InitialLinkedCredit
	currentReceiver := u - 1

	// loop
	for {
		select {
		case <-ticker.C:
			// compute receiveiving rate
			r := float64(nMsgs) / float64(MonitorInterval.Seconds())

			fmt.Printf("%v;%v;%v\n", time.Now(), int32(u), r)

			if nSamples >= SampleSize-1 {
				nSamples = 0
				u++
				currentReceiver = u - 1

			} else {
				nSamples++
			}

			// reinitialise variables
			nMsgs = 0
			ticker.Reset(MonitorInterval)
		default:
		}
		// receive message
		message, err := receivers[currentReceiver].Receive(ctx, nil)
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				log.Print("consumer stopped")
				return
			}
			log.Fatalf("receive message: %v", err)
		}

		// process message
		//log.Printf("received: %s", string(message.GetData()))
		//time.Sleep(ProcessingTime)

		// ack message
		if err := receivers[currentReceiver].AcceptMessage(ctx, message); err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return
			}
			log.Fatalf("accept message: %v", err)
		}
		nMsgs++
	}
}

func RunClosedLoop() {

	// create context
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// create queue
	queueAddress := "/queues/" + url.PathEscape(QueueName)
	if err := EnsureQueue(
		ctx,
		"http://localhost:15672",
		"guest",
		"guest",
		"/",
		QueueName,
	); err != nil {
		log.Fatalf("declare queue: %v", err)
	}

	// create connection
	connection, err := amqp.Dial(
		ctx,
		"amqp://localhost:5672",
		&amqp.ConnOptions{
			SASLType: amqp.SASLTypePlain("guest", "guest"),
		},
	)
	if err != nil {
		log.Fatalf("connect to RabbitMQ: %v", err)
	}
	defer connection.Close()

	// create pool of receivers
	receivers := [MaxLC]amqp.Receiver{}
	for i := 0; i < MaxLC; i++ {
		// create session
		session, err := connection.NewSession(ctx, nil)
		if err != nil {
			log.Fatalf("create session: %v", err)
		}
		defer session.Close(context.Background())

		temp, err := session.NewReceiver(
			ctx,
			queueAddress,
			&amqp.ReceiverOptions{
				Credit: int32(i + 1),
			},
		)
		if err != nil {
			log.Fatalf("create receiver: %v", err)
		}
		receivers[i] = *temp
		defer receivers[i].Close(context.Background())
	}

	log.Printf("Waiting for messages...\n")

	// controller setup
	pid := basicpid.Controller{}
	pid.Initialise(Direction, MinLC, MaxLC, Kp, Ki, Kd)
	//controller := hpa.Controller{}
	//controller.Initialise(MinLC, MaxLC)
	u := float64(InitialLinkedCredit)

	// initialisations
	ticker := time.NewTicker(MonitorInterval)
	nMsgs := 0
	nSamples := 0
	//receiver.IssueCredit(1)

	//client := &http.Client{Timeout: 5 * time.Second}

	// loop
	//remainCredits := 1
	fmt.Println("************* Begin of Experiment ************* ")
	for {
		select {
		case <-ticker.C:
			// compute receiving rate
			r := float64(nMsgs) / float64(MonitorInterval.Seconds())

			// get backlog

			/*
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
			*/
			//fmt.Printf("%v;%v;%v;%v;%v\n", time.Now(), nSamples+1, int32(u), RateGoal, r)
			fmt.Printf("%v;%v;%v;%v\n", nSamples+1, int32(u), RateGoal, int(r))

			//fmt.Printf("[Link-credit=%v;BacklogGoal=%v;=CurrentBacklog=%v;ReceivingRate=%v s\n", u, BacklogGoal, backlog.Ready+backlog.Unacknowledged, r)

			// invoke controller
			if ControllerType == PI {
				u = pid.Update(RateGoal, r)
				currentReceiver = int32(u) - 1
			}
			if ControllerType == Native {
				u = InitialLinkedCredit // TODO
				currentReceiver = int32(u) - 1
			}
			// update & check samples
			nSamples++
			if nSamples >= SampleSize {
				fmt.Println("************* End of Experiment ************* ")
				os.Exit(0)
			}

			// reinitialise variables
			nMsgs = 0
			ticker.Reset(MonitorInterval)
		default:
		}

		// receive message
		message, err := receivers[currentReceiver].Receive(ctx, nil)
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				log.Print("consumer stopped")
				return
			}
			log.Fatalf("receive message: %v", err)
		}

		// process message
		//log.Printf("received: %s", string(message.GetData()))
		//time.Sleep(ProcessingTime)

		// ack message
		if err := receivers[currentReceiver].AcceptMessage(ctx, message); err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return
			}
			log.Fatalf("accept message: %v", err)
		}
		nMsgs++
	}
}

func RunClosedLoopMulti() {

	// create context
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// create queue
	queueAddress := "/queues/" + url.PathEscape(QueueName)
	if err := EnsureQueue(
		ctx,
		"http://localhost:15672",
		"guest",
		"guest",
		"/",
		QueueName,
	); err != nil {
		log.Fatalf("declare queue: %v", err)
	}

	// create connection
	connection, err := amqp.Dial(
		ctx,
		"amqp://localhost:5672",
		&amqp.ConnOptions{
			SASLType: amqp.SASLTypePlain("guest", "guest"),
		},
	)
	if err != nil {
		log.Fatalf("connect to RabbitMQ: %v", err)
	}
	defer connection.Close()

	// create pool of receivers
	receivers := [MaxLC]amqp.Receiver{}
	for i := 0; i < MaxLC; i++ {
		// create session
		session, err := connection.NewSession(ctx, nil)
		if err != nil {
			log.Fatalf("create session: %v", err)
		}
		defer session.Close(context.Background())

		temp, err := session.NewReceiver(
			ctx,
			queueAddress,
			&amqp.ReceiverOptions{
				Credit: int32(i + 1),
			},
		)
		if err != nil {
			log.Fatalf("create receiver: %v", err)
		}
		receivers[i] = *temp
		defer receivers[i].Close(context.Background())
	}

	log.Printf("Waiting for messages...\n")

	// controller setup
	pid := basicpid.Controller{}
	pid.Initialise(Direction, MinLC, MaxLC, Kp, Ki, Kd)
	//controller := hpa.Controller{}
	//controller.Initialise(MinLC, MaxLC)
	u := float64(InitialLinkedCredit)

	// initialisations
	ticker := time.NewTicker(MonitorInterval)
	nMsgs := 0
	nSamples := 0
	nSamplesPerLevel := 0
	contLevels := 0
	//receiver.IssueCredit(1)

	//client := &http.Client{Timeout: 5 * time.Second}

	// loop
	//remainCredits := 1
	fmt.Println("************* Begin of Experiment ************* ")
	for {
		select {
		case <-ticker.C:
			// compute receiving rate
			r := float64(nMsgs) / float64(MonitorInterval.Seconds())

			// get backlog

			/*
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
			*/
			//fmt.Printf("%v;%v;%v;%v;%v\n", time.Now(), nSamples+1, int32(u), RateGoal, r)
			fmt.Printf("%v;%v;%v;%v\n", nSamples+1, int32(u), ratesGoal[contLevels], int(r))

			//fmt.Printf("[Link-credit=%v;BacklogGoal=%v;=CurrentBacklog=%v;ReceivingRate=%v s\n", u, BacklogGoal, backlog.Ready+backlog.Unacknowledged, r)

			// invoke controller
			if ControllerType == PI {
				u = pid.Update(float64(ratesGoal[contLevels]), r)
				currentReceiver = int32(u) - 1
			}
			if ControllerType == Native {
				u = InitialLinkedCredit
			}

			// update & check samples
			nMsgs = 0
			nSamples++
			nSamplesPerLevel++

			if nSamplesPerLevel >= SamplesPerLevel {
				nSamplesPerLevel = 0
				contLevels++
			}

			if contLevels >= len(ratesGoal) {
				fmt.Println("****** End of Experiment ******")
				os.Exit(1)
			}

			// reinitialise variables
			ticker.Reset(MonitorInterval)
		default:
		}

		// receive message
		message, err := receivers[currentReceiver].Receive(ctx, nil)
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				log.Print("consumer stopped")
				return
			}
			log.Fatalf("receive message: %v", err)
		}

		// process message
		//log.Printf("received: %s", string(message.GetData()))
		//time.Sleep(ProcessingTime)

		// ack message
		if err := receivers[currentReceiver].AcceptMessage(ctx, message); err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return
			}
			log.Fatalf("accept message: %v", err)
		}
		nMsgs++
	}
}

func RunClosedLoop3() error {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	queueAddress := "/queues/" + url.PathEscape(QueueName)

	if err := EnsureQueue(
		ctx,
		"http://localhost:15672",
		"guest",
		"guest",
		"/",
		QueueName,
	); err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}

	connection, err := amqp.Dial(
		ctx,
		"amqp://localhost:5672",
		&amqp.ConnOptions{
			SASLType: amqp.SASLTypePlain("guest", "guest"),
		},
	)
	if err != nil {
		return fmt.Errorf("connect to RabbitMQ: %w", err)
	}
	defer connection.Close()

	session, err := connection.NewSession(ctx, nil)
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	defer session.Close(context.Background())

	// Credit -1 enables manual link-credit management.
	receiver, err := session.NewReceiver(
		ctx,
		queueAddress,
		&amqp.ReceiverOptions{
			Credit: -1,
		},
	)
	if err != nil {
		return fmt.Errorf("create receiver: %w", err)
	}
	defer receiver.Close(context.Background())

	var received atomic.Uint64
	var desiredRate atomic.Uint64
	var outstandingCredit atomic.Int64

	setRate := func(rate float64) {
		rate = math.Max(float64(MinLC), rate)
		rate = math.Min(float64(MaxLC), rate)
		desiredRate.Store(math.Float64bits(rate))
	}

	getRate := func() float64 {
		return math.Float64frombits(desiredRate.Load())
	}

	setRate(float64(InitialLinkedCredit))

	receiveErrors := make(chan error, 1)

	go func() {
		for {
			message, err := receiver.Receive(ctx, nil)
			if err != nil {
				if ctx.Err() != nil {
					receiveErrors <- nil
				} else {
					receiveErrors <- fmt.Errorf("receive message: %w", err)
				}
				return
			}

			// One granted credit was consumed by this delivery.
			outstandingCredit.Add(-1)

			// Process the message here.
			// time.Sleep(ProcessingTime)

			if err := receiver.AcceptMessage(ctx, message); err != nil {
				if ctx.Err() != nil {
					receiveErrors <- nil
				} else {
					receiveErrors <- fmt.Errorf("accept message: %w", err)
				}
				return
			}
			received.Add(1)
		}
	}()

	pid := basicpid.Controller{}
	pid.Initialise(Direction, MinLC, MaxLC, Kp, Ki, Kd)

	monitorTicker := time.NewTicker(MonitorInterval)
	defer monitorTicker.Stop()

	// Credit is issued in smaller batches so that it approximates a rate.
	const creditPeriod = 5 * time.Second
	const maxOutstandingCredit int64 = 1000

	creditTicker := time.NewTicker(creditPeriod)
	defer creditTicker.Stop()

	var creditRemainder float64
	nSamples := 0

	log.Print("waiting for messages")

	for {
		select {
		case <-ctx.Done():
			return nil

		case err := <-receiveErrors:
			return err

		case <-creditTicker.C:
			rate := getRate()

			creditValue :=
				rate*creditPeriod.Seconds() + creditRemainder

			credit := int64(math.Floor(creditValue))
			creditRemainder = creditValue - float64(credit)

			// Do not allow credits to accumulate indefinitely when
			// the queue is empty.
			available :=
				maxOutstandingCredit - outstandingCredit.Load()

			if credit > available {
				credit = available
			}

			if credit <= 0 {
				continue
			}

			// Reserve before issuing because Receive runs concurrently.
			outstandingCredit.Add(credit)

			if err := receiver.IssueCredit(uint32(credit)); err != nil {
				outstandingCredit.Add(-credit)
				return fmt.Errorf("issue credit: %w", err)
			}

		case now := <-monitorTicker.C:
			count := received.Swap(0)
			measuredRate :=
				float64(count) / MonitorInterval.Seconds()

			controllerOutput := pid.Update(
				RateGoal,
				measuredRate,
			)

			setRate(controllerOutput)

			nSamples++

			fmt.Printf(
				"%v;%v;%v;%v;%v\n",
				now,
				nSamples,
				int(getRate()),
				RateGoal,
				int(measuredRate),
			)

			if nSamples >= SampleSize {
				fmt.Println("End of experiment")
				return nil
			}
		}
	}
}

func RunClosedLoop2() {

	// create context
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// create queue
	queueAddress := "/queues/" + url.PathEscape(QueueName)
	if err := EnsureQueue(
		ctx,
		"http://localhost:15672",
		"guest",
		"guest",
		"/",
		QueueName,
	); err != nil {
		log.Fatalf("declare queue: %v", err)
	}

	// create connection
	connection, err := amqp.Dial(
		ctx,
		"amqp://localhost:5672",
		&amqp.ConnOptions{
			SASLType: amqp.SASLTypePlain("guest", "guest"),
		},
	)
	if err != nil {
		log.Fatalf("connect to RabbitMQ: %v", err)
	}
	defer connection.Close()

	// create session
	session, err := connection.NewSession(ctx, nil)
	if err != nil {
		log.Fatalf("create session: %v", err)
	}
	defer session.Close(context.Background())

	// create receiver
	receiver, err := session.NewReceiver(
		ctx,
		queueAddress,
		&amqp.ReceiverOptions{
			Credit: -1,
		},
	)
	if err != nil {
		log.Fatalf("create receiver: %v", err)
	}
	defer receiver.Close(context.Background())

	log.Printf("Waiting for messages...\n")

	// controller setup
	pid := basicpid.Controller{}
	pid.Initialise(Direction, MinLC, MaxLC, Kp, Ki, Kd)
	//controller := hpa.Controller{}
	//controller.Initialise(MinLC, MaxLC)
	u := float64(InitialLinkedCredit)
	receiver.IssueCredit(1) // todo

	// initialisations
	ticker := time.NewTicker(MonitorInterval)
	nMsgs := 0
	nSamples := 0
	//receiver.IssueCredit(1)

	//client := &http.Client{Timeout: 5 * time.Second}

	// loop
	//remainCredits := 1
	fmt.Println("************* Begin of Experiment ************* ")
	for {
		select {
		case <-ticker.C:
			// compute receiving rate
			r := float64(nMsgs) / float64(MonitorInterval.Seconds())

			// get backlog

			/*
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
			*/
			//fmt.Printf("%v;%v;%v;%v;%v\n", time.Now(), nSamples+1, int32(u), RateGoal, r)
			fmt.Printf("%v;%v;%v;%v\n", nSamples+1, int32(u), RateGoal, r)

			//fmt.Printf("[Link-credit=%v;BacklogGoal=%v;=CurrentBacklog=%v;ReceivingRate=%v s\n", u, BacklogGoal, backlog.Ready+backlog.Unacknowledged, r)

			// invoke controller
			u = pid.Update(RateGoal, r)

			// update & check samples
			nSamples++
			if nSamples >= SampleSize {
				fmt.Println("************* End of Experiment ************* ")
				os.Exit(0)
			}

			// update link credit
			receiver.DrainCredit(ctx, nil)
			receiver.IssueCredit(uint32(u))
			//receiver.IssueCredit(uint32(u))

			// reinitialise variables
			nMsgs = 0
			ticker.Reset(MonitorInterval)
		default:
		}

		// receive message
		message, err := receiver.Receive(ctx, nil)
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				log.Print("consumer stopped")
				return
			}
			log.Fatalf("receive message: %v", err)
		}

		// process message
		//log.Printf("received: %s", string(message.GetData()))
		//time.Sleep(ProcessingTime)

		// ack message
		if err := receiver.AcceptMessage(ctx, message); err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return
			}
			log.Fatalf("accept message: %v", err)
		}
		receiver.IssueCredit(1)
		nMsgs++
	}
}

func RunClosedLoop1() {

	// create context
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// create queue
	queueAddress := "/queues/" + url.PathEscape(QueueName)
	if err := EnsureQueue(
		ctx,
		"http://localhost:15672",
		"guest",
		"guest",
		"/",
		QueueName,
	); err != nil {
		log.Fatalf("declare queue: %v", err)
	}

	// create connection
	connection, err := amqp.Dial(
		ctx,
		"amqp://localhost:5672",
		&amqp.ConnOptions{
			SASLType: amqp.SASLTypePlain("guest", "guest"),
		},
	)
	if err != nil {
		log.Fatalf("connect to RabbitMQ: %v", err)
	}
	defer connection.Close()

	// create session
	session, err := connection.NewSession(ctx, nil)
	if err != nil {
		log.Fatalf("create session: %v", err)
	}
	defer session.Close(context.Background())

	// create receiver
	receiver, err := session.NewReceiver(
		ctx,
		queueAddress,
		&amqp.ReceiverOptions{
			Credit: InitialLinkedCredit,
		},
	)
	if err != nil {
		log.Fatalf("create receiver: %v", err)
	}
	defer receiver.Close(context.Background())

	log.Printf("Waiting for messages...\n")

	// controller setup
	pid := basicpid.Controller{}
	pid.Initialise(Direction, MinLC, MaxLC, Kp, Ki, Kd)
	//controller := hpa.Controller{}
	//controller.Initialise(MinLC, MaxLC)
	u := float64(InitialLinkedCredit)
	//receiver.IssueCredit(InitialLinkedCredit)

	// initialisations
	ticker := time.NewTicker(MonitorInterval)
	nMsgs := 0
	nSamples := 0
	//receiver.IssueCredit(1)

	//client := &http.Client{Timeout: 5 * time.Second}

	// loop
	previousU := 0
	fmt.Println("************* Begin of Experiment ************* ")
	for {
		select {
		case <-ticker.C:
			// compute receiving rate
			r := float64(nMsgs) / float64(MonitorInterval.Seconds())

			// get backlog

			/*
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
			*/
			fmt.Printf("%v;%v;%v;%v;%v\n", time.Now(), nSamples+1, int32(u), RateGoal, r)

			//fmt.Printf("[Link-credit=%v;BacklogGoal=%v;=CurrentBacklog=%v;ReceivingRate=%v s\n", u, BacklogGoal, backlog.Ready+backlog.Unacknowledged, r)

			// invoke controller
			u = pid.Update(RateGoal, r)

			if int(u) != previousU {
				previousU = int(u)

				// close receiver
				err = receiver.Close(context.Background())
				if err != nil {
					log.Fatal("close receiver: %w", err)
				}

				// close session
				err = session.Close(context.Background())
				if err != nil {
					log.Fatal("close session: %w", err)
				}

				// stop & create context
				ctx.Done()
				ctx, stop = signal.NotifyContext(
					context.Background(),
					os.Interrupt,
					syscall.SIGTERM,
				)

				// create new session
				session = nil
				session, err = connection.NewSession(ctx, nil)
				if err != nil {
					log.Fatalf("create session: %v", err)
				}
				//defer session.Close(context.Background())

				// create new receiver
				receiver = nil
				receiver, err = session.NewReceiver(
					ctx,
					queueAddress,
					&amqp.ReceiverOptions{
						Credit: int32(u),
					},
				)
				if err != nil {
					log.Fatalf("create receiver: %v", err)
				}
			}
			// update & check samples
			nSamples++
			if nSamples >= SampleSize {
				fmt.Println("************* End of Experiment ************* ")
				os.Exit(0)
			}

			// reinitialise variables
			nMsgs = 0
			ticker.Reset(MonitorInterval)
		default:
		}

		// receive message
		message, err := receiver.Receive(ctx, nil)
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				log.Print("consumer stopped")
				return
			}
			log.Fatalf("receive message: %v", err)
		}

		// process message
		//log.Printf("received: %s", string(message.GetData()))
		//time.Sleep(ProcessingTime)

		// ack message
		if err := receiver.AcceptMessage(ctx, message); err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return
			}
			log.Fatalf("accept message: %v", err)
		}
		nMsgs++
	}
}

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

func EnsureQueue(
	ctx context.Context,
	managementURL string,
	username string,
	password string,
	vhost string,
	queue string,
) error {
	if managementURL == "" {
		return errors.New("management URL must not be empty")
	}
	if vhost == "" {
		return errors.New("virtual host must not be empty")
	}
	if queue == "" {
		return errors.New("queue name must not be empty")
	}

	payload, err := json.Marshal(struct {
		AutoDelete bool           `json:"auto_delete"`
		Durable    bool           `json:"durable"`
		Arguments  map[string]any `json:"arguments"`
	}{
		AutoDelete: false,
		Durable:    true,
		Arguments:  map[string]any{"x-queue-type": "classic"},
	})
	if err != nil {
		return fmt.Errorf("encode queue declaration: %w", err)
	}

	endpoint := strings.TrimRight(managementURL, "/") +
		"/api/queues/" + url.PathEscape(vhost) + "/" + url.PathEscape(queue)
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPut,
		endpoint,
		bytes.NewReader(payload),
	)
	if err != nil {
		return fmt.Errorf("create queue declaration request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(username, password)

	client := &http.Client{Timeout: 10 * time.Second}
	response, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("execute queue declaration request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode >= http.StatusOK &&
		response.StatusCode < http.StatusMultipleChoices {
		return nil
	}

	body, readErr := io.ReadAll(io.LimitReader(response.Body, 8<<10))
	if readErr != nil {
		return fmt.Errorf(
			"queue declaration returned %s; response body could not be read: %w",
			response.Status,
			readErr,
		)
	}
	return fmt.Errorf(
		"queue declaration returned %s: %s",
		response.Status,
		strings.TrimSpace(string(body)),
	)
}

func myController(goal, measured, previousOut float64) float64 {
	r := previousOut
	if measured > goal {
		r += 1
	}
	if measured < goal {
		r -= 1
	}

	if r < MinLC {
		r = MinLC
	}
	if r > MaxLC {
		r = MaxLC
	}
	return r
}
