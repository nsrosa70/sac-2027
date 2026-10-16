package main

import (
	"adaptivesystemscourse/internal/control/hpa"
	basicpid "adaptivesystemscourse/internal/control/pid"
	"adaptivesystemscourse/internal/shared"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

const ClientQueueSize = 1
const SubscriptionName = "demo-subscription"
const TopicName = "persistent://public/default/demo"
const ProcessingTime = time.Duration(25 * time.Millisecond)

// const BacklogGoal = 5000 // 1o. backlog avaliado
const BacklogGoal = 2000

const MinDispatchRate = 1
const MaxDispatchRate = 10000

//const LatencyGoal = time.Duration(1) * time.Second

// const Kp = 0.95
// const Kp = 0.95
// const Ki = 0.1
//const Kp = 0.25 // 29/09/2026
//const Ki = 0.11 // 29/09/2026

const Kp = 0.30  // 01/10/2026 (final experiments)
const Ki = 0.015 // 01/10/2026

const Kd = 0.0
const ReceiverQueueSize = -1 // No queue

const SampleSize = 150
const SamplesPerLevel = 150
const MonitorInterval = time.Duration(5) * time.Second
const Native = 1
const PI = 2
const ControllerType = PI

const RateGoal = 233 //
const InitialDispatchRate = 240

var RatesGoal = [6]int32{233, 498, 100, 453, 343, 233}

func main() {
	//go OpenLoop()
	go ClosedLoop()
	//go ClosedLoopMultipleRates()

	fmt.Scanln()
}

func ClosedLoop() {
	// parameters
	serviceURL := flag.String("url", "pulsar://localhost:6650", "Pulsar service URL")
	flag.Parse()

	// configuration
	adminURL := flag.String(
		"admin-url",
		"http://localhost:8080",
		"Pulsar Admin API URL",
	)
	tenant := flag.String(
		"tenant",
		"public",
		"Pulsar tenant",
	)
	namespace := flag.String(
		"namespace",
		"default",
		"Pulsar namespace",
	)
	token := flag.String(
		"token",
		"",
		"optional authentication token",
	)
	flag.Parse()

	controller := shared.DispatchRateController{
		AdminURL:  *adminURL,
		Tenant:    *tenant,
		Namespace: *namespace,
		Token:     *token,
		Client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}

	// create client
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: *serviceURL})
	if err != nil {
		log.Fatalf("create client: %v", err)
	}
	defer client.Close()

	// create subscriber-prefetch
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:             TopicName,
		SubscriptionName:  SubscriptionName,
		Type:              pulsar.Shared,
		ReceiverQueueSize: ReceiverQueueSize,
	})
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	defer consumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	//log.Printf("listening topic=%q subscription=%q queue_size=%d", *topic, *subscription, *queueSize)
	fmt.Println("Consumer:: Waiting for messages...")
	fmt.Println("****** Begin of Experiment ******")
	ticker := time.NewTicker(MonitorInterval)

	// configure initial rate goal
	dispatchRate := int64(InitialDispatchRate) // xx msg/s
	if err := controller.SetDispatchRate(ctx, InitialDispatchRate); err != nil {
		log.Fatalf("change dispatch rate: %v", err)
	}
	//latencies := []time.Duration{}

	// configure controller
	hpa := new(hpa.Controller)
	hpa.Initialise(1, MinDispatchRate, MaxDispatchRate)
	//aimd.Init(1.0, 1000.0, 100, float64(dispatchRate), float64(dispatchRate))
	pid := new(basicpid.Controller)
	pid.Initialise(1, MinDispatchRate, MaxDispatchRate, Kp, Ki, Kd) // TODO
	nMsgs := 0

	// admin
	//admin := PulsarAdminClient{
	//	BaseURL: "http://localhost:8080",
	//	// Token: "your-jwt-token",
	//}

	nSamples := 0
	for {
		select {
		case <-ticker.C:
			// compute current metric
			//p95latency, err := durationPercentile(latencies, 95) // TODO
			//if err != nil {
			//	fmt.Println("Error:", err)
			//	return
			//}

			// get backlog
			/*stats, err := admin.GetSubscriptionStats(
				ctx,
				"public",
				"default",
				"demo",
				"demo-subscription",
				false, // estimated backlog; preferable for frequent polling
			)
			if err != nil {
				panic(err)
			}
			*/
			// compute actual rate
			actualRate := float64(nMsgs) / float64(MonitorInterval.Seconds())
			fmt.Printf("%v;%v;%v;%v\n", nSamples+1, RateGoal, int(actualRate), dispatchRate)
			//b = int(stats.MsgBacklog)

			// invoke controller
			switch ControllerType {
			case Native:
				dispatchRate = InitialDispatchRate
			case PI:
				dispatchRate = int64(pid.Update(RateGoal, actualRate))
			default:
				fmt.Println("Wrong controller type")
				os.Exit(1)
			}
			//dispatchRate = int64(hpa.Update(RateGoal, actualRate, float64(dispatchRate)))
			//dispatchRate = int64(aimd.Update(float64(latencyGoal.Milliseconds()), float64(p95latency.Milliseconds())))
			//dispatchRate = int64(myController(BacklogGoal, float64(stats.MsgBacklog), float64(dispatchRate)))
			//dispatchRate = RateGoal

			// adjust the dispatch rate
			if err := controller.SetDispatchRate(ctx, dispatchRate); err != nil {
				log.Fatalf("change dispatch rate: %v", err)
			}

			//fmt.Printf("[BacklogGoal=%v]; [ActualBacklog=%v]; [DispatchRate=%v];[ActualRate=%v];%v \n", BacklogGoal, stats.MsgBacklog, dispatchRate, actualRate, isEffective(dispatchRate, actualRate))
			//fmt.Printf("%v;%v;%v;%v;%v\n", BacklogGoal, stats.MsgBacklog, dispatchRate, actualRate, isEffective(dispatchRate, actualRate))

			//fmt.Printf("%v;%v;%v;%v;%v;%v \n", len(latencies), latencyGoal.Milliseconds(), p95latency.Milliseconds(), dispatchRate, float64(count)/float64(monitorInterval.Seconds()), stats.MsgBacklog)
			//fmt.Printf("%v;%v;%v\n", dispatchRate, latencyGoal.Milliseconds(), p95latency.Milliseconds())

			// reset latencies
			nMsgs = 0
			nSamples++
			//latencies = latencies[:0]

			if nSamples >= SampleSize {
				fmt.Println("****** End of Experiment ******")
				os.Exit(1)
			}

			// reset ticker
			ticker.Reset(MonitorInterval)
		default:
		}
		// receive message
		message, err := consumer.Receive(ctx)
		if err != nil {
			log.Fatalf("receive message: %v", err)
		}

		// processing time
		//fmt.Println(ProcessingTime * time.Millisecond)
		//time.Sleep(ProcessingTime * time.Millisecond)

		// send ack back to the broker
		consumer.Ack(message)

		// increment message counter
		nMsgs++

		// compute & store latency
		/*
			sentAtText, exists := message.Properties()["sent-at-ns"]
			if !exists {
				log.Println("missing sent-at-ns property")
				consumer.Nack(message)
				return
			}
			sentAt, err := time.Parse(time.RFC3339Nano, sentAtText)
			if err != nil {
				log.Printf("invalid sent-at-ns property: %v", err)
				consumer.Nack(message)
				return
			}
			receivedAt := time.Now().UTC()
			latency := receivedAt.Sub(sentAt)
			latencies = append(latencies, latency)
		*/
	}
}

func ClosedLoopMultipleRates() {
	// parameters
	serviceURL := flag.String("url", "pulsar://localhost:6650", "Pulsar service URL")
	flag.Parse()

	// configuration
	adminURL := flag.String(
		"admin-url",
		"http://localhost:8080",
		"Pulsar Admin API URL",
	)
	tenant := flag.String(
		"tenant",
		"public",
		"Pulsar tenant",
	)
	namespace := flag.String(
		"namespace",
		"default",
		"Pulsar namespace",
	)
	token := flag.String(
		"token",
		"",
		"optional authentication token",
	)
	flag.Parse()

	controller := shared.DispatchRateController{
		AdminURL:  *adminURL,
		Tenant:    *tenant,
		Namespace: *namespace,
		Token:     *token,
		Client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}

	// create client
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: *serviceURL})
	if err != nil {
		log.Fatalf("create client: %v", err)
	}
	defer client.Close()

	// create subscriber-prefetch
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:             TopicName,
		SubscriptionName:  SubscriptionName,
		Type:              pulsar.Shared,
		ReceiverQueueSize: ReceiverQueueSize,
	})
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	defer consumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	//log.Printf("listening topic=%q subscription=%q queue_size=%d", *topic, *subscription, *queueSize)
	fmt.Println("Consumer:: Waiting for messages...")
	fmt.Println("****** Begin of Experiment ******")
	ticker := time.NewTicker(MonitorInterval)

	// configure initial rate goal
	dispatchRate := int64(RatesGoal[0]) // xx msg/s
	if err := controller.SetDispatchRate(ctx, dispatchRate); err != nil {
		log.Fatalf("change dispatch rate: %v", err)
	}
	//latencies := []time.Duration{}

	// configure controller
	//hpa := new(hpa.Controller)
	//hpa.Initialise(1, MinDispatchRate, MaxDispatchRate)
	//aimd.Init(1.0, 1000.0, 100, float64(dispatchRate), float64(dispatchRate))
	pid := new(basicpid.Controller)
	pid.Initialise(1, MinDispatchRate, MaxDispatchRate, Kp, Ki, Kd) // TODO
	nMsgs := 0

	// admin
	//admin := PulsarAdminClient{
	//	BaseURL: "http://localhost:8080",
	//	// Token: "your-jwt-token",
	//}

	nSamples := 0
	nSamplesPerLevel := 0
	contLevels := 0
	for {
		select {
		case <-ticker.C:
			// compute current metric
			//p95latency, err := durationPercentile(latencies, 95) // TODO
			//if err != nil {
			//	fmt.Println("Error:", err)
			//	return
			//}

			// get backlog
			/*stats, err := admin.GetSubscriptionStats(
				ctx,
				"public",
				"default",
				"demo",
				"demo-subscription",
				false, // estimated backlog; preferable for frequent polling
			)
			if err != nil {
				panic(err)
			}
			*/
			// compute actual rate
			actualRate := float64(nMsgs) / float64(MonitorInterval.Seconds())
			fmt.Printf("%v;%v;%v;%v\n", nSamples+1, RatesGoal[contLevels], int(actualRate), dispatchRate)
			//b = int(stats.MsgBacklog)

			// invoke controller
			switch ControllerType {
			case Native:
				dispatchRate = RateGoal
			case PI:
				dispatchRate = int64(pid.Update(float64(RatesGoal[contLevels]), actualRate))
			default:
				fmt.Println("Wrong controller type")
				os.Exit(1)
			}
			//dispatchRate = int64(hpa.Update(RateGoal, actualRate, float64(dispatchRate)))
			//dispatchRate = int64(aimd.Update(float64(latencyGoal.Milliseconds()), float64(p95latency.Milliseconds())))
			//dispatchRate = int64(myController(BacklogGoal, float64(stats.MsgBacklog), float64(dispatchRate)))
			//dispatchRate = RateGoal

			// adjust the dispatch rate
			if err := controller.SetDispatchRate(ctx, dispatchRate); err != nil {
				log.Fatalf("change dispatch rate: %v", err)
			}

			//fmt.Printf("[BacklogGoal=%v]; [ActualBacklog=%v]; [DispatchRate=%v];[ActualRate=%v];%v \n", BacklogGoal, stats.MsgBacklog, dispatchRate, actualRate, isEffective(dispatchRate, actualRate))
			//fmt.Printf("%v;%v;%v;%v;%v\n", BacklogGoal, stats.MsgBacklog, dispatchRate, actualRate, isEffective(dispatchRate, actualRate))

			//fmt.Printf("%v;%v;%v;%v;%v;%v \n", len(latencies), latencyGoal.Milliseconds(), p95latency.Milliseconds(), dispatchRate, float64(count)/float64(monitorInterval.Seconds()), stats.MsgBacklog)
			//fmt.Printf("%v;%v;%v\n", dispatchRate, latencyGoal.Milliseconds(), p95latency.Milliseconds())

			// reset variables
			nMsgs = 0
			nSamples++
			nSamplesPerLevel++

			if nSamplesPerLevel >= SamplesPerLevel {
				nSamplesPerLevel = 0
				contLevels++

			}

			if contLevels >= len(RatesGoal) {
				fmt.Println("****** End of Experiment ******")
				os.Exit(1)
			}

			// reset ticker
			ticker.Reset(MonitorInterval)
		default:
		}
		// receive message
		message, err := consumer.Receive(ctx)
		if err != nil {
			log.Fatalf("receive message: %v", err)
		}

		// processing time
		//fmt.Println(ProcessingTime * time.Millisecond)
		//time.Sleep(ProcessingTime * time.Millisecond)

		// send ack back to the broker
		consumer.Ack(message)

		// increment message counter
		nMsgs++

		// compute & store latency
		/*
			sentAtText, exists := message.Properties()["sent-at-ns"]
			if !exists {
				log.Println("missing sent-at-ns property")
				consumer.Nack(message)
				return
			}
			sentAt, err := time.Parse(time.RFC3339Nano, sentAtText)
			if err != nil {
				log.Printf("invalid sent-at-ns property: %v", err)
				consumer.Nack(message)
				return
			}
			receivedAt := time.Now().UTC()
			latency := receivedAt.Sub(sentAt)
			latencies = append(latencies, latency)
		*/
	}
}

func OpenLoop() {
	// parameters
	serviceURL := flag.String("url", "pulsar://localhost:6650", "Pulsar service URL")
	flag.Parse()

	// configuration
	adminURL := flag.String(
		"admin-url",
		"http://localhost:8080",
		"Pulsar Admin API URL",
	)
	tenant := flag.String(
		"tenant",
		"public",
		"Pulsar tenant",
	)
	namespace := flag.String(
		"namespace",
		"default",
		"Pulsar namespace",
	)
	token := flag.String(
		"token",
		"",
		"optional authentication token",
	)
	flag.Parse()

	controller := shared.DispatchRateController{
		AdminURL:  *adminURL,
		Tenant:    *tenant,
		Namespace: *namespace,
		Token:     *token,
		Client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}

	// create client
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: *serviceURL})
	if err != nil {
		log.Fatalf("create client: %v", err)
	}
	defer client.Close()

	// create subscriber-prefetch
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            TopicName,
		SubscriptionName: SubscriptionName,
		Type:             pulsar.Shared,
		//ReceiverQueueSize: ClientQueueSize,
		ReceiverQueueSize: ReceiverQueueSize, // TODO
	})
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	defer consumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// setup
	dispatchRate := int64(InitialDispatchRate) // xx msg/s

	//log.Printf("listening topic=%q subscription=%q queue_size=%d", *topic, *subscription, *queueSize)
	fmt.Println("Consumer:: Waiting for messages...")
	ticker := time.NewTicker(MonitorInterval)

	// controller
	nMsgs := 0
	nSamples := 0

	// configure initial rate goal
	if err := controller.SetDispatchRate(ctx, InitialDispatchRate); err != nil {
		log.Fatalf("change dispatch rate: %v", err)
	}

	// admin
	//admin := PulsarAdminClient{
	//	BaseURL: "http://localhost:8080",
	//}

	// initialise
	for {
		select {
		case <-ticker.C:
			r := float64(nMsgs) / float64(MonitorInterval.Seconds())
			nMsgs = 0
			/*
				// get backlog
				stats, err := admin.GetSubscriptionStats(
					ctx,
					"public",
					"default",
					"demo",
					"demo-subscription",
					false, // estimated backlog; preferable for frequent polling
				)
				if err != nil {
					panic(err)
				}
			*/
			fmt.Printf("%v;%v;%v\n", time.Now(), dispatchRate, r)
			if nSamples >= SampleSize-1 {
				nSamples = 0
				dispatchRate += 10

				// adjust the dispatch rate
				if err := controller.SetDispatchRate(ctx, dispatchRate); err != nil {
					log.Fatalf("change dispatch rate: %v", err)
				}
			} else {
				nSamples++
			}

			// reset ticker
			ticker.Reset(MonitorInterval)
		default:
		}

		// receive message
		message, err := consumer.Receive(ctx)
		if err != nil {
			log.Fatalf("receive message: %v", err)
		}

		// process message
		//time.Sleep(ProcessingTime)

		// send ack back to broker
		consumer.Ack(message)

		// increment message counter
		nMsgs++
	}
}

func percentileDuration(
	sample []time.Duration,
	percentile float64,
) (time.Duration, error) {
	if len(sample) == 0 {
		return 0, errors.New("sample cannot be empty")
	}

	if percentile <= 0 || percentile > 100 {
		return 0, errors.New(
			"percentile must be greater than 0 and at most 100",
		)
	}

	// Copy the sample so the original slice is not modified.
	sorted := append([]time.Duration(nil), sample...)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	// Nearest-rank method.
	rank := int(math.Ceil(
		percentile / 100.0 * float64(len(sorted)),
	))

	for i := range sorted {
		fmt.Println(sorted[i].Milliseconds(), sorted[rank].Milliseconds())
	}
	return sorted[rank-1], nil
}

type TopicStats struct {
	Subscriptions map[string]SubscriptionStats `json:"subscriptions"`
}

type SubscriptionStats struct {
	MsgBacklog                      int64   `json:"msgBacklog"`
	BacklogSize                     int64   `json:"backlogSize"`
	MsgBacklogNoDelayed             int64   `json:"msgBacklogNoDelayed"`
	UnackedMessages                 int64   `json:"unackedMessages"`
	MsgDelayed                      int64   `json:"msgDelayed"`
	MsgRateOut                      float64 `json:"msgRateOut"`
	MsgThroughputOut                float64 `json:"msgThroughputOut"`
	MessageAckRate                  float64 `json:"messageAckRate"`
	EarliestMsgPublishTimeInBacklog int64   `json:"earliestMsgPublishTimeInBacklog"`
}

type PulsarAdminClient struct {
	BaseURL    string
	Token      string
	HTTPClient *http.Client
}

func (p *PulsarAdminClient) GetSubscriptionStats(
	ctx context.Context,
	tenant string,
	namespace string,
	topic string,
	subscription string,
	precise bool,
) (SubscriptionStats, error) {
	endpoint := fmt.Sprintf(
		"%s/admin/v2/persistent/%s/%s/%s/stats",
		p.BaseURL,
		url.PathEscape(tenant),
		url.PathEscape(namespace),
		url.PathEscape(topic),
	)

	// Supported by current Pulsar Admin REST implementations.
	if precise {
		endpoint += "?get_precise_backlog=true"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return SubscriptionStats{}, err
	}

	if p.Token != "" {
		req.Header.Set("Authorization", "Bearer "+p.Token)
	}

	client := p.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}

	resp, err := client.Do(req)
	if err != nil {
		return SubscriptionStats{}, fmt.Errorf("request topic stats: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return SubscriptionStats{}, fmt.Errorf(
			"pulsar admin returned %s: %s",
			resp.Status,
			string(body),
		)
	}

	var stats TopicStats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return SubscriptionStats{}, fmt.Errorf("decode topic stats: %w", err)
	}

	subStats, found := stats.Subscriptions[subscription]
	if !found {
		return SubscriptionStats{}, fmt.Errorf(
			"subscription %q not found",
			subscription,
		)
	}

	return subStats, nil
}

func myController(goal, measured, dr float64) float64 {
	r := dr
	if measured <= goal {
		r -= 50.0
	}
	if measured > goal {
		r += 50.0
	}

	if dr < MinDispatchRate {
		r = MinDispatchRate
	}
	if dr > MaxDispatchRate {
		r = MaxDispatchRate
	}
	return r
}

// DurationPercentile calculates the percentile of a time.Duration slice using linear interpolation.
func durationPercentile(input []time.Duration, percent float64) (time.Duration, error) {
	if len(input) == 0 {
		return 0, fmt.Errorf("input slice cannot be empty")
	}
	if percent < 0 || percent > 100 {
		return 0, fmt.Errorf("percent must be between 0 and 100")
	}

	// 1. Duplicate the dataset so the original slice isn't mutated
	data := make([]time.Duration, len(input))
	copy(data, input)

	// 2. Sort the durations (time.Duration natively supports sorting using Slice or SliceStable)
	sort.Slice(data, func(i, j int) bool {
		return data[i] < data[j]
	})

	// Edge cases for exact boundaries
	if percent == 0 {
		return data[0], nil
	}
	if percent == 100 {
		return data[len(data)-1], nil
	}

	// 3. Compute the rank/index
	rank := (percent / 100) * float64(len(data)-1)

	// 4. Split into integer and fractional components
	k := int(math.Floor(rank))
	f := rank - float64(k)

	// 5. Interpolate using nanosecond values, then convert back to time.Duration
	d1 := float64(data[k].Nanoseconds())
	d2 := float64(data[k+1].Nanoseconds())
	interpolatedNs := d1 + f*(d2-d1)

	// Round to nearest integer nanosecond before casting back to time.Duration
	return time.Duration(math.Round(interpolatedNs)), nil
}

func isEffective(d int64, a float64) string {
	if a/float64(d) < 0.8 {
		return "Ineffective"
	} else {
		return "Effective"
	}
}
