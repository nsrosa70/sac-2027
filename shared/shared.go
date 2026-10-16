package shared

import (
	"adaptivesystemscourse/internal/config"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/v3/process"
)

const EC2 = "EC2"
const PID = "PID"
const AdaptivePID = "AdaptivePID"
const Neural = "Neural"
const AIMD = "AIMD"
const HPA = "HPA"
const ConfigFile = "config.json"
const ControlDir = "ControlDir"
const GoRoot = "GOROOT"
const Experiment = "Experiment"
const OpenLoop = "OpenLoop"
const Training = "Training"
const AdaptiveController = "AdaptiveController"
const StaticController = "StaticController"
const EC2_RABBITMQ = "EC2_RABBITMQ"
const EC2_SUBSCRIBER = "EC2_SUBSCRIBER"
const EC2_PUBLISHER = "EC2_PUBLISHER"
const Subscriber = "Subscriber"
const Publisher = "Publisher"
const Fixed = "Fixed"
const Randon = "Randon"
const MonitorTime = 1 // seconds
const NumberOfColors = 7

const AsTAR = "AsTAR"
const BasicOnoff = "OnOff"
const DeadZoneOnoff = "OnOffwithDeadZone"
const HysteresisOnoff = "OnOffwithHysteresis"
const BasicP = "BasicP"
const BasicPi = "BasicPI"
const BasicPid = "BasicPID"
const SmoothingPid = "SmoothingDerivativePID"
const IncrementalFormPid = "IncrementalFormPID"
const ErrorSquarePidFull = "ErrorSquarePIDFull"
const ErrorSquarePidProportional = "ErrorSquarePIDProportional"
const DeadZonePid = "DeadZonePID"
const GainScheduling = "GainScheduling"
const PIwithTwoDegreesOfFreedom = "PIWithTwoDegreesOfFreedom"
const WindUp = "WindUp"
const SetpointWeighting = "SetpointWeighting"

var ColorBehaviours = []string{"\033[31m", "\033[32m", "\033[33m", "\033[34m", "\033[35m", "\033[36m", "\033[37m"}

type EnvVariable struct {
	Name  string
	Type  interface{} `json:"type"`
	Value interface{} `json:"value"`
}

type ControlGains struct {
	Kp float64 `json:"kp"`
	Ki float64 `json:"ki"`
	Kd float64 `json:"kd"`
}

func MyRandon(min, max int) int {
	var r int
	for {
		r = rand.Intn(max)
		if r >= min && r <= max {
			break
		}
	}
	return r
}

func GetCPUUsage() float64 {
	var r []float64
	r, err := cpu.Percent(time.Second, false)
	if err != nil {
		fmt.Println("Error:", err)
	}
	return r[0]
}

func FindGoProcessNameToBeMonitored() string {
	var r string

	// Specify the process name you want to monitor
	processNamePattern := "go_build_main_go"

	// Find the process by name
	processes, err := process.Processes()
	if err != nil {
		fmt.Printf("Error getting processes: %v\n", err)
		return r
	}

	var targetProcess *process.Process
	for _, p := range processes {
		name, err := p.Name()
		if err != nil {
			continue
		}
		if strings.Contains(name, processNamePattern) {
			targetProcess = p
			r = name
			break
		}
	}

	if targetProcess == nil {
		fmt.Printf("No process found having '%s' pattern name\n", processNamePattern)
		os.Exit(0)
	}
	return r
}

func RemoveContents(dir string) error {
	files, err := filepath.Glob(filepath.Join(dir, "*"))
	if err != nil {
		return err
	}
	for _, file := range files {
		err = os.RemoveAll(file)
		if err != nil {
			return err
		}
	}
	return nil
}

func ErrorHandler(f string, msg string) {
	fmt.Println(f + "::" + msg)
	os.Exit(0)
}

func GetFunction() string {
	fpcs := make([]uintptr, 1)

	// Skip 2 levels to get the caller
	n := runtime.Callers(2, fpcs)
	if n == 0 {
		fmt.Println("MSG: NO CALLER")
	}

	caller := runtime.FuncForPC(fpcs[0] - 1)
	if caller == nil {
		fmt.Println("MSG CALLER WAS NIL")
	}
	return caller.Name()
}

func LocalizegGo() string {
	r := ""
	found := false

	for _, e := range os.Environ() {
		pair := strings.SplitN(e, "=", 2)
		if pair[0] == "GOROOT" {
			r = pair[1]
			found = true
		}
	}

	if !found {
		fmt.Println("Shared:: Error:: OS EnvironmentSubscriber variable 'GOROOT' not configured\n")
		os.Exit(1)
	}
	return r
}

func FileExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

func RemoveFolder(path string) error {
	err := os.RemoveAll(path)
	return err
}

func ToStrings(params ...interface{}) []string {
	var result []string
	for _, param := range params {
		str := fmt.Sprintf("%v", param)
		result = append(result, str)
	}
	return result
}

func RunCommand(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func NormallyDistributedDuration(mean, stdDev time.Duration) time.Duration {
	// Convert durations to float64 nanoseconds
	meanNs := float64(mean.Nanoseconds())
	stdDevNs := float64(stdDev.Nanoseconds())

	// Generate normally distributed value
	normalValue := rand.NormFloat64()*stdDevNs + meanNs

	// Convert back to time.Duration, ensuring non-negative
	if normalValue < 0 {
		normalValue = 0
	}

	return time.Duration(normalValue) * time.Nanosecond
}

func SetPC(ch *amqp091.Channel, pc int) int {
	// Set QoS: prefetch count = 1 (fair dispatch)
	err := ch.Qos(
		pc,   // prefetch count
		0,    // prefetch size (0 = ignore)
		true, // global (false = per consumer)
	)
	FailOnError(err, "Failed to set QoS")

	return pc
}

func GetQueueLength(queueName string) int {
	url := fmt.Sprintf("http://localhost:15672/api/queues/%%2f/%s", queueName)
	req, _ := http.NewRequest("GET", url, nil)
	req.SetBasicAuth("guest", "guest")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Println("Error querying queue:", err)
		return 0
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var data struct {
		Messages int `json:"messages"`
	}
	json.Unmarshal(body, &data)

	return data.Messages
}

func GenerateSinPC() []int {
	r := []int{}
	start := 0.0
	end := 4 * math.Pi
	step := 0.001

	for x := start; x <= end; x += step {
		// Shift sine so that at x=0 value is -1
		shifted := math.Sin(x - math.Pi/2)
		// Scale from [-1, 1] to [0, 100]
		scaled := (shifted + 1) * 50
		temp := int(math.Round(scaled))
		if temp != 0 {
			r = append(r, int(math.Round(scaled)))
		}
	}
	return r
}

func LoadConfig(caller string) *config.Config {

	fmt.Println("Loading config...")

	// Load OS environment variable
	dir, found := os.LookupEnv(ControlDir)
	if !found {
		ErrorHandler(GetFunction(), "Environment variable '"+ControlDir+"' is required")
	}

	// Read conf file
	file, err := os.Open(dir + "/" + ConfigFile)
	if err != nil {
		ErrorHandler(GetFunction(), "File '"+dir+"/"+ConfigFile+"' not found")
	}
	defer file.Close()

	// Decode file
	decoder := json.NewDecoder(file)
	config := &config.Config{}
	err = decoder.Decode(config)
	if err != nil {
		ErrorHandler(GetFunction(), "Error loading config: "+err.Error())
	}

	// update env host variables
	if config.RabbitMQ.Host == "" {
		config.RabbitMQ.Host, found = os.LookupEnv(EC2_RABBITMQ)
		if !found {
			ErrorHandler(GetFunction(), "Environment variable '"+EC2_RABBITMQ+"' is required")
		}
	}

	if config.Subscriber.Host == "" {
		config.Subscriber.Host, found = os.LookupEnv(EC2_SUBSCRIBER)
		if !found {
			ErrorHandler(GetFunction(), "Environment variable '"+EC2_SUBSCRIBER+"' is required")
		}
	}
	return config
}

func GenerateEnvFile(caller string) map[string]string {
	envFileContent := map[string]string{}

	// Prepare file to write
	file, err := os.Create("env.env")
	if err != nil {
		ErrorHandler(GetFunction(), "Error creating file: "+err.Error())
	}
	defer file.Close()

	// Read config file
	Conf := LoadConfig(caller)

	// Check ec2 rabbitmq, ec2 publisher, ec2 subscriber-prefetch
	Conf.RabbitMQ.Host = os.Getenv(EC2_RABBITMQ)
	if Conf.RabbitMQ.Host == "" {
		ErrorHandler(GetFunction(), "Environment variable '"+EC2_RABBITMQ+"' is not set.")
	}
	Conf.Subscriber.Host = os.Getenv(EC2_SUBSCRIBER)
	if Conf.Subscriber.Host == "" {
		ErrorHandler(GetFunction(), "Environment variable '"+EC2_SUBSCRIBER+"' is not set.")
	}
	Conf.Publisher.Host = os.Getenv(EC2_PUBLISHER)
	if Conf.Publisher.Host == "" {
		ErrorHandler(GetFunction(), "Environment variable '"+EC2_PUBLISHER+"' is not set.")
	}

	// Configure non configuration environment variables
	envFileContent[EC2_RABBITMQ] = Conf.RabbitMQ.Host
	envFileContent[EC2_SUBSCRIBER] = Conf.Subscriber.Host
	envFileContent[EC2_PUBLISHER] = Conf.Publisher.Host
	envFileContent[ControlDir] = Conf.Environment.ControlDir
	envFileContent[GoRoot] = Conf.Environment.GoRoot

	// Configure configuration environment variables
	temp := ""
	IterateStruct("", Conf, &temp)
	confContent := strings.Split(temp, "\n")
	for line := 0; line < len(confContent); line++ {
		l := confContent[line]
		if strings.Index(l, "=") != -1 {
			p := strings.Split(confContent[line], "=")
			envFileContent[p[0]] = p[1]
		}
	}

	// sort environment variables
	type Pair struct {
		Key   string
		Value string
	}

	var pairs []Pair
	for k, v := range envFileContent {
		pairs = append(pairs, Pair{k, v})
	}

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Key < pairs[j].Key // For ascending order
		// return pairs[i].Value > pairs[j].Value // For descending order
	})
	for _, p := range pairs {
		fmt.Fprintf(file, p.Key+"="+p.Value+"\n")
	}
	return envFileContent
}

func SetEnvVariables(f map[string]string) {
	for k, v := range f {
		err := os.Setenv(k, v)
		if err != nil {
			ErrorHandler(GetFunction(), err.Error())
		}
	}
}

func EnvToConfig(v string, t reflect.Type) interface{} {
	x := os.Getenv(v)

	if x == "" {
		ErrorHandler(GetFunction(), v+" is not set.")
	}

	switch t.String() {
	case "string":
		return x
	case "int":
		temp, err := strconv.Atoi(x)
		if err != nil {
			ErrorHandler(GetFunction(), v+" is not set.")
		}
		return temp
	case "float64":
		temp, err := strconv.ParseFloat(x, 64)
		if err != nil {
			ErrorHandler(GetFunction(), v+" is not set.")
		}
		return temp
	case "[]float64":
		b := strings.Index(x, "[")
		e := strings.Index(x, "]")
		t1 := x[b+1 : e]
		t2 := strings.Split(t1, " ")
		temp := []float64{}
		for i := 0; i < len(t2); i++ {
			t, err := strconv.ParseFloat(t2[i], 64)
			if err != nil {
				ErrorHandler(GetFunction(), v+" error in parser variable")
			}
			temp = append(temp, t)
		}
		return temp
	default:
		ErrorHandler(GetFunction(), "Type '"+reflect.TypeOf(t).String()+"' of '"+v+"' is unknown.")
	}
	return nil
}

func UpdateConfig(Conf *config.Config) {
	fmt.Println("Updating config...")

	Conf.Environment.ControlDir = EnvToConfig(ControlDir, reflect.TypeOf(Conf.Environment.ControlDir)).(string)
	Conf.Environment.GoRoot = EnvToConfig(GoRoot, reflect.TypeOf(Conf.Environment.GoRoot)).(string)

	Conf.RabbitMQ.Host = EnvToConfig("RabbitmqConf_Host", reflect.TypeOf(Conf.RabbitMQ.Host)).(string)
	Conf.RabbitMQ.Port = EnvToConfig("RabbitmqConf_Port", reflect.TypeOf(Conf.RabbitMQ.Port)).(int)
	Conf.RabbitMQ.User = EnvToConfig("RabbitmqConf_User", reflect.TypeOf(Conf.RabbitMQ.User)).(string)
	Conf.RabbitMQ.Pwd = EnvToConfig("RabbitmqConf_Pwd", reflect.TypeOf(Conf.RabbitMQ.Pwd)).(string)
	Conf.RabbitMQ.QueueName = EnvToConfig("RabbitmqConf_QueueName", reflect.TypeOf(Conf.RabbitMQ.QueueName)).(string)

	Conf.Publisher.Host = EnvToConfig("PublisherConf_Host", reflect.TypeOf(Conf.Publisher.Host)).(string)
	Conf.Publisher.ExecutionEnv = EnvToConfig("PublisherConf_ExecutionEnv", reflect.TypeOf(Conf.Publisher.ExecutionEnv)).(string)

	Conf.Subscriber.Host = EnvToConfig("SubscriberConf_Host", reflect.TypeOf(Conf.Subscriber.Host)).(string)
	Conf.Subscriber.ExecutionEnv = EnvToConfig("SubscriberConf_ExecutionEnv", reflect.TypeOf(Conf.Subscriber.ExecutionEnv)).(string)

	Conf.PID.Direction = EnvToConfig("PIDConf_Direction", reflect.TypeOf(Conf.PID.Direction)).(float64)
	Conf.PID.Kp = EnvToConfig("PIDConf_Kp", reflect.TypeOf(Conf.PID.Kp)).(float64)
	Conf.PID.Ki = EnvToConfig("PIDConf_Ki", reflect.TypeOf(Conf.PID.Ki)).(float64)
	Conf.PID.Kd = EnvToConfig("PIDConf_Kd", reflect.TypeOf(Conf.PID.Kd)).(float64)
	Conf.PID.Max = EnvToConfig("PIDConf_Max", reflect.TypeOf(Conf.PID.Max)).(float64)
	Conf.PID.Min = EnvToConfig("PIDConf_Min", reflect.TypeOf(Conf.PID.Min)).(float64)

	Conf.NEURAL.DataTrainedPath = EnvToConfig("NeuralConf_DataTrainedPath", reflect.TypeOf(Conf.NEURAL.DataTrainedPath)).(string)
	Conf.NEURAL.DataTrainedFile = EnvToConfig("NeuralConf_DataTrainedFile", reflect.TypeOf(Conf.NEURAL.DataTrainedFile)).(string)
	Conf.NEURAL.DataTrainingPath = EnvToConfig("NeuralConf_DataTrainingPath", reflect.TypeOf(Conf.NEURAL.DataTrainingPath)).(string)
	Conf.NEURAL.DataTrainingFile = EnvToConfig("NeuralConf_DataTrainingFile", reflect.TypeOf(Conf.NEURAL.DataTrainingFile)).(string)

	Conf.Experiment.ExecutionType = EnvToConfig("ExpConf_ExecutionType", reflect.TypeOf(Conf.Experiment.ExecutionType)).(string)
	Conf.Experiment.SameLevelPc = EnvToConfig("ExpConf_SameLevelPc", reflect.TypeOf(Conf.Experiment.SameLevelPc)).(int)
	Conf.Experiment.Setpoint = EnvToConfig("ExpConf_Setpoint", reflect.TypeOf(Conf.Experiment.Setpoint)).(float64)
	Conf.Experiment.PC = EnvToConfig("ExpConf_PC", reflect.TypeOf(Conf.Experiment.PC)).(int)
	Conf.Experiment.MonitorInterval = EnvToConfig("ExpConf_MonitorInterval", reflect.TypeOf(Conf.Experiment.MonitorInterval)).(int)
	Conf.Experiment.ControllerType = EnvToConfig("ExpConf_ControllerType", reflect.TypeOf(Conf.Experiment.ControllerType)).(string)
	Conf.Experiment.DockerDataDir = EnvToConfig("ExpConf_DockerDataDir", reflect.TypeOf(Conf.Experiment.DockerDataDir)).(string)
	Conf.Experiment.DockerDataFile = EnvToConfig("ExpConf_DockerDataFile", reflect.TypeOf(Conf.Experiment.DockerDataFile)).(string)
	Conf.Experiment.SampleSize = EnvToConfig("ExpConf_SampleSize", reflect.TypeOf(Conf.Experiment.SampleSize)).(int)
	Conf.Experiment.PublishInterval = EnvToConfig("ExpConf_PublishInterval", reflect.TypeOf(Conf.Experiment.PublishInterval)).(int)
	Conf.Experiment.PublishIntervalSTD = EnvToConfig("ExpConf_PublishIntervalSTD", reflect.TypeOf(Conf.Experiment.PublishIntervalSTD)).(int)
	Conf.Experiment.NumberPublishers = EnvToConfig("ExpConf_NumberPublishers", reflect.TypeOf(Conf.Experiment.NumberPublishers)).(int)
	Conf.Experiment.PCStep = EnvToConfig("ExpConf_PCStep", reflect.TypeOf(Conf.Experiment.PCStep)).(int)
	Conf.Experiment.AdaptiveWindowSize = EnvToConfig("ExpConf_AdaptiveWindowSize", reflect.TypeOf(Conf.Experiment.AdaptiveWindowSize)).(int)
	Conf.Experiment.SameLevelSetpoint = EnvToConfig("ExpConf_SameLevelSetpoint", reflect.TypeOf(Conf.Experiment.SameLevelSetpoint)).(int)
	Conf.Experiment.Setpoints = EnvToConfig("ExpConf_Setpoints", reflect.TypeOf(Conf.Experiment.Setpoints)).([]float64)
	Conf.Experiment.QueueMaxLenght = EnvToConfig("ExpConf_QueueMaxLenght", reflect.TypeOf(Conf.Experiment.QueueMaxLenght)).(int)
	Conf.Experiment.IncDecPublishersStep = EnvToConfig("ExpConf_IncDecPublishersStep", reflect.TypeOf(Conf.Experiment.IncDecPublishersStep)).(int)
	Conf.Experiment.IncDecInterval = EnvToConfig("ExpConf_IncDecInterval", reflect.TypeOf(Conf.Experiment.IncDecInterval)).(int)
	Conf.Experiment.MinNumberOfPublishers = EnvToConfig("ExpConf_MinNumberOfPublishers", reflect.TypeOf(Conf.Experiment.MinNumberOfPublishers)).(int)
	Conf.Experiment.PublishersType = EnvToConfig("ExpConf_PublishersType", reflect.TypeOf(Conf.Experiment.PublishersType)).(string)
	Conf.Experiment.UseEnvironmentVariables = EnvToConfig("ExpConf_UseEnvironmentVariables", reflect.TypeOf(Conf.Experiment.UseEnvironmentVariables)).(int)

	Conf.Python.Path = EnvToConfig("PythonConf_Path", reflect.TypeOf(Conf.Python.Path)).(string)
	Conf.Python.Executable = EnvToConfig("PythonConf_Executable", reflect.TypeOf(Conf.Python.Executable)).(string)
	Conf.Python.Script = EnvToConfig("PythonConf_Script", reflect.TypeOf(Conf.Python.Script)).(string)
	Conf.Python.ErrorsFile = EnvToConfig("PythonConf_ErrorsFile", reflect.TypeOf(Conf.Python.ErrorsFile)).(string)
	Conf.Python.GainsFile = EnvToConfig("PythonConf_GainsFile", reflect.TypeOf(Conf.Python.GainsFile)).(string)
	Conf.Python.Mode = EnvToConfig("PythonConf_Mode", reflect.TypeOf(Conf.Python.Mode)).(string)
	Conf.Python.JitterOn = EnvToConfig("PythonConf_JitterOn", reflect.TypeOf(Conf.Python.JitterOn)).(string)
}

func IterateStruct(ns string, s interface{}, r *string) string {
	val := reflect.ValueOf(s)

	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		ErrorHandler(GetFunction(), "Not a struct")
	}

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := val.Type().Field(i)

		if field.Kind() != reflect.Struct {
			temp := fmt.Sprintf("%s_%s=%v\n", ns, fieldType.Name, field.Interface())
			//temp = strings.ReplaceAll(temp, "-", "_")
			*r += temp
		}
		if field.Kind() == reflect.Struct {
			IterateStruct(field.Type().Name(), field.Interface(), r) // Recurse for nested structs
		}
	}
	return *r
}

func GetBetterPC(setpoint int, d map[int]int) int {
	r := 0
	minorDiff := 1000000
	for rate := range d {
		if int(math.Abs(float64(rate-setpoint))) < minorDiff {
			minorDiff = int(math.Abs(float64(rate - setpoint)))
			r = d[rate]
		}
	}
	return r
}

func ReadTrainedData(path, file string) map[int]int {
	r := make(map[int]int)

	// REMOVE
	//ListFolder(path)

	//read file
	/*
		content, err := os.ReadFile(path + "/" + file)
		if err != nil {
			ErrorHandler(GetFunction(), "Failed to read file: '"+path+"/"+file+"'")
		}


		// convert lines into a map
		for _, line := range strings.Split(string(content), "\n") {
			// PC
			info := strings.Split(line, ";")
			pc, err := strconv.Atoi(strings.Trim(info[0], " "))
			if err != nil {
				ErrorHandler(GetFunction(), "Failed to parse rate")
			}

			// Rate
			temp := strings.Trim(info[1], " ")
			rate, err := strconv.Atoi(temp[:len(temp)-1])
			if err != nil {
				ErrorHandler(GetFunction(), "Failed to parse rate")
			}
			r[int(rate)] = int(pc)
		}
	*/

	f, err := os.Open(path + "/" + file)
	if err != nil {
		ErrorHandler(GetFunction(), "Error opening file:"+err.Error())
	}
	defer f.Close() // Ensure the file is closed after the function exits

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Text()
		info := strings.Split(line, ";")
		pc, err := strconv.Atoi(strings.Trim(info[0], " "))
		if err != nil {
			ErrorHandler(GetFunction(), "Failed to parse rate")
		}

		// Rate
		temp := strings.Trim(info[1], " ")
		rate, err := strconv.Atoi(temp[:len(temp)])
		if err != nil {
			ErrorHandler(GetFunction(), "Failed to parse rate")
		}

		r[int(rate)] = int(pc)
	}

	return r
}

func ListFolder(dirPath string) {

	entries, err := os.ReadDir(dirPath)
	if err != nil {
		log.Fatalf("Failed to read directory: %v", err)
	}

	fmt.Printf("Contents of folder '%s':\n", dirPath)
	for _, entry := range entries {
		if entry.IsDir() {
			fmt.Printf("- Directory: %s\n", entry.Name())
		} else {
			fmt.Printf("- File: %s\n", entry.Name())
		}
	}
}

func GetNNGains(path, executable, script, errorsFile, gainsFile string, oldKp, oldKi, oldKd float64, mode string, jitterOn string) (float64, float64, float64) {
	// Run the command and capture its output
	oldKpStr := strconv.FormatFloat(oldKp, 'f', 10, 64)
	oldKiStr := strconv.FormatFloat(oldKi, 'f', 10, 64)
	oldKdStr := strconv.FormatFloat(oldKd, 'f', 10, 64)

	cmd := exec.Command(executable,
		script,
		"--mode", mode,
		"--kp", oldKpStr,
		"--ki", oldKiStr,
		"--kd", oldKdStr,
		"--csv", errorsFile,
		"--jitter_on", jitterOn,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		ErrorHandler(GetFunction(), "Error executing Python script:"+string(output))
	}

	// filter warning messages
	o := string(output)
	i := strings.Index(o, "{'kp':") // TODO
	o = o[i:]
	o = strings.ReplaceAll(o, "'", "\"") // TODO
	jsonData := []byte(o)

	// obtain gains inside json
	pythonResp := ControlGains{}
	err = json.Unmarshal(jsonData, &pythonResp)
	if err != nil {
		ErrorHandler(GetFunction(), "Error in processing JSON of Python script")
	}

	return pythonResp.Kp, pythonResp.Ki, pythonResp.Kd
}

func ListTxtFile(path string, fileName string) {
	// Open & read control gains file
	f, err := os.Open(path + "/" + fileName)
	if err != nil {
		ErrorHandler(GetFunction(), "Error opening file:"+err.Error())
	}
	defer f.Close() // Ensure the file is closed after the function exits

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Text()
		fmt.Println(line)
	}

	return
}

// QueueInfo holds the relevant queue fields from RabbitMQ Management API.
type QueueInfo struct {
	Name     string `json:"name"`
	Messages int    `json:"messages"`
	State    string `json:"state"`
}

// GetQueueInfo retrieves queue information (including message count)
// from the RabbitMQ Management API.
func GetQueueInfo(apiURL, username, password, vhost, queue string) (*QueueInfo, error) {
	// Encode vhost and queue for URL safety
	encodedVhost := url.PathEscape(vhost)
	encodedQueue := url.PathEscape(queue)

	fullURL := fmt.Sprintf("%s/api/queues/%s/%s", apiURL, encodedVhost, encodedQueue)

	req, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.SetBasicAuth(username, password)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status: %s", resp.Status)
	}

	var info QueueInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &info, nil
}

func ShowConfiguration(caller string, c config.Config) {
	if caller == Subscriber {
		fmt.Println("************** SUBSCRIBER ******************")
		fmt.Println("Execution Environment : ", c.Subscriber.ExecutionEnv)
		fmt.Println("RabbitMQ Host         : ", c.RabbitMQ.Host)
		fmt.Println("Control Dir           : ", c.Environment.ControlDir)
		fmt.Println("Execution Type        : ", c.Experiment.ExecutionType)
		fmt.Println("Sample Size           : ", c.Experiment.SampleSize)
		fmt.Println("Same Level PC         : ", c.Experiment.SameLevelPc)
		fmt.Println("Same Level Setpoint   : ", c.Experiment.SameLevelSetpoint)
		fmt.Println("Prefetch Count        : ", c.Experiment.PC)
		fmt.Println("Setpoint              : ", c.Experiment.Setpoint)
		fmt.Println("Monitor Interval      : ", c.Experiment.MonitorInterval, "s")
		fmt.Println("Controller Type       : ", c.Experiment.ControllerType)
		fmt.Println("Setpoint(s)           : ", c.Experiment.Setpoints)
		if c.Experiment.ControllerType == PID || c.Experiment.ControllerType == AdaptivePID {
			fmt.Println("Kp,Ki,Kd              : ", c.PID.Kp, c.PID.Ki, c.PID.Kd)
			fmt.Println("Window Size           : ", c.Experiment.AdaptiveWindowSize)
			fmt.Println("Neural Mode           : ", c.Python.Mode)
			fmt.Println("Python Script         : ", c.Python.Script)
			fmt.Println("Jitter (0=OFF,1=ON)   : ", c.Python.JitterOn)
		}
		fmt.Println("Queue Max Size        : ", c.Experiment.QueueMaxLenght)
		fmt.Println("********************************************")
	} else {
		ErrorHandler(GetFunction(), "Failed to show configuration:: Caller '"+caller+"' is invalid")
	}
}

func CreateOutputFile(conf config.Config, fileName string) *os.File {
	fp := ""
	if conf.Subscriber.ExecutionEnv == EC2 {
		fp = conf.Experiment.DockerDataDir + "/" + fileName
	} else {
		fp = conf.Environment.ControlDir + "/" + fileName
	}
	expFile, err := os.OpenFile(fp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		ErrorHandler(GetFunction(), "Error opening file:"+fp)
	}

	return expFile
}

func SampleNormal(rng *rand.Rand, mean float64, std float64) float64 {
	return mean + std*rng.NormFloat64()
}

// Pulsar

type DispatchRate struct {
	DispatchThrottlingRateInMsg  int64 `json:"dispatchThrottlingRateInMsg"`
	DispatchThrottlingRateInByte int64 `json:"dispatchThrottlingRateInByte"`
	RatePeriodInSecond           int   `json:"ratePeriodInSecond"`
	RelativeToPublishRate        bool  `json:"relativeToPublishRate"`
}

type DispatchRateController struct {
	AdminURL  string
	Tenant    string
	Namespace string
	Token     string
	Client    *http.Client
}

func (c *DispatchRateController) SetDispatchRate(
	ctx context.Context,
	messagesPerSecond int64,
) error {
	policy := DispatchRate{
		DispatchThrottlingRateInMsg:  messagesPerSecond,
		DispatchThrottlingRateInByte: -1,
		RatePeriodInSecond:           1,
		RelativeToPublishRate:        false,
	}

	body, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("encode dispatch policy: %w", err)
	}

	url := fmt.Sprintf(
		"%s/admin/v2/namespaces/%s/%s/dispatchRate",
		c.AdminURL,
		c.Tenant,
		c.Namespace,
	)

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		url,
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}

	resp, err := c.Client.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		responseBody, _ := io.ReadAll(resp.Body)

		return fmt.Errorf(
			"admin API returned %s: %s",
			resp.Status,
			responseBody,
		)
	}

	return nil
}
