package sqsv2_test

import (
	"context"
	"errors"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/brokers/sqsv2"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/retry"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

var (
	testBroker           iface.Broker
	testAWSSQSBroker     *sqsv2.Broker
	errAWSSQSBroker      *sqsv2.Broker
	cnf                  *config.Config
	receiveMessageOutput *awssqs.ReceiveMessageOutput
)

func init() {
	testAWSSQSBroker = sqsv2.TestAWSSQSBroker
	errAWSSQSBroker = sqsv2.ErrAWSSQSBroker
	cnf = sqsv2.TestConf
	receiveMessageOutput = sqsv2.ReceiveMessageOutput
	testBroker = sqsv2.New(cnf)
}

func TestNewAWSSQSBroker(t *testing.T) {
	assert.IsType(t, testAWSSQSBroker, testBroker)
}

func TestPrivateFunc_continueReceivingMessages(t *testing.T) {
	qURL := testAWSSQSBroker.DefaultQueueURLForTest()
	deliveries := make(chan *awssqs.ReceiveMessageOutput)
	firstStep := make(chan int)
	nextStep := make(chan int)
	go func() {
		stopReceivingChan := testAWSSQSBroker.GetStopReceivingChanForTest()
		firstStep <- 1
		stopReceivingChan <- 1
	}()

	var (
		whetherContinue bool
		err             error
	)
	<-firstStep
	// Test the case that a signal was received from stopReceivingChan
	go func() {
		whetherContinue, err = testAWSSQSBroker.ContinueReceivingMessagesForTest(qURL, deliveries)
		nextStep <- 1
	}()
	<-nextStep
	assert.False(t, whetherContinue)
	assert.Nil(t, err)

	// Test the default condition
	whetherContinue, err = testAWSSQSBroker.ContinueReceivingMessagesForTest(qURL, deliveries)
	assert.True(t, whetherContinue)
	assert.Nil(t, err)

	// Test the error
	whetherContinue, err = errAWSSQSBroker.ContinueReceivingMessagesForTest(qURL, deliveries)
	assert.True(t, whetherContinue)
	assert.NotNil(t, err)

	// Test when there is no message
	outputCopy := *receiveMessageOutput
	receiveMessageOutput.Messages = []types.Message{}
	whetherContinue, err = testAWSSQSBroker.ContinueReceivingMessagesForTest(qURL, deliveries)
	assert.True(t, whetherContinue)
	assert.Nil(t, err)
	// recover original value
	*receiveMessageOutput = outputCopy

}

func TestPrivateFunc_consume(t *testing.T) {
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}
	pool := make(chan struct{}, 0)
	wk := server1.NewWorker("sms_worker", 0)
	deliveries := make(chan *sqsv2.ReceivedMessages)
	outputCopy := *receiveMessageOutput
	outputCopy.Messages = []types.Message{}
	receivedMsg := sqsv2.ReceivedMessages{Delivery: &outputCopy}
	go func() { deliveries <- &receivedMsg }()

	// an infinite loop will be executed only when there is no error
	err = testAWSSQSBroker.ConsumeForTest(deliveries, 0, wk, pool)
	assert.NotNil(t, err)

}

func TestPrivateFunc_consumeOne(t *testing.T) {
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}
	wk := server1.NewWorker("sms_worker", 0)
	outputCopy := *receiveMessageOutput
	outputCopy.Messages = []types.Message{}
	receivedMsg := sqsv2.ReceivedMessages{Delivery: &outputCopy}
	err = testAWSSQSBroker.ConsumeOneForTest(&receivedMsg, wk)
	assert.NotNil(t, err)

	err = testAWSSQSBroker.ConsumeOneForTest(&receivedMsg, wk)
	assert.NotNil(t, err)

	outputCopy.Messages = []types.Message{
		{
			Body: aws.String("foo message"),
		},
	}
	receivedMsg = sqsv2.ReceivedMessages{Delivery: &outputCopy}
	err = testAWSSQSBroker.ConsumeOneForTest(&receivedMsg, wk)
	assert.NotNil(t, err)
}

func TestPrivateFunc_initializePool(t *testing.T) {
	concurrency := 9
	pool := make(chan struct{}, concurrency)
	testAWSSQSBroker.InitializePoolForTest(pool, concurrency)
	assert.Len(t, pool, concurrency)
}

func TestPrivateFunc_startConsuming(t *testing.T) {
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}

	wk := server1.NewWorker("sms_worker", 0)
	retryFunc := testAWSSQSBroker.GetRetryFuncForTest()
	stopChan := testAWSSQSBroker.GetStopChanForTest()
	retryStopChan := testAWSSQSBroker.GetRetryStopChanForTest()
	assert.Nil(t, retryFunc)
	testAWSSQSBroker.StartConsumingForTest("fooTag", 1, wk)
	assert.IsType(t, retryFunc, retry.Closure())
	assert.Equal(t, len(stopChan), 0)
	assert.Equal(t, len(retryStopChan), 0)
}

func TestPrivateFuncDefaultQueueURL(t *testing.T) {
	qURL := testAWSSQSBroker.DefaultQueueURLForTest()

	assert.EqualValues(t, *qURL, "https://sqs.foo.amazonaws.com.cn/test_queue")
}

func TestPrivateFunc_stopReceiving(t *testing.T) {
	go testAWSSQSBroker.StopReceivingForTest()
	stopReceivingChan := testAWSSQSBroker.GetStopReceivingChanForTest()
	assert.NotNil(t, <-stopReceivingChan)
}

func TestPrivateFunc_receiveMessage(t *testing.T) {
	qURL := testAWSSQSBroker.DefaultQueueURLForTest()
	output, err := testAWSSQSBroker.ReceiveMessageForTest(qURL)
	assert.Nil(t, err)
	assert.Equal(t, receiveMessageOutput, output)
}

func TestPrivateFunc_consumeDeliveries(t *testing.T) {
	concurrency := 0
	pool := make(chan struct{}, concurrency)
	errorsChan := make(chan error)
	deliveries := make(chan *sqsv2.ReceivedMessages)
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}
	wk := server1.NewWorker("sms_worker", 0)
	outputCopy := *receiveMessageOutput
	outputCopy.Messages = []types.Message{}
	receivedMsg := sqsv2.ReceivedMessages{Delivery: &outputCopy}
	go func() { deliveries <- &receivedMsg }()
	whetherContinue, err := testAWSSQSBroker.ConsumeDeliveriesForTest(deliveries, concurrency, wk, pool, errorsChan)
	assert.True(t, whetherContinue)
	assert.Nil(t, err)

	go func() { errorsChan <- errors.New("foo error") }()
	whetherContinue, err = testAWSSQSBroker.ConsumeDeliveriesForTest(deliveries, concurrency, wk, pool, errorsChan)
	assert.False(t, whetherContinue)
	assert.NotNil(t, err)

	go func() { testAWSSQSBroker.GetStopChanForTest() <- 1 }()
	whetherContinue, err = testAWSSQSBroker.ConsumeDeliveriesForTest(deliveries, concurrency, wk, pool, errorsChan)
	assert.False(t, whetherContinue)
	assert.Nil(t, err)

	outputCopy = *receiveMessageOutput
	outputCopy.Messages = []types.Message{}
	receivedMsg = sqsv2.ReceivedMessages{Delivery: &outputCopy}
	go func() { deliveries <- &receivedMsg }()
	whetherContinue, err = testAWSSQSBroker.ConsumeDeliveriesForTest(deliveries, concurrency, wk, pool, errorsChan)
	e := <-errorsChan
	assert.True(t, whetherContinue)
	assert.NotNil(t, e)
	assert.Nil(t, err)

	// using a wait group and a channel to fix the racing problem
	outputCopy = *receiveMessageOutput
	outputCopy.Messages = []types.Message{}
	receivedMsg = sqsv2.ReceivedMessages{Delivery: &outputCopy}
	var wg sync.WaitGroup
	wg.Add(1)
	nextStep := make(chan bool, 1)
	go func() {
		defer wg.Done()
		// nextStep <- true runs after defer wg.Done(), to make sure the next go routine runs after this go routine
		nextStep <- true
		deliveries <- &receivedMsg
	}()
	if <-nextStep {
		// <-pool will block the routine in the following steps, so pool <- struct{}{} will be executed for sure
		go func() { wg.Wait(); pool <- struct{}{} }()
	}
	whetherContinue, err = testAWSSQSBroker.ConsumeDeliveriesForTest(deliveries, concurrency, wk, pool, errorsChan)
	// the pool shouldn't be consumed
	p := <-pool
	assert.True(t, whetherContinue)
	assert.NotNil(t, p)
	assert.Nil(t, err)
}

func TestPrivateFunc_deleteOne(t *testing.T) {
	outputCopy := *receiveMessageOutput
	outputCopy.Messages = []types.Message{
		{
			Body:          aws.String("foo message"),
			ReceiptHandle: aws.String("receipthandle"),
		},
	}
	receivedMsg := sqsv2.ReceivedMessages{Delivery: &outputCopy}
	err := testAWSSQSBroker.DeleteOneForTest(&receivedMsg)
	assert.Nil(t, err)

	err = errAWSSQSBroker.DeleteOneForTest(&receivedMsg)
	assert.NotNil(t, err)
}

func Test_CustomQueueName(t *testing.T) {
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}

	wk := server1.NewWorker("test-worker", 0)
	qURL := testAWSSQSBroker.GetQueueURLForTest(wk)
	assert.Equal(t, qURL, testAWSSQSBroker.DefaultQueueURLForTest(), "")

	wk2 := server1.NewCustomQueueWorker("test-worker", 0, "my-custom-queue")
	qURL2 := testAWSSQSBroker.GetQueueURLForTest(wk2)
	assert.Equal(t, qURL2, testAWSSQSBroker.GetCustomQueueURL("my-custom-queue"), "")
}

func TestPrivateFunc_consumeWithConcurrency(t *testing.T) {

	msg := `{
        "UUID": "uuid-dummy-task",
        "Name": "test-task",
        "RoutingKey": "dummy-routing"
	}
	`

	testResp := "47f8b355-5115-4b45-b33a-439016400411"
	output := make(chan string) // The output channel

	cnf.ResultBackend = "eager"
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}
	err = server1.RegisterTask("test-task", func(ctx context.Context) error {
		output <- testResp

		return nil
	})
	testAWSSQSBroker.SetRegisteredTaskNames([]string{"test-task"})
	assert.NoError(t, err)
	pool := make(chan struct{}, 1)
	pool <- struct{}{}
	wk := server1.NewWorker("sms_worker", 1)
	deliveries := make(chan *sqsv2.ReceivedMessages)
	outputCopy := *receiveMessageOutput
	outputCopy.Messages = []types.Message{
		{
			MessageId: aws.String("test-sqs-msg1"),
			Body:      aws.String(msg),
		},
	}
	receivedMsg := sqsv2.ReceivedMessages{Delivery: &outputCopy}
	go func() {
		deliveries <- &receivedMsg

	}()

	go func() {
		err = testAWSSQSBroker.ConsumeForTest(deliveries, 1, wk, pool)
	}()

	select {
	case resp := <-output:
		assert.Equal(t, testResp, resp)

	case <-time.After(10 * time.Second):
		// call timed out
		t.Fatal("task not processed in 10 seconds")
	}
}

type roundRobinQueues struct {
	queues       []string
	currentIndex int
}

func NewRoundRobinQueues(queues []string) *roundRobinQueues {
	return &roundRobinQueues{
		queues:       queues,
		currentIndex: -1,
	}
}

func (r *roundRobinQueues) Peek() string {
	return r.queues[r.currentIndex]
}

func (r *roundRobinQueues) Next() string {
	r.currentIndex += 1
	if r.currentIndex >= len(r.queues) {
		r.currentIndex = 0
	}

	q := r.queues[r.currentIndex]
	return q
}

func TestPrivateFunc_consumeWithRoundRobinQueues(t *testing.T) {
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}

	w := server1.NewWorker("test-worker", 0)

	// Assigning a getQueueHandler to `Next` method of roundRobinQueues
	rr := NewRoundRobinQueues([]string{"custom-queue-0", "custom-queue-1", "custom-queue-2", "custom-queue-3"})
	w.SetGetQueueHandler(rr.Next)

	for i := 0; i < 5; i++ {
		// the queue url of the broker should match the current queue url of roundRobin
		// and thus queues are being utilized in round-robin fashion
		qURL := testAWSSQSBroker.GetQueueURLForTest(w)
		assert.Equal(t, qURL, testAWSSQSBroker.GetCustomQueueURL(rr.Peek()))
	}
}
