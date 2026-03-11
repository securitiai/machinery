package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RichardKnop/redsync"
	"github.com/go-redis/redis"
	"github.com/spf13/cast"

	"github.com/RichardKnop/machinery/v1/brokers/errs"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
)

const (
	messageVisibilitySet = "message-visibility-set"
	hSetExpirySeconds    = 604800 // 1 week
)

// popResult holds the result of a Lua pop operation
type popResult struct {
	hash         string
	message      string
	receiveCount int
}

// popLuaScript atomically pops a message from the queue, computes its hash
// using redis.sha1hex(), stores metadata in an HSET, and adds it to the
// visibility timeout set. Returns the hash, message body, and current
// receive count so the caller doesn't need separate lookups.
var popLuaScript = `
local queue = KEYS[1]
local visibility_set = KEYS[2]

local visibility_timeout = tonumber(ARGV[1])
local queue_name = ARGV[2]
local current_time = tonumber(ARGV[3])
local hset_expiry = tonumber(ARGV[4])

-- Get first message from queue
local message = redis.call('LRANGE', queue, 0, 0)
if #message == 0 then
    return nil
end

local queue_message = message[1]

-- Compute hash atomically inside Lua to avoid race conditions
local task_hash = 'task_' .. redis.sha1hex(queue_message)

-- Calculate visibility timeout timestamp
local visibility_timestamp = current_time + visibility_timeout

-- Store message metadata in hash
redis.call('HMSET', task_hash, 'message', queue_message, 'queue', queue_name)

-- Add to visibility set with expiry timestamp as score
redis.call('ZADD', visibility_set, visibility_timestamp, task_hash)

-- Set expiry on the HSET to prevent leaks
if hset_expiry > 0 then
    redis.call('EXPIRE', task_hash, hset_expiry)
end

-- Get current receive count (visibility_counter) from the HSET
local receive_count = redis.call('HGET', task_hash, 'visibility_counter')
if not receive_count then
    receive_count = 0
else
    receive_count = tonumber(receive_count)
end

-- Remove message from queue
redis.call('LREM', queue, 1, queue_message)

return {task_hash, queue_message, receive_count}
`

// BrokerGR_DLQ represents a Redis broker using go-redis, with enhancements for DLQ support
type BrokerGR_DLQ struct {
	common.Broker
	rclient      redis.UniversalClient
	consumingWG  sync.WaitGroup // wait group to make sure whole consumption completes
	processingWG sync.WaitGroup // use wait group to make sure task processing completes
	delayedWG    sync.WaitGroup
	// If set, path to a socket file overrides hostname
	socketPath string
	redsync    *redsync.Redsync
	redisOnce  sync.Once
}

// NewGR_DLQ creates new Broker instance
func NewGR_DLQ(cnf *config.Config, addrs []string, password string, db int) iface.Broker {
	b := &BrokerGR_DLQ{Broker: common.NewBroker(cnf)}

	ropt := &redis.UniversalOptions{
		Addrs:           addrs,
		DB:              db,
		Password:        password,
		ReadTimeout:     time.Duration(cnf.Redis.ReadTimeout) * time.Second,
		WriteTimeout:    time.Duration(cnf.Redis.WriteTimeout) * time.Second,
		DialTimeout:     time.Duration(cnf.Redis.ConnectTimeout) * time.Second,
		IdleTimeout:     time.Duration(cnf.Redis.IdleTimeout) * time.Second,
		MinIdleConns:    cnf.Redis.MinIdleConns,
		MinRetryBackoff: time.Duration(cnf.Redis.MinRetryBackoff) * time.Millisecond,
		MaxRetryBackoff: time.Duration(cnf.Redis.MaxRetryBackoff) * time.Millisecond,
		MaxRetries:      cnf.Redis.MaxRetries,
		PoolSize:        cnf.Redis.PoolSize,
		TLSConfig:       cnf.TLSConfig,
	}
	if cnf.Redis != nil {
		// if we're specifying MasterName here, then we'll always connect to db 0, since provided db is ignored in cluster mode
		ropt.MasterName = cnf.Redis.MasterName
	}

	b.rclient = redis.NewUniversalClient(ropt)

	if cnf.Redis.DelayedTasksKey != "" {
		redisDelayedTasksKey = cnf.Redis.DelayedTasksKey
	}
	return b
}

// StartConsuming enters a loop and waits for incoming messages
func (b *BrokerGR_DLQ) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (bool, error) {
	b.consumingWG.Add(1)
	defer b.consumingWG.Done()

	if concurrency < 1 {
		concurrency = 1
	}

	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)

	// Ping the server to make sure connection is live
	_, err := b.rclient.Ping().Result()
	if err != nil {
		b.GetRetryFunc()(b.GetRetryStopChan())

		// Return err if retry is still true.
		// If retry is false, broker.StopConsuming() has been called and
		// therefore Redis might have been stopped. Return nil exit
		// StartConsuming()
		if b.GetRetry() {
			return b.GetRetry(), err
		}
		return b.GetRetry(), errs.ErrConsumerStopped
	}

	// Channel to which we will push tasks ready for processing by worker
	deliveries := make(chan popResult, concurrency)
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}

	// A receiving goroutine keeps popping messages from the queue using a Lua script.
	// The Lua script atomically pops the message, computes its hash, and returns
	// the hash, message body, and receive count.
	go func() {

		log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				close(deliveries)
				return
			case <-pool:
				result, _ := b.nextTask(getQueueGR(b.GetConfig(), taskProcessor))
				if result != nil {
					deliveries <- *result
				} else {
					pool <- struct{}{}
				}
			}
		}
	}()

	// A goroutine to watch for delayed tasks and push them to deliveries
	// channel for consumption by the worker
	b.delayedWG.Add(1)
	go func() {
		defer b.delayedWG.Done()

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				return
			default:
				task, err := b.nextDelayedTask(redisDelayedTasksKey)
				if err != nil {
					continue
				}

				signature := new(tasks.Signature)
				decoder := json.NewDecoder(bytes.NewReader(task))
				decoder.UseNumber()
				if err := decoder.Decode(signature); err != nil {
					log.ERROR.Print(errs.NewErrCouldNotUnmarshaTaskSignature(task, err))
				}

				if err := b.Publish(context.Background(), signature); err != nil {
					log.ERROR.Print(err)
				}
			}
		}
	}()

	if err := b.consume(deliveries, concurrency, taskProcessor, pool); err != nil {
		return b.GetRetry(), err
	}

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()

	return b.GetRetry(), nil
}

// StopConsuming quits the loop
func (b *BrokerGR_DLQ) StopConsuming() {
	b.Broker.StopConsuming()
	// Waiting for the delayed tasks goroutine to have stopped
	b.delayedWG.Wait()
	// Waiting for consumption to finish
	b.consumingWG.Wait()

	b.rclient.Close()
}

// Publish places a new message on the default queue
func (b *BrokerGR_DLQ) Publish(ctx context.Context, signature *tasks.Signature) error {
	// Adjust routing key (this decides which queue the message will be published to)
	b.Broker.AdjustRoutingKey(signature)

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	if signature.ETA != nil {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			score := signature.ETA.UnixNano()
			err = b.rclient.ZAdd(redisDelayedTasksKey, redis.Z{Score: float64(score), Member: msg}).Err()
			return err
		}
	}

	err = b.rclient.RPush(signature.RoutingKey, msg).Err()
	return err
}

// GetPendingTasks returns a slice of task signatures waiting in the queue
func (b *BrokerGR_DLQ) GetPendingTasks(queue string) ([]*tasks.Signature, error) {

	if queue == "" {
		queue = b.GetConfig().DefaultQueue
	}
	results, err := b.rclient.LRange(queue, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	taskSignatures := make([]*tasks.Signature, len(results))
	for i, result := range results {
		signature := new(tasks.Signature)
		decoder := json.NewDecoder(strings.NewReader(result))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return nil, err
		}
		taskSignatures[i] = signature
	}
	return taskSignatures, nil
}

// GetDelayedTasks returns a slice of task signatures that are scheduled, but not yet in the queue
func (b *BrokerGR_DLQ) GetDelayedTasks() ([]*tasks.Signature, error) {
	results, err := b.rclient.ZRange(redisDelayedTasksKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	taskSignatures := make([]*tasks.Signature, len(results))
	for i, result := range results {
		signature := new(tasks.Signature)
		decoder := json.NewDecoder(strings.NewReader(result))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return nil, err
		}
		taskSignatures[i] = signature
	}
	return taskSignatures, nil
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *BrokerGR_DLQ) consume(deliveries <-chan popResult, concurrency int, taskProcessor iface.TaskProcessor, pool chan struct{}) error {
	errorsChan := make(chan error)

	for {
		select {
		case err := <-errorsChan:
			return err
		case d, open := <-deliveries:
			if !open {
				return nil
			}

			b.processingWG.Add(1)

			// Consume the task inside a goroutine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d, taskProcessor); err != nil {
					errorsChan <- err
				}

				b.processingWG.Done()

				if concurrency > 0 {
					// give slot back to pool
					pool <- struct{}{}
				}
			}()
		}
	}
}

// consumeOne processes a single message using TaskProcessor
func (b *BrokerGR_DLQ) consumeOne(delivery popResult, taskProcessor iface.TaskProcessor) error {
	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader([]byte(delivery.message)))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return errs.NewErrCouldNotUnmarshaTaskSignature([]byte(delivery.message), err)
	}
	// propagating hash UUID for possible application usage, for example, refreshing visibility
	oldUuid := signature.UUID
	signature.UUID = delivery.hash

	if !b.IsTaskRegistered(signature.Name) {
		log.INFO.Printf("Task not registered with this worker. message: %+v", signature)
		// deleting sig so that this isn't picked up by redis-reaper later
		err := b.deleteOne(signature)
		if err != nil {
			log.INFO.Printf("Couldn't delete signature for task, error: %v", err)
			return err
		}
		return nil
	}

	// receiveCount comes from the Lua script (visibility_counter from HSET).
	// Increment before adding to signature since ApproximateReceiveCount is the number
	// of times a message is received, whereas the HSET value represents the number of
	// retries which is one less than ApproximateReceiveCount.
	receiveCount := delivery.receiveCount + 1
	receiveCountString := cast.ToString(receiveCount)

	signature.Attributes = map[string]*string{}
	signature.Attributes["ApproximateReceiveCount"] = &receiveCountString

	log.DEBUG.Printf("Received new message: %+v", signature)
	log.INFO.Printf("Processing task. Old UUID: %s New UUID: %s", oldUuid, signature.UUID)

	if err := taskProcessor.Process(signature); err != nil {
		// stop task deletion in case we want to send messages to dlq in redis
		if err == errs.ErrStopTaskDeletion {
			return nil
		}
		return err
	}

	if err := b.deleteOne(signature); err != nil {
		log.ERROR.Printf("error when deleting the delivery. Error=%s", err)
		return err
	}

	return nil
}

// nextTask pops next available task from the default queue using a Lua script
// for atomicity. The hash is computed inside the Lua script using redis.sha1hex()
// to avoid race conditions between peeking and popping.
func (b *BrokerGR_DLQ) nextTask(queue string) (*popResult, error) {

	pollPeriodMilliseconds := 1000 // default poll period for normal tasks
	if b.GetConfig().Redis != nil {
		configuredPollPeriod := b.GetConfig().Redis.NormalTasksPollPeriod
		if configuredPollPeriod > 0 {
			pollPeriodMilliseconds = configuredPollPeriod
		}
	}
	pollPeriod := time.Duration(pollPeriodMilliseconds) * time.Millisecond
	visibilityTimeout := *b.GetConfig().Redis.VisibilityTimeout
	if visibilityTimeout <= 0 {
		visibilityTimeout = 60
	}

	keys := []string{queue, messageVisibilitySet}
	args := []interface{}{
		visibilityTimeout,
		queue,
		time.Now().Unix(),
		hSetExpirySeconds,
	}

	scriptResult, err := b.rclient.Eval(popLuaScript, keys, args...).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			time.Sleep(pollPeriod)
		}
		return nil, err
	}

	resultArray, ok := scriptResult.([]interface{})
	if !ok || len(resultArray) != 3 {
		return nil, fmt.Errorf("unexpected result format from pop Lua script")
	}

	hash, ok := resultArray[0].(string)
	if !ok {
		return nil, fmt.Errorf("invalid hash in pop Lua script result")
	}

	message, ok := resultArray[1].(string)
	if !ok {
		return nil, fmt.Errorf("invalid message in pop Lua script result")
	}

	receiveCount := 0
	if rc, ok := resultArray[2].(int64); ok {
		receiveCount = int(rc)
	}

	return &popResult{
		hash:         hash,
		message:      message,
		receiveCount: receiveCount,
	}, nil
}

// nextDelayedTask pops a value from the ZSET key using WATCH/MULTI/EXEC commands.
func (b *BrokerGR_DLQ) nextDelayedTask(key string) (result []byte, err error) {

	//pipe := b.rclient.Pipeline()
	//
	//defer func() {
	//	// Return connection to normal state on error.
	//	// https://redis.io/commands/discard
	//	if err != nil {
	//		pipe.Discard()
	//	}
	//}()

	var items []string

	pollPeriod := 500 // default poll period for delayed tasks
	if b.GetConfig().Redis != nil {
		configuredPollPeriod := b.GetConfig().Redis.DelayedTasksPollPeriod
		// the default period is 0, which bombards redis with requests, despite
		// our intention of doing the opposite
		if configuredPollPeriod > 0 {
			pollPeriod = configuredPollPeriod
		}
	}

	for {
		// Space out queries to ZSET so we don't bombard redis
		// server with relentless ZRANGEBYSCOREs
		time.Sleep(time.Duration(pollPeriod) * time.Millisecond)
		watchFunc := func(tx *redis.Tx) error {

			now := time.Now().UTC().UnixNano()

			// https://redis.io/commands/zrangebyscore
			items, err = tx.ZRevRangeByScore(key, redis.ZRangeBy{
				Min: "0", Max: strconv.FormatInt(now, 10), Offset: 0, Count: 1,
			}).Result()
			if err != nil {
				return err
			}
			if len(items) != 1 {
				return redis.Nil
			}
			_, err = tx.TxPipelined(func(pipe redis.Pipeliner) error {
				return pipe.ZRem(key, items[0]).Err()
			})
			return err
		}
		if err = b.rclient.Watch(watchFunc, key); err != nil {
			return
		}
		result = []byte(items[0])
		break
	}

	return
}

// deleteOne is a method to delete a redis message from the message visibility set as well as its temporary HSET
func (b *BrokerGR_DLQ) deleteOne(signature *tasks.Signature) error {

	gHash := signature.UUID
	watchFunc := func(tx *redis.Tx) error {
		if err := b.rclient.Del(gHash).Err(); err != nil {
			return err
		}
		if err := b.rclient.ZRem(messageVisibilitySet, gHash).Err(); err != nil {
			return err
		}
		return nil
	}
	// not watching gHash, because do not want transaction to cancel in any case
	return b.rclient.Watch(watchFunc)
}
