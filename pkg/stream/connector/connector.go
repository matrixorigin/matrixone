// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package moconnector

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	mokafka "github.com/matrixorigin/matrixone/pkg/stream/adapter/kafka"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"go.uber.org/zap"
)

func getBufferLimit(bufferLimitStr string) int {
	bufferLimit, err := strconv.Atoi(bufferLimitStr) // Convert the string to an integer
	if err != nil {
		// Handle the error, perhaps set to default if the conversion fails
		bufferLimit = 1
	}
	return bufferLimit
}

func getTimeWindow(timeWindowStr string) int {
	timeWindow, err := strconv.Atoi(timeWindowStr) // Convert the string to an integer
	if err != nil {
		// Handle the error, perhaps set to default if the conversion fails
		timeWindow = 1000 // 1second, 1000ms
	}
	return timeWindow
}

func KafkaSinkConnectorExecutor(
	logger *zap.Logger,
	ts taskservice.TaskService,
	ieFactory func() ie.InternalExecutor,
	attachToTask func(context.Context, uint64, taskservice.ActiveRoutine) error,
) func(context.Context, task.Task) error {
	return func(ctx context.Context, t task.Task) error {
		ctx1, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		tasks, err := ts.QueryDaemonTask(ctx1,
			taskservice.WithTaskIDCond(taskservice.EQ, t.GetID()),
		)
		if err != nil {
			return err
		}
		if len(tasks) != 1 {
			return moerr.NewInternalErrorf(ctx, "invalid tasks count %d", len(tasks))
		}
		details, ok := tasks[0].Details.Details.(*task.Details_Connector)
		if !ok {
			return moerr.NewInternalError(ctx, "invalid details type")
		}
		options := details.Connector.Options
		fullTableName := details.Connector.TableName
		// Set database and table name for options.
		ss := strings.Split(fullTableName, ".")
		options[mokafka.DatabaseKey] = ss[0]
		options[mokafka.TableKey] = ss[1]
		options[mokafka.CREATED_AT] = tasks[0].CreateAt.String()
		bufferLimit := getBufferLimit(options[mokafka.BufferLimitKey])

		c, err := NewKafkaMoConnector(logger, options, ieFactory(), bufferLimit)
		if err != nil {
			return err
		}
		if err := attachToTask(ctx, t.GetID(), c); err != nil {
			return err
		}
		// Start the connector task and hangs here.
		if err := c.Start(ctx); err != nil {
			return err
		}
		return nil
	}
}

// KafkaMoConnector is an example implementation of the Connector interface for a Kafka to MO Table connection.

type KafkaMoConnector struct {
	logger       *zap.Logger
	kafkaAdapter mokafka.KafkaAdapterInterface
	options      map[string]string
	ie           ie.InternalExecutor
	decoder      Decoder
	converter    Converter
	resumeC      chan struct{}
	cancelC      chan struct{}
	pauseC       chan struct{}
	bufferLimit  int
}

func convertToKafkaConfig(configs map[string]string) *kafka.ConfigMap {
	kafkaConfigs := &kafka.ConfigMap{}
	allowedKeys := map[string]struct{}{
		"bootstrap.servers": {},
		"security.protocol": {},
		"sasl.mechanisms":   {},
		"sasl.username":     {},
		"sasl.password":     {},
		// Add other Kafka-specific properties here...
	}

	for key, value := range configs {
		if _, ok := allowedKeys[key]; ok {
			kafkaConfigs.SetKey(key, value)
		}
	}
	groupId := configs[mokafka.TopicKey] + "-" + configs[mokafka.DatabaseKey] + "-" + configs[mokafka.TableKey] + "-" + configs[mokafka.PartitionKey] + "-" + configs[mokafka.CREATED_AT]
	kafkaConfigs.SetKey("group.id", groupId)
	return kafkaConfigs
}

func NewKafkaMoConnector(logger *zap.Logger, options map[string]string, ie ie.InternalExecutor, buffer_limit int) (*KafkaMoConnector, error) {
	// Validate options before proceeding
	kmc := &KafkaMoConnector{
		logger:      logger,
		options:     options,
		ie:          ie,
		decoder:     newJsonDecoder(),
		bufferLimit: buffer_limit,
	}
	if err := kmc.validateParams(); err != nil {
		return nil, err
	}
	kmc.converter = newSQLConverter(options[mokafka.DatabaseKey], options[mokafka.TableKey])

	// Create a Kafka consumer using the provided options
	kafkaAdapter, err := mokafka.NewKafkaAdapter(convertToKafkaConfig(options))
	if err != nil {
		return nil, err
	}

	kmc.kafkaAdapter = kafkaAdapter
	kmc.resumeC = make(chan struct{})
	kmc.cancelC = make(chan struct{})
	kmc.pauseC = make(chan struct{})
	return kmc, nil
}

func (k *KafkaMoConnector) validateParams() error {
	// 1. Check mandatory fields
	mandatoryFields := []string{
		"type", "topic", "value",
		"bootstrap.servers",
	}

	for _, field := range mandatoryFields {
		if _, exists := k.options[field]; !exists || k.options[field] == "" {
			return moerr.NewInternalError(context.Background(), "missing required params")
		}
	}

	// 2. Check for valid type
	if k.options["type"] != "kafka" {
		return moerr.NewInternalError(context.Background(), "Invalid connector type")
	}

	// 3. Check for supported value format
	if k.options["value"] != "json" {
		return moerr.NewInternalError(context.Background(), "Unsupported value format")
	}

	return nil
}

// Start begins consuming messages from Kafka and writing them to the MO Table.
func (k *KafkaMoConnector) Start(ctx context.Context) error {
	if k.kafkaAdapter == nil {
		return moerr.NewInternalError(ctx, "Kafka Adapter not initialized")
	}

	ct, err := k.kafkaAdapter.GetKafkaConsumer()

	if err != nil {
		return moerr.NewInternalError(ctx, "Kafka Adapter Consumer not initialized")
	}
	// Define the topic to consume from
	topic := k.options[mokafka.TopicKey]

	// Subscribe to the topic
	if k.options[mokafka.PartitionKey] != "" {
		partition, err := strconv.Atoi(k.options[mokafka.PartitionKey])
		if err != nil {
			return moerr.NewInternalError(ctx, "Invalid partition")
		}
		if err := ct.Assign([]kafka.TopicPartition{{Topic: &topic, Partition: int32(partition)}}); err != nil {
			return moerr.NewInternalError(ctx, "Failed to assign partition")
		}
	} else {
		if err := ct.Subscribe(topic, nil); err != nil {
			return moerr.NewInternalError(ctx, "Failed to subscribe to topic")
		}
	}
	// Continuously listen for messages
	var buffered_messages []*kafka.Message
	timeWindow := getTimeWindow(k.options[mokafka.TimeWindowKey])
	var timer *time.Timer
	timer = time.NewTimer(time.Duration(timeWindow) * time.Millisecond)
	timerRunning := false
	var mutex sync.Mutex

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-k.cancelC:
			timer.Stop()
			return ct.Close()

		case <-k.pauseC:
			select {
			case <-ctx.Done():
				return nil
			case <-k.cancelC:
				return nil
			case <-k.resumeC:
			}

		default:
			if ct.IsClosed() {
				return nil
			}
			ev := ct.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				if e.Value == nil {
					continue
				}
				mutex.Lock()
				buffered_messages = append(buffered_messages, e)

				// Start the timer if it's not already running
				if !timerRunning {
					timer.Stop()
					timer = time.AfterFunc(time.Duration(timeWindow)*time.Millisecond, func() {
						mutex.Lock()
						if len(buffered_messages) > 0 {
							k.insertRow(buffered_messages)
							buffered_messages = buffered_messages[:0]
						}
						timerRunning = false
						mutex.Unlock()
					})
					timerRunning = true
				}

				// Flush the buffer if the limit is reached
				if len(buffered_messages) >= k.bufferLimit {
					if timerRunning {
						timer.Stop()
						timerRunning = false
					}
					k.insertRow(buffered_messages)
					buffered_messages = buffered_messages[:0]
				}
				mutex.Unlock()
			case kafka.Error:
				// Handle the error accordingly.
				k.logger.Error("got error message", zap.Error(e))
			default:
				// Ignored other types of events
			}
		}
	}
}

// Resume implements the taskservice.ActiveRoutine interface.
func (k *KafkaMoConnector) Resume() error {
	k.resumeC <- struct{}{}
	return nil
}

// Pause implements the taskservice.ActiveRoutine interface.
func (k *KafkaMoConnector) Pause() error {
	k.pauseC <- struct{}{}
	return nil
}

// Cancel implements the taskservice.ActiveRoutine interface.
func (k *KafkaMoConnector) Cancel() error {
	// Cancel the connector go-routine.
	close(k.cancelC)
	return nil
}

func (k *KafkaMoConnector) Close() error {
	// Close the Kafka consumer.
	ct, err := k.kafkaAdapter.GetKafkaConsumer()
	if err != nil {
		return moerr.NewInternalError(context.Background(), "Kafka Adapter Consumer not initialized")
	}
	if err := ct.Close(); err != nil {
		return moerr.NewInternalError(context.Background(), "Error closing Kafka consumer")
	}
	return nil
}

func (k *KafkaMoConnector) insertRow(msgs []*kafka.Message) {
	opts := ie.SessionOverrideOptions{}
	ctx := context.Background()
	sql := k.options["sql"]
	dbName := k.options[mokafka.DatabaseKey]
	tableName := k.options[mokafka.TableKey]
	if sql == "" {
		return
	}
	ctx = context.WithValue(ctx, defines.SourceScanResKey{}, msgs)

	sql = fmt.Sprintf("USE %s; INSERT INTO %s.%s %s ",
		dbName, dbName, tableName, sql)
	err := k.ie.Exec(ctx, sql, opts)
	if err != nil {
		k.logger.Error("failed to insert row", zap.String("SQL", sql), zap.Error(err))
	}
}
