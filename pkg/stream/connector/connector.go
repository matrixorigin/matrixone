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
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	mokafka "github.com/matrixorigin/matrixone/pkg/stream/adapter/kafka"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"go.uber.org/zap"
)

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
			return moerr.NewInternalError(ctx, "invalid tasks count %d", len(tasks))
		}
		details, ok := tasks[0].Details.Details.(*task.Details_Connector)
		if !ok {
			return moerr.NewInternalError(ctx, "invalid details type")
		}
		options := details.Connector.Options
		fullTableName := details.Connector.TableName
		// Set database and table name for options.
		ss := strings.Split(fullTableName, ".")
		options["database"] = ss[0]
		options["table"] = ss[1]
		c, err := NewKafkaMoConnector(logger, options, ieFactory())
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
	groupId := configs["topic"] + "-" + configs["database"] + "-" + configs["table"]
	kafkaConfigs.SetKey("group.id", groupId)
	return kafkaConfigs
}

func NewKafkaMoConnector(logger *zap.Logger, options map[string]string, ie ie.InternalExecutor) (*KafkaMoConnector, error) {
	// Validate options before proceeding
	kmc := &KafkaMoConnector{
		logger:  logger,
		options: options,
		ie:      ie,
		decoder: newJsonDecoder(),
	}
	if err := kmc.validateParams(); err != nil {
		return nil, err
	}
	kmc.converter = newSQLConverter(options["database"], options["table"])

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
		"type", "topic", "database", "table", "value",
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
	topic := k.options["topic"]

	// Subscribe to the topic
	if err := ct.Subscribe(topic, nil); err != nil {
		return moerr.NewInternalError(ctx, "Failed to subscribe to topic")
	}
	// Continuously listen for messages
	for {
		select {
		case <-ctx.Done():
			return nil

		case <-k.cancelC:
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
				var insertSQL string
				switch k.options["value"] {
				case "json":
					obj, err := k.decoder.Decode(e.Value)
					if err != nil {
						k.logger.Error("failed to decode from json data", zap.Error(err))
						continue
					}
					insertSQL, err = k.converter.Convert(ctx, obj)
					if err != nil {
						k.logger.Error("failed to convert to insert SQL", zap.Error(err))
						continue
					}
				case "avro":
					// Handle Avro decoding and conversion to SQL here
					// For now, we'll skip it since you mentioned not to use SchemaRegistry
				case "protobuf":
					// Handle Protobuf decoding and conversion to SQL here
					// For now, we'll skip it since you mentioned not to use SchemaRegistry
				default:
					return moerr.NewInternalError(ctx, "Unsupported value format")
				}

				// Insert the row data to table.
				k.insertRow(insertSQL)

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

func (k *KafkaMoConnector) insertRow(sql string) {
	opts := ie.SessionOverrideOptions{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	err := k.ie.Exec(ctx, sql, opts)
	if err != nil {
		k.logger.Error("failed to insert row", zap.String("SQL", sql), zap.Error(err))
	}
}
