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

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	mokafka "github.com/matrixorigin/matrixone/pkg/stream/adapter/kafka"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type ConnectorStatus int

const (
	CREATED ConnectorStatus = iota
	RUNNING
	STOPPED
	ERROR
)

func (s ConnectorStatus) String() string {
	return [...]string{"CREATED", "RUNNING", "STOPPED", "ERROR"}[s]
}

// ConnectorManagerInterface defines the operations for managing connectors.
type ConnectorManagerInterface interface {
	// CreateConnector creates a new connector based on the provided name and options.
	CreateConnector(ctx context.Context, name string, options map[string]interface{}) error

	// StopConnector stops the connector with the given name.
	StopConnector(ctx context.Context, name string) error

	// GetConnector retrieves the connector with the given name.
	GetConnector(name string) (Connector, error)

	ListConnectors() []Connector
}

var _ ConnectorManagerInterface = (*ConnectorManager)(nil)

type ConnectorManager struct {
	ctx        context.Context
	connectors map[string]Connector
}

func NewConnectorManager(ctx context.Context) *ConnectorManager {
	return &ConnectorManager{
		connectors: make(map[string]Connector),
		ctx:        ctx,
	}
}

func (cm *ConnectorManager) CreateConnector(ctx context.Context, name string, options map[string]any) error {
	if _, exists := cm.connectors[name]; exists {
		return moerr.NewInternalError(ctx, "Connector already exists")
	}

	switch options["type"] {
	case "kafka-mo":
		v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.InternalSQLExecutor)
		if !ok {
			return moerr.NewInternalError(ctx, "Internal SQL Executor not found")
		}
		ie := v.(executor.SQLExecutor)

		connector, err := NewKafkaMoConnector(options, ie)
		if err != nil {
			return err
		}
		cm.connectors[name] = connector
	default:
		return moerr.NewInternalError(ctx, "Invalid connector type")
	}
	return nil
}

func (cm *ConnectorManager) StopConnector(ctx context.Context, name string) error {
	//todo: implement stop connecot
	return nil
}

func (cm *ConnectorManager) GetConnector(name string) (Connector, error) {
	if connector, exists := cm.connectors[name]; exists {
		return connector, nil
	}
	return nil, moerr.NewInternalError(context.Background(), "Connector not found")
}

func (cm *ConnectorManager) ListConnectors() []Connector {
	connectors := make([]Connector, 0)
	for _, connector := range cm.connectors {
		connectors = append(connectors, connector)
	}
	return connectors
}

// Connector is an interface for various types of connectors.
type Connector interface {
	Prepare() error
	Start(ctx context.Context) error
	Close() error
	Status() ConnectorStatus
}

// KafkaMoConnector is an example implementation of the Connector interface for a Kafka to MO Table connection.

type KafkaMoConnector struct {
	kafkaAdapter mokafka.KafkaAdapterInterface
	options      map[string]any
	ie           executor.SQLExecutor
	stopChan     chan struct{}
	status       ConnectorStatus
}

func convertToKafkaConfig(configs map[string]interface{}) *kafka.ConfigMap {
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
	groupId := configs["topic"].(string) + "-" + configs["database"].(string) + "-" + configs["table"].(string)
	kafkaConfigs.SetKey("group.id", groupId)
	return kafkaConfigs
}

func NewKafkaMoConnector(options map[string]any, ie executor.SQLExecutor) (*KafkaMoConnector, error) {
	// Validate options before proceeding
	kmc := &KafkaMoConnector{
		options: options,
		ie:      ie,
	}
	if err := kmc.validateParams(); err != nil {
		return nil, err
	}

	// Create a Kafka consumer using the provided options
	kafkaAdapter, err := mokafka.NewKafkaAdapter(convertToKafkaConfig(options))
	if err != nil {
		return nil, err
	}

	kmc.kafkaAdapter = kafkaAdapter
	kmc.stopChan = make(chan struct{})
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
	if k.options["type"] != "kafka-mo" {
		return moerr.NewInternalError(context.Background(), "Invalid connector type")
	}

	// 3. Check for supported value format
	if k.options["value"] != "json" {
		return moerr.NewInternalError(context.Background(), "Unsupported value format")
	}

	return nil
}
func (k *KafkaMoConnector) Status() ConnectorStatus {
	return k.status
}

// Prepare initializes resources, validates configurations, and prepares the connector for starting.
func (k *KafkaMoConnector) Prepare() error {
	// 1. Validate input params (assuming a separate function for this)
	if err := k.validateParams(); err != nil {
		return err
	}
	// 2. Create or find table in MO
	return k.createOrFindTable(k.options)
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
	topic := k.options["topic"].(string)

	// Subscribe to the topic
	if err := ct.Subscribe(topic, nil); err != nil {
		return moerr.NewInternalError(ctx, "Failed to subscribe to topic")
	}
	// Continuously listen for messages
	for {
		select {
		case <-k.stopChan:
			return nil
		default:
			ev := ct.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				var insertSQL string
				var err error

				switch k.options["value"].(string) {
				case "json":
					// Convert the JSON message into an SQL INSERT statement
					insertSQL, err = convertJSONToInsertSQL(string(e.Value), k.options["database"].(string), k.options["table"].(string))
				case "avro":
					// Handle Avro decoding and conversion to SQL here
					// For now, we'll skip it since you mentioned not to use SchemaRegistry
				case "protobuf":
					// Handle Protobuf decoding and conversion to SQL here
					// For now, we'll skip it since you mentioned not to use SchemaRegistry
				default:
					return moerr.NewInternalError(ctx, "Unsupported value format")
				}

				if err != nil {
					return moerr.NewInternalError(ctx, "Error converting message to SQL")
				}

				// Execute the INSERT statement
				opts := executor.Options{}
				_, err = k.ie.Exec(ctx, insertSQL, opts)
				if err != nil {
					return moerr.NewInternalError(ctx, "Error executing SQL")
				}
			case kafka.Error:
				// Handle the error accordingly.
				return e
			default:
				// Ignored other types of events
			}
		}
	}
}

// Assuming a simple function to convert JSON to SQL INSERT statement
func convertJSONToInsertSQL(jsonMessage string, database string, table string) (string, error) {
	// This is a placeholder. Actual conversion logic will depend on the structure of the JSON and the table schema.
	return fmt.Sprintf("INSERT INTO %s.%s VALUES (...);", database, table), nil
}

func (k *KafkaMoConnector) Close() error {
	close(k.stopChan)
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

func (k *KafkaMoConnector) createOrFindTable(options map[string]interface{}) error {
	database := options["database"].(string)
	tableName := options["table"].(string)

	// Check if the table exists
	if !k.doesTableExist(context.Background(), database, tableName) {
		// Todo: enable create table
		k.createTable(context.Background(), database, tableName)
		return moerr.NewInternalError(context.Background(), "Table does not exist")
	}

	return nil
}

func (k *KafkaMoConnector) doesTableExist(ctx context.Context, database, tableName string) bool {
	query := fmt.Sprintf("SHOW TABLES IN %s LIKE '%s';", database, tableName)
	opts := executor.Options{}
	result, err := k.ie.Exec(ctx, query, opts)
	if err != nil || len(result.Batches) == 0 {
		return false
	}
	// Further validation can be added based on the 'result' structure
	return true
}

func (k *KafkaMoConnector) createTable(ctx context.Context, database, tableName string) error {
	// todo: define the schema for the table
	query := fmt.Sprintf("CREATE TABLE %s.%s (id INT, data VARCHAR(255));", database, tableName)
	opts := executor.Options{}
	_, err := k.ie.Exec(ctx, query, opts)
	return err
}
