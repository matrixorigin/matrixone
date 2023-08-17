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

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type ConnectorManager struct {
	connectors map[string]Connector
}

func (cm *ConnectorManager) CreateConnector(ctx context.Context, name string, options map[string]string) error {
	if _, exists := cm.connectors[name]; exists {
		return moerr.NewInternalError(ctx, "Connector already exists")
	}

	switch options["type"] {
	case "kafka-mo":
		connector, err := NewKafkaMoConnector(options)
		if err != nil {
			return err
		}
		cm.connectors[name] = connector
	default:
		return moerr.NewInternalError(ctx, "Invalid connector type")
	}
	return nil
}

// Connector is an interface for various types of connectors.
type Connector interface {
	Prepare() error
	Start() error
	Close() error
}

// KafkaMoConnector is an example implementation of the Connector interface for a Kafka to MO Table connection.
type KafkaMoConnector struct {
	consumer *kafka.Consumer
	// ... other necessary fields ...
}

func NewKafkaMoConnector(options map[string]string) (*KafkaMoConnector, error) {
	// Create a Kafka consumer using the provided options
	return nil, nil
}

// Prepare initializes resources, validates configurations, and prepares the connector for starting.
func (k *KafkaMoConnector) Prepare() error {
	// Initialize the Kafka consumer using provided options.
	return nil
}

// Start begins consuming messages from Kafka and writing them to the MO Table.
func (k *KafkaMoConnector) Start() error {
	// Start a loop or goroutine to continuously consume messages from Kafka.
	return nil
}

// Close gracefully shuts down the connector.
func (k *KafkaMoConnector) Close() error {
	// Ensure any remaining data is processed.
	// Close the Kafka consumer.
	// Release any other resources.
	return nil
}
