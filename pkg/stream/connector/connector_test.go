// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package moconnector

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type MockSQLExecutor struct {
	execCount    int
	executedSQLs []string
	wg           *sync.WaitGroup
}

func (m *MockSQLExecutor) Exec(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
	m.execCount++
	m.executedSQLs = append(m.executedSQLs, sql)
	m.wg.Done() // Decrement the WaitGroup counter after processing a message
	return executor.Result{}, nil
}

func (m *MockSQLExecutor) ExecTxn(ctx context.Context, execFunc func(executor.TxnExecutor) error, opts executor.Options) error {
	return nil
}

func TestKafkaMoConnector(t *testing.T) {
	// Setup mock Kafka cluster
	mockCluster, err := kafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("Failed to create MockCluster: %s", err)
	}
	defer mockCluster.Close()

	broker := mockCluster.BootstrapServers()
	topic := "testTopic"

	var wg sync.WaitGroup
	mockExecutor := &MockSQLExecutor{wg: &wg}

	// Create KafkaMoConnector instance
	options := map[string]any{
		"type":              "kafka-mo",
		"topic":             topic,
		"database":          "testDB",
		"table":             "testTable",
		"value":             "json",
		"bootstrap.servers": broker,
	}
	connector, err := NewKafkaMoConnector(options, mockExecutor)
	if err != nil {
		t.Fatalf("Failed to create KafkaMoConnector: %s", err)
	}

	// Produce mock data
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		t.Fatalf("Failed to create producer: %s", err)
	}
	type MessagePayload struct {
		Name string `json:"name"`
		Age  int32  `json:"age"`
	}
	payload := MessagePayload{
		Name: "test_name",
		Age:  100,
	}
	value, _ := json.Marshal(payload)

	// produce 10 messages
	for i := 0; i < 10; i++ {
		wg.Add(1) // Increment the WaitGroup counter for each message
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          value,
		}, nil)
	}
	// Start the connector in a goroutine
	go func() {
		connector.Start(context.Background())
	}()

	// Create a channel to signal when all messages are processed
	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	// Wait for all messages to be processed or timeout
	select {
	case <-done:
		// All messages processed
	case <-time.After(10 * time.Second): // Adjust the timeout duration as needed
		t.Fatal("Timeout waiting for messages to be processed")
	}

	// Stop the connector
	if err := connector.Close(); err != nil {
		t.Errorf("Error in Close: %s", err)
	}

	// Verify that the MockSQLExecutor has executed the correct SQL for 10 times
	if mockExecutor.execCount != 10 {
		t.Errorf("Expected SQL to be executed 10 times, but got %d", mockExecutor.execCount)
	}

	// Additional verification for executed SQLs can be added here, if needed.
	//for _, sql := range mockExecutor.executedSQLs {
	//	// Example: t.Logf("Executed SQL: %s", sql)
	//}
}
