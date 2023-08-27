// Copyright 2021 Matrix Origin
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

package mokafka

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/stream/adapter/kafka/test/proto/test_v1"
	"github.com/stretchr/testify/assert"
)

type MockKafkaAdapter struct{}

func (m *MockKafkaAdapter) InitSchemaRegistry(url string) error {
	return nil // Mocked response
}

func (m *MockKafkaAdapter) Close() {
	// Mocked response
}

func (m *MockKafkaAdapter) GetKafkaConsumer() (*kafka.Consumer, error) {
	return nil, nil // Mocked response
}
func (m *MockKafkaAdapter) CreateTopic(ctx context.Context, topicName string, partitions int, replicationFactor int) error {
	return nil // Mocked response
}

func (m *MockKafkaAdapter) DescribeTopicDetails(ctx context.Context, topicName string) (*kafka.TopicMetadata, error) {
	return nil, nil // Mocked response
}

func (m *MockKafkaAdapter) ReadMessagesFromPartition(topic string, partition int32, offset int64, limit int) ([]*kafka.Message, error) {
	return nil, nil // Mocked response
}

func (m *MockKafkaAdapter) ReadMessagesFromTopic(topic string, offset int64, limit int64) ([]*kafka.Message, error) {
	return nil, nil // Mocked response
}

func (m *MockKafkaAdapter) GetSchemaForTopic(topic string, isKey bool) (schemaregistry.SchemaMetadata, error) {
	return schemaregistry.SchemaMetadata{}, nil // Mocked response
}

func (m *MockKafkaAdapter) ProduceMessage(topic string, key, value []byte) (int64, error) {
	return 0, nil // Mocked response
}

func TestNewKafkaAdapter(t *testing.T) {

	mockCluster, err := kafka.NewMockCluster(1)
	if err != nil {
		fmt.Printf("Failed to create MockCluster: %s\n", err)
		os.Exit(1)
	}
	defer mockCluster.Close()

	broker := mockCluster.BootstrapServers()

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v\n", p)
	deliveryChan := make(chan kafka.Event)

	topic := "Test"
	value := "Hello Go!"
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(value),
		Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
	}, deliveryChan)

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	close(deliveryChan)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     broker,
		"broker.address.family": "v4",
		"group.id":              "group",
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest"})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}
	defer c.Close()

	fmt.Printf("Created Consumer %v\n", c)

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to subscribe to consumer: %s\n", err)
		os.Exit(1)
	}

	msg, err := c.ReadMessage(-1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read message: %s\n", err)
		os.Exit(1)
	}

	fmt.Println("received message: ", string(msg.Value))

}

func TestValidateConfig_MissingRequiredKeys(t *testing.T) {
	ctx := context.Background()
	configs := map[string]interface{}{
		// Intentionally leaving out some keys to simulate missing keys
		TypeKey:             "someType",
		BootstrapServersKey: "localhost:9092",
	}

	err := ValidateConfig(ctx, configs, NewKafkaAdapter)
	if err == nil || !strings.Contains(err.Error(), "missing required key") {
		t.Errorf("Expected an error about missing keys, got: %v", err)
	}
}

func TestValidateConfig_UnsupportedValueKey(t *testing.T) {
	ctx := context.Background()
	configs := map[string]interface{}{
		TypeKey:             "someType",
		TopicKey:            "someTopic",
		ValueKey:            "UNSUPPORTED_VALUE",
		BootstrapServersKey: "localhost:9092",
	}

	err := ValidateConfig(ctx, configs, NewKafkaAdapter)
	if err == nil || !strings.Contains(err.Error(), "Unsupported value for key") {
		t.Errorf("Expected an error about unsupported value, got: %v", err)
	}
}

func TestValidateConfig_ValidConfigurations(t *testing.T) {
	ctx := context.Background()
	configs := map[string]interface{}{
		TypeKey:             "someType",
		TopicKey:            "someTopic",
		ValueKey:            "json", // or PROTOBUF
		BootstrapServersKey: "localhost:9092",
	}

	// Assuming you have a way to stub out or mock NewKafkaAdapter and DescribeTopicDetails here
	mockFactory := func(configMap *kafka.ConfigMap) (KafkaAdapterInterface, error) {
		return &MockKafkaAdapter{}, nil
	}
	err := ValidateConfig(ctx, configs, mockFactory)
	if err != nil {
		t.Errorf("Did not expect an error, got: %v", err)
	}
}

func TestRetrieveDataWIthJson(t *testing.T) {
	// Setup
	mockCluster, err := kafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("Failed to create MockCluster: %s", err)
	}
	defer mockCluster.Close()

	broker := mockCluster.BootstrapServers()
	topic := "TestTopic"

	// Produce mock data
	// (You can add more messages or customize this part as necessary)
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

	// produce 100 messages
	for i := 0; i < 100; i++ {
		err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          value,
		}, nil)
		if err != nil {
			return
		}
	}

	// Setup configs for RetrieveData
	configs := map[string]interface{}{
		"type":              "kafka",
		"bootstrap.servers": broker,
		"topic":             topic,
		"value":             "json",
	}
	attrs := []string{
		"name", "age",
	}
	types := []types.Type{
		types.New(types.T_char, 30, 0),
		types.New(types.T_int32, 10, 0),
	}
	offset := int64(0)
	limit := int64(50)

	// Call RetrieveData
	batch, err := RetrieveData(context.Background(), configs, attrs, types, offset, limit, mpool.MustNewZero(), NewKafkaAdapter)
	if err != nil {
		t.Fatalf("RetrieveData failed: %s", err)
	}

	// Assertions
	assert.Equal(t, 2, batch.VectorCount(), "Expected 2 vectors in the batch")
	assert.Equal(t, batch.Vecs[0].Length(), 50, "Expected 50 row in the batch")
}

func TestRetrieveDataWIthProtobuf(t *testing.T) {
	// Setup
	mockCluster, err := kafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("Failed to create MockCluster: %s", err)
	}
	defer mockCluster.Close()

	broker := mockCluster.BootstrapServers()
	topic := "TestTopic"

	// Produce mock data
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		t.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()

	user := test_v1.UserMessage{
		Name:  "dummy_name",
		Age:   10,
		Email: "dummy@test.com",
	}
	payload, _ := proto.Marshal(&user)

	// produce 100 messages
	for i := 0; i < 100; i++ {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          payload,
		}, nil)
	}

	// Setup configs for RetrieveData
	configs := map[string]interface{}{
		"type":              "kafka",
		"bootstrap.servers": broker,
		"topic":             topic,
		"value":             "protobuf",
		"protobuf.message":  "test_v1.UserMessage",
		"protobuf.schema":   "syntax = \"proto3\";\noption go_package = \"./proto/test_v1\";\npackage test_v1;\n\nmessage UserMessage {\n  string Name = 1;\n  string Email = 2;\n  int32 Age = 4;\n}",
	}
	attrs := []string{
		"Name", "Age",
	}
	types := []types.Type{
		types.New(types.T_char, 30, 0),
		types.New(types.T_int32, 10, 0),
	}
	offset := int64(0)
	limit := int64(50)

	// Call RetrieveData
	batch, err := RetrieveData(context.Background(), configs, attrs, types, offset, limit, mpool.MustNewZero(), NewKafkaAdapter)
	if err != nil {
		t.Fatalf("RetrieveData failed: %s", err)
	}

	// Assertions
	assert.Equal(t, 2, batch.VectorCount(), "Expected 2 vectors in the batch")
	assert.Equal(t, batch.Vecs[0].Length(), 50, "Expected 50 row in the batch")

}
