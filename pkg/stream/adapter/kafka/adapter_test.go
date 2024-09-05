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
	"github.com/matrixorigin/matrixone/pkg/container/vector"

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

func (m *MockKafkaAdapter) ReadMessagesFromTopic(topic string, offset int64, limit int64, configs map[string]interface{}) ([]*kafka.Message, error) {
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
	batch, err := RetrieveData(context.Background(), nil, configs, attrs, types, offset, limit, mpool.MustNewZero(), NewKafkaAdapter)
	if err != nil {
		t.Fatalf("RetrieveData failed: %s", err)
	}

	// Assertions
	assert.Equal(t, 2, batch.VectorCount(), "Expected 2 vectors in the batch")
	assert.Equal(t, batch.Vecs[0].Length(), 50, "Expected 50 row in the batch")
}
func TestPopulateBatchFromMSGWithJSON(t *testing.T) {

	// Define types and attributes as per your schema
	typs := []types.Type{
		types.New(types.T_char, 30, 0),
		types.New(types.T_int32, 32, 0),
		types.New(types.T_int16, 16, 0),
		types.New(types.T_int8, 8, 0),
		types.New(types.T_uint8, 8, 0),
		types.New(types.T_uint64, 64, 0),
		types.New(types.T_float32, 32, 0),
		types.New(types.T_datetime, 64, 0),
		types.New(types.T_json, 64, 0),
	}
	attrs := []string{
		"1", "2", "3", "4", "5", "6", "7", "8", "9",
	}
	configs := map[string]interface{}{
		"value": "json",
	}
	// Create test msgs data
	msgs := []*kafka.Message{
		{
			Value: []byte(`{
            "1": "test_name1",
            "2": 99,
            "3": 123,
            "4": 1,
            "5": 2,
            "6": 1234567890123,
            "7": 123.3,
            "8": "2021-01-01",
            "9": "{\"a\": 1}"
        }`),
		},
		{
			Value: []byte(`{
				"1": "test_name2",
				"2": 150,
				"3": 456,
				"4": 3,
				"5": 4,
				"6": 9876543210987,
				"7": 456.7,
				"8": "2000-12-04",
				"9": "{\"c1\": \"\\\"\",\"c2\":\"\\\\:\",\"c3\": \"\\\\,\",\"c4\":\"\\\\\\\"\\\\:\\\\,\",\"c5\":\"\\\\{\",\"c6\":\"\\\\}\"}"
		}
		`),
		},
		{
			Value: []byte(`{
            "1": "test_name3",
            "2": 2147483648,
            "3": 32768,
            "4": 128,
            "5": 256,
            "6": 1844674407370955161644444,
            "7": "123.33.3",
            "8": "2021-01-01T00:00:00",
            "9": "invalid_json"
        }`),
		},
	}

	expected := [][]interface{}{
		{
			"test_name1",
			int32(99),
			int16(123),
			int8(1),
			uint8(2),
			uint64(1234567890123),
			float32(123.3),
			types.Datetime(63745056000000000),
			"\x01\x01\x00\x00\x00\x1c\x00\x00\x00\x13\x00\x00\x00\x01\x00\t\x14\x00\x00\x00a\x01\x00\x00\x00\x00\x00\x00\x00",
		},
		{
			"test_name2",
			int32(150),
			int16(456),
			int8(3),
			uint8(4),
			uint64(9876543210987),
			float32(456.7),
			types.Datetime(63111484800000000),
			"\x01\x06\x00\x00\x00k\x00\x00\x00J\x00\x00\x00\x02\x00L\x00\x00\x00\x02\x00N\x00\x00\x00\x02\x00P\x00\x00\x00\x02\x00R\x00\x00\x00\x02\x00T\x00\x00\x00\x02\x00\fV\x00\x00\x00\fX\x00\x00\x00\f[\x00\x00\x00\f^\x00\x00\x00\fe\x00\x00\x00\fh\x00\x00\x00c1c2c3c4c5c6\x01\"\x02\\:\x02\\,\x06\\\"\\:\\,\x02\\{\x02\\}",
		},
		{
			"test_name3",
			nil, // int32 overflow
			nil, // int16 overflow
			nil, // int8 overflow
			nil, // uint8 overflow
			nil, // uint64 overflow
			nil, // valid float32
			nil, // invalid datetime
			nil, // invalid JSON
		},
	}

	// Call PopulateBatchFromMSG
	batch, err := PopulateBatchFromMSG(context.Background(), nil, typs, attrs, msgs, configs, mpool.MustNewZero())
	if err != nil {
		t.Fatalf("PopulateBatchFromMSG failed: %s", err)
	}

	// Assertions
	if batch == nil {
		t.Errorf("Expected non-nil batch")
	} else {

		// Check the data in the batch
		for colIdx, attr := range attrs {
			vec := batch.Vecs[colIdx]
			if vec.Length() != len(msgs) {
				t.Errorf("Expected %d rows in column '%s', got %d", len(msgs), attr, vec.Length())
			} else {
				for rowIdx := 0; rowIdx < vec.Length(); rowIdx++ {
					expectedValue := expected[rowIdx][colIdx]
					actualValue := getNonNullValue(vec, uint32(rowIdx))
					if vec.GetNulls().Contains(uint64(rowIdx)) {
						actualValue = nil
					}
					assert.Equal(t, expectedValue, actualValue, fmt.Sprintf("Mismatch in row %d, column '%s'", rowIdx, attr))
				}
			}
		}
	}

}

func getNonNullValue(col *vector.Vector, row uint32) any {

	switch col.GetType().Oid {
	case types.T_bool:
		return vector.GetFixedAtWithTypeCheck[bool](col, int(row))
	case types.T_int8:
		return vector.GetFixedAtWithTypeCheck[int8](col, int(row))
	case types.T_int16:
		return vector.GetFixedAtWithTypeCheck[int16](col, int(row))
	case types.T_int32:
		return vector.GetFixedAtWithTypeCheck[int32](col, int(row))
	case types.T_int64:
		return vector.GetFixedAtWithTypeCheck[int64](col, int(row))
	case types.T_uint8:
		return vector.GetFixedAtWithTypeCheck[uint8](col, int(row))
	case types.T_uint16:
		return vector.GetFixedAtWithTypeCheck[uint16](col, int(row))
	case types.T_uint32:
		return vector.GetFixedAtWithTypeCheck[uint32](col, int(row))
	case types.T_uint64:
		return vector.GetFixedAtWithTypeCheck[uint64](col, int(row))
	case types.T_decimal64:
		return vector.GetFixedAtWithTypeCheck[types.Decimal64](col, int(row))
	case types.T_decimal128:
		return vector.GetFixedAtWithTypeCheck[types.Decimal128](col, int(row))
	case types.T_uuid:
		return vector.GetFixedAtWithTypeCheck[types.Uuid](col, int(row))
	case types.T_float32:
		return vector.GetFixedAtWithTypeCheck[float32](col, int(row))
	case types.T_float64:
		return vector.GetFixedAtWithTypeCheck[float64](col, int(row))
	case types.T_date:
		return vector.GetFixedAtWithTypeCheck[types.Date](col, int(row))
	case types.T_time:
		return vector.GetFixedAtWithTypeCheck[types.Time](col, int(row))
	case types.T_datetime:
		return vector.GetFixedAtWithTypeCheck[types.Datetime](col, int(row))
	case types.T_timestamp:
		return vector.GetFixedAtWithTypeCheck[types.Timestamp](col, int(row))
	case types.T_enum:
		return vector.GetFixedAtWithTypeCheck[types.Enum](col, int(row))
	case types.T_TS:
		return vector.GetFixedAtWithTypeCheck[types.TS](col, int(row))
	case types.T_Rowid:
		return vector.GetFixedAtWithTypeCheck[types.Rowid](col, int(row))
	case types.T_Blockid:
		return vector.GetFixedAtWithTypeCheck[types.Blockid](col, int(row))
	case types.T_json:
		return col.UnsafeGetStringAt(int(row))
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64:
		return col.UnsafeGetStringAt(int(row))
	default:
		//return vector.ErrVecTypeNotSupport
		panic(any("No Support"))
	}
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
	batch, err := RetrieveData(context.Background(), nil, configs, attrs, types, offset, limit, mpool.MustNewZero(), NewKafkaAdapter)
	if err != nil {
		t.Fatalf("RetrieveData failed: %s", err)
	}

	// Assertions
	assert.Equal(t, 2, batch.VectorCount(), "Expected 2 vectors in the batch")
	assert.Equal(t, batch.Vecs[0].Length(), 50, "Expected 50 row in the batch")

}
