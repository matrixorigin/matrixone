package main

import (
	"context"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/protobuf"
	"github.com/google/uuid"
	mokafka "github.com/matrixorigin/matrixone/pkg/stream/adapter/kafka"
	"github.com/matrixorigin/matrixone/pkg/stream/adapter/kafka/example/proto/test_v1"
)

const (
	topicName         = "test_topic17"
	partitions        = 1
	replicationFactor = 1
)

func main() {
	// Define your Kafka configuratio
	config := &kafka.ConfigMap{
		"bootstrap.servers":     "127.0.0.1:62610",
		"group.id":              "myGroup",
		"auto.offset.reset":     "earliest", // Read from the beginning
		"enable.auto.commit":    false,
		"session.timeout.ms":    6000,
		"broker.address.family": "v4",
	}

	// Initialize KafkaAdapter
	ka, err := mokafka.NewKafkaAdapter(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize KafkaAdapter: %s\n", err)
		os.Exit(1)
	}

	// Initialize Schema Registry
	err = ka.InitSchemaRegistry("http://localhost:8081")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize schema registry: %s\n", err)
		os.Exit(1)
	}

	// Create the topic using KafkaAdapter
	err = ka.CreateTopic(context.Background(), topicName, partitions, replicationFactor)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create topic: %s\n", err)
	}

	// Serialize the message
	s, err := protobuf.NewSerializer(ka.SchemaRegistry, serde.ValueSerde, protobuf.NewSerializerConfig())

	testMSG := test_v1.MOTestMessage{Value: 42}
	testKey := uuid.New()
	serialized, err := s.Serialize(topicName, &testMSG)
	if err != nil {
		return
	}

	// send msg with schema test.v1.proto
	offset, err := ka.ProduceMessage(topicName, testKey[:], serialized)

	// Fetch schema for a topic
	schema, err := ka.GetSchemaForTopic(topicName, false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to fetch schema: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Schema for topic %s: %s\n", topicName, schema)

	// Read message from topic
	msgs, err := ka.ReadMessagesFromTopic(topicName, 0, 1)
	// Deserialize the message

	d, err := protobuf.NewDeserializer(ka.SchemaRegistry, serde.ValueSerde, protobuf.NewDeserializerConfig())
	// Register the message type
	err = d.ProtoRegistry.RegisterMessage((&test_v1.MOTestMessage{}).ProtoReflect().Type())
	if err != nil {
		return
	}

	for _, msg := range msgs {
		m, err := d.Deserialize(topicName, msg.Value)
		if err != nil {
			return
		}
		fmt.Printf("message %v with offset %d\n", m, offset)
	}

	// dynamic parse the proto message to json  without generated protobuf code
	res, err := mokafka.DeserializeProtobuf(schema, msgs[0].Value)
	fmt.Printf("message %v with offset %d\n", res, offset)
}
