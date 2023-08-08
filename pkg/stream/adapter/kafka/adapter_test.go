package kafka

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestKafkaAdapter_CreateTopic(t *testing.T) {

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:51058"})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	a, err := kafka.NewAdminClientFromProducer(p)
	// create topic
	topic := "myTopic"
	numPartitions := 1
	replicationFactor := 1

	results, err := a.CreateTopics(
		context.Background(),
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
		}},
	)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			t.Errorf("Failed to create topic %s: %s", result.Topic, result.Error.String())
		}
	}
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
	err = p.Produce(&kafka.Message{
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

func main() {
	// Define your Kafka configuration
	config := &kafka.ConfigMap{
		"bootstrap.servers":     "127.0.0.1:51266",
		"group.id":              "myGroup",
		"auto.offset.reset":     "earliest", // Read from the beginning
		"enable.auto.commit":    false,
		"session.timeout.ms":    6000,
		"broker.address.family": "v4",
	}

	// Create a new consumer
	c, err := kafka.NewConsumer(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}
	defer c.Close()

	// Define your topic
	topic := "your-topic-name"

	// Subscribe to the topic
	err = c.Subscribe(topic, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to subscribe to topic: %s\n", err)
		os.Exit(1)
	}

	// Read messages from the topic
	for {
		msg, err := c.ReadMessage(1 * time.Second)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read message: %s\n", err)
			break
		}
		fmt.Printf("Received message on %s: %s\n", msg.TopicPartition, string(msg.Value))
	}
}
