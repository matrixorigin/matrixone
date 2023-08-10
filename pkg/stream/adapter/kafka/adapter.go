package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type AdminClientInterface interface {
	CreateTopics(ctx context.Context, topics []kafka.TopicSpecification) ([]kafka.TopicResult, error)
}

type KafkaAdapter struct {
	Producer       *kafka.Producer
	Consumer       *kafka.Consumer
	AdminClient    *kafka.AdminClient
	SchemaRegistry schemaregistry.Client
	Brokers        []string
	ConfigMap      *kafka.ConfigMap
	Connected      bool
}

func (ka *KafkaAdapter) InitSchemaRegistry(url string) error {
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(url))
	if err != nil {
		return fmt.Errorf("failed to create schema registry client: %w", err)
	}
	ka.SchemaRegistry = client
	return nil
}

func NewKafkaAdapter(brokers []string, configMap *kafka.ConfigMap) (*KafkaAdapter, error) {
	// Ensure provided brokers are not empty
	if len(brokers) == 0 {
		return nil, fmt.Errorf("brokers list is empty")
	}

	// Create a new admin client instance
	adminClient, err := kafka.NewAdminClient(configMap)
	if err != nil {
		return nil, fmt.Errorf("unable to create confluent admin client: %w", err)
	}

	// Create a new consumer client instance
	consumer, err := kafka.NewConsumer(configMap)
	if err != nil {
		return nil, fmt.Errorf("unable to create confluent consumer client: %w", err)
	}

	// Create a new producer client instance
	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		return nil, fmt.Errorf("unable to create confluent producer client: %w", err)
	}

	// Return a new KafkaAdapter instance
	return &KafkaAdapter{
		Producer:    producer,
		AdminClient: adminClient,
		Consumer:    consumer,
		Brokers:     brokers,
		ConfigMap:   configMap,
		Connected:   true,
	}, nil
}

func (ka *KafkaAdapter) CreateTopic(ctx context.Context, topicName string, partitions int, replicationFactor int) error {
	topicSpecification := kafka.TopicSpecification{
		Topic:             topicName,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
		// can add more configs here
	}

	results, err := ka.AdminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpecification})
	if err != nil {
		return err
	}

	// Check results for errors
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			return result.Error
		}
	}
	return nil
}

func (ka *KafkaAdapter) DescribeTopicDetails(ctx context.Context, topicName string) (*kafka.TopicMetadata, error) {

	// Fetch metadata
	meta, err := ka.AdminClient.GetMetadata(&topicName, false, int(10*time.Second.Milliseconds()))
	if err != nil {
		return nil, err
	}

	// Find and return the topic's metadata
	for _, topic := range meta.Topics {
		if topic.Topic == topicName {
			return &topic, nil
		}
	}

	return nil, moerr.NewInternalError(ctx, "topic not found")
}

func (ka *KafkaAdapter) ReadMessagesFromPartition(topic string, partition int32, offset int64, limit int) ([]*kafka.Message, error) {
	if ka.Consumer == nil {
		return nil, fmt.Errorf("consumer not initialized")
	}

	// Assign the specific partition with the desired offset
	err := ka.Consumer.Assign([]kafka.TopicPartition{
		{Topic: &topic, Partition: partition, Offset: kafka.Offset(offset)},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to assign partition: %w", err)
	}

	var messages []*kafka.Message
	for i := 0; i < limit; i++ {
		msg, err := ka.Consumer.ReadMessage(-1) // Wait indefinitely until a message is available
		if err != nil {
			// Check for timeout
			if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
				break // Exit the loop if a timeout occurs
			} else {
				return nil, fmt.Errorf("failed to read message: %w", err)
			}
		}
		messages = append(messages, msg)
	}

	return messages, nil
}

func (ka *KafkaAdapter) ReadMessagesFromTopic(topic string, offset int64, limit int) ([]*kafka.Message, error) {
	if ka.Consumer == nil {
		return nil, fmt.Errorf("consumer not initialized")
	}

	// Fetch metadata to get all partitions
	meta, err := ka.Consumer.GetMetadata(&topic, false, 5000) // timeout in ms
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata: %w", err)
	}

	var partitions []kafka.TopicPartition
	topicMetadata, ok := meta.Topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic not found in metadata")
	}

	for _, p := range topicMetadata.Partitions {
		partitions = append(partitions, kafka.TopicPartition{Topic: &topic, Partition: p.ID, Offset: kafka.Offset(offset)})
	}

	err = ka.Consumer.Assign(partitions)
	if err != nil {
		return nil, fmt.Errorf("failed to assign partitions: %w", err)
	}

	var messages []*kafka.Message
	for i := 0; i < limit; i++ {
		msg, err := ka.Consumer.ReadMessage(-1) // Wait indefinitely until a message is available
		if err != nil {
			// Check for timeout
			if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
				break // Exit the loop if a timeout occurs
			} else {
				return nil, fmt.Errorf("failed to read message: %w", err)
			}
		}
		messages = append(messages, msg)
	}

	return messages, nil
}

func (ka *KafkaAdapter) BatchRead(topic string, startOffset int64, limit int, batchSize int) ([]*kafka.Message, error) {
	// Fetch metadata to get all partitions
	meta, err := ka.Consumer.GetMetadata(&topic, false, 5000) // timeout in ms
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata: %w", err)
	}

	topicMetadata, ok := meta.Topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic not found in metadata")
	}

	numGoroutines := (limit + batchSize - 1) / batchSize

	messagesCh := make(chan []*kafka.Message, numGoroutines)
	errCh := make(chan error, numGoroutines)
	var wg sync.WaitGroup

	// Loop over each partition and start goroutines for reading
	for _, p := range topicMetadata.Partitions {
		wg.Add(1)
		go func(partition int32) {
			defer wg.Done()

			// Read a batch of messages
			messages, err := ka.ReadMessagesFromPartition(topic, partition, startOffset, batchSize)
			if err != nil {
				errCh <- err
				return
			}
			messagesCh <- messages
		}(p.ID)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	close(messagesCh)
	close(errCh)

	// Collect all messages
	var allMessages []*kafka.Message
	for batch := range messagesCh {
		allMessages = append(allMessages, batch...)
	}

	// Return the first error encountered, if any
	for err := range errCh {
		return nil, err
	}

	return allMessages, nil
}

func (ka *KafkaAdapter) GetSchemaForTopic(topic string, isKey bool) (string, error) {
	if ka.SchemaRegistry == nil {
		return "", fmt.Errorf("schema registry not initialized")
	}

	subjectSuffix := "value"
	if isKey {
		subjectSuffix = "key"
	}
	subject := fmt.Sprintf("%s-%s", topic, subjectSuffix)

	// Fetch the schema for the subject
	schema, err := ka.SchemaRegistry.GetLatestSchemaMetadata(subject)
	if err != nil {
		return "", fmt.Errorf("failed to fetch schema for topic %s: %w", topic, err)
	}
	return schema.Schema, nil
}
