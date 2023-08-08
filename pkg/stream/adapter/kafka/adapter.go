package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type AdminClientInterface interface {
	CreateTopics(ctx context.Context, topics []kafka.TopicSpecification) ([]kafka.TopicResult, error)
}

type KafkaAdapter struct {
	Producer    *kafka.Producer
	Consumer    *kafka.Consumer
	AdminClient *kafka.AdminClient
	Brokers     []string
	ConfigMap   *kafka.ConfigMap
	Connected   bool
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

func (ka *KafkaAdapter) ReadMessages(topic string, offset int64, limit int) ([]*kafka.Message, error) {
	if ka.Consumer == nil {
		return nil, fmt.Errorf("consumer not initialized")
	}

	// Set the starting offset for the topic
	err := ka.Consumer.Assign([]kafka.TopicPartition{
		{Topic: &topic, Partition: 0, Offset: kafka.Offset(offset)},
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

func (ka *KafkaAdapter) BatchRead(topic string, startOffset int64, limit int, batchSize int) ([]*kafka.Message, error) {
	// Calculate how many goroutines to spawn based on the limit and batch size
	numGoroutines := (limit + batchSize - 1) / batchSize

	messagesCh := make(chan []*kafka.Message, numGoroutines)
	errCh := make(chan error, numGoroutines)
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(offset int64) {
			defer wg.Done()

			// Read a batch of messages
			messages, err := ka.readBatch(topic, offset, batchSize)
			if err != nil {
				errCh <- err
				return
			}
			messagesCh <- messages
		}(startOffset + int64(i*batchSize))
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

func (ka *KafkaAdapter) readBatch(topic string, offset int64, batchSize int) ([]*kafka.Message, error) {
	var messages []*kafka.Message

	ka.Consumer.Assign([]kafka.TopicPartition{
		{Topic: &topic, Partition: 0, Offset: kafka.Offset(offset)},
	})

	for i := 0; i < batchSize; i++ {
		msg, err := ka.Consumer.ReadMessage(-1) // blocking read
		if err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}

	return messages, nil
}
