package mokafka

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/gogo/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type AdminClientInterface interface {
	CreateTopics(ctx context.Context, topics []kafka.TopicSpecification) ([]kafka.TopicResult, error)
}

type KafkaAdapter struct {
	Producer       *kafka.Producer
	Consumer       *kafka.Consumer
	AdminClient    *kafka.AdminClient
	SchemaRegistry schemaregistry.Client
	ConfigMap      *kafka.ConfigMap
	Connected      bool
}

func (ka *KafkaAdapter) InitSchemaRegistry(url string) error {
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(url))
	if err != nil {
		return err
	}
	ka.SchemaRegistry = client
	return nil
}

func NewKafkaAdapter(configMap *kafka.ConfigMap) (*KafkaAdapter, error) {
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
		return nil, moerr.NewInternalError(context.Background(), "consumer not initialized")
	}

	// Fetch metadata to get all partitions
	meta, err := ka.Consumer.GetMetadata(&topic, false, -1) // timeout in ms
	if err != nil {
		return nil, err
	}

	topicMetadata, ok := meta.Topics[topic]
	if !ok {
		return nil, moerr.NewInternalError(context.Background(), "topic not found in metadata")
	}

	var messages []*kafka.Message
	for _, p := range topicMetadata.Partitions {
		// Fetch the high watermark for the partition
		_, highwatermarkHigh, err := ka.Consumer.QueryWatermarkOffsets(topic, p.ID, -1)
		if err != nil {
			return nil, fmt.Errorf("failed to query watermark offsets: %w", err)
		}

		// Calculate the number of messages available to consume
		availableMessages := int(highwatermarkHigh - offset)
		if availableMessages <= 0 {
			continue
		}

		// Determine the number of messages to consume from this partition
		partitionLimit := limit - len(messages)
		if partitionLimit > availableMessages {
			partitionLimit = availableMessages
		}

		// Assign the specific partition with the desired offset
		err = ka.Consumer.Assign([]kafka.TopicPartition{
			{Topic: &topic, Partition: p.ID, Offset: kafka.Offset(offset)},
		})
		if err != nil {
			return nil, err
		}

		for i := 0; i < partitionLimit; i++ {
			msg, err := ka.Consumer.ReadMessage(-1) // Wait indefinitely until a message is available
			if err != nil {
				// Check for timeout
				var kafkaErr kafka.Error
				if errors.As(err, &kafkaErr) && kafkaErr.Code() == kafka.ErrTimedOut {
					break // Exit the loop if a timeout occurs
				}
			}
			messages = append(messages, msg)
		}
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

func (ka *KafkaAdapter) ProduceMessage(topic string, key, value []byte) (int64, error) {

	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Key:            key,
		Value:          value,
	}

	err := ka.Producer.Produce(message, deliveryChan)
	if err != nil {
		return -1, moerr.NewInternalError(context.Background(), fmt.Sprintf("failed to produce message: %s", err))
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		return -1, m.TopicPartition.Error
	}

	return int64(m.TopicPartition.Offset), nil
}

func newBatch(batchSize int, typs []types.Type, pool *mpool.MPool) *batch.Batch {
	batch := batch.NewWithSize(len(typs))
	for i, typ := range typs {
		switch typ.Oid {
		case types.T_datetime:
			typ.Scale = 6
		}
		vec := vector.NewVec(typ)
		vec.PreExtend(batchSize, pool)
		vec.SetLength(batchSize)
		batch.Vecs[i] = vec
	}
	return batch
}

func PopulateBatchFromMSG(typs []types.Type, attrKeys []string, pool *mpool.MPool, msgs []*kafka.Message, valueSchemaMeta *schemaregistry.SchemaMetadata) (*batch.Batch, error) {
	b := newBatch(len(msgs), typs, pool)
	// parse the msg and populate the batch
	for i, msg := range msgs {
		switch valueSchemaMeta.SchemaType {
		case "JSON":
		case "AVRO":
		case "PROTOBUF":
			md, _ := ConvertProtobufSchemaToMD(valueSchemaMeta.Schema, valueSchemaMeta.SchemaInfo.Schema)
			msgValue, _ := DeserializeProtobuf(md, msg.Value)
			getOneRowProtoData(context.Background(), b, attrKeys, msgValue, i, typs, pool)
		default:
			return nil, moerr.NewInternalError(context.Background(), fmt.Sprintf("unsupported schema type: %s", valueSchemaMeta.SchemaType))
		}
	}

	return b, nil
}
func getOneRowProtoData(ctx context.Context, bat *batch.Batch, attrKeys []string, msg *dynamic.Message, rowIdx int, typs []types.Type, mp *mpool.MPool) error {

	for colIdx, typ := range typs {
		fieldValue := msg.GetFieldByName(attrKeys[colIdx])
		if fieldValue == nil {
			return moerr.NewInternalError(ctx, "field not found: %s", attrKeys[colIdx])
		}

		id := typ.Oid
		vec := bat.Vecs[colIdx]
		switch id {
		case types.T_int64:
			val, ok := fieldValue.(int64)
			if !ok {
				return moerr.NewInternalError(ctx, "expected int64 type for column %d but got %T", colIdx, fieldValue)
			}
			cols := vector.MustFixedCol[int64](vec)
			cols[rowIdx] = val

		case types.T_uint64:
			val, ok := fieldValue.(uint64)
			if !ok {
				return moerr.NewInternalError(ctx, "expected uint64 type for column %d but got %T", colIdx, fieldValue)
			}
			cols := vector.MustFixedCol[uint64](vec)
			cols[rowIdx] = val

		case types.T_float64:
			val, ok := fieldValue.(float64)
			if !ok {
				return moerr.NewInternalError(ctx, "expected float64 type for column %d but got %T", colIdx, fieldValue)
			}
			cols := vector.MustFixedCol[float64](vec)
			cols[rowIdx] = val

		case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
			val, ok := fieldValue.(string)
			if !ok {
				return moerr.NewInternalError(ctx, "expected string type for column %d but got %T", colIdx, fieldValue)
			}
			err := vector.SetStringAt(vec, rowIdx, val, mp)
			if err != nil {
				return err
			}

		case types.T_bool:
			val, ok := fieldValue.(bool)
			if !ok {
				return moerr.NewInternalError(ctx, "expected bool type for column %d but got %T", colIdx, fieldValue)
			}
			cols := vector.MustFixedCol[bool](vec)
			cols[rowIdx] = val

		case types.T_json:
			val, ok := fieldValue.([]byte)
			if !ok || len(val) == 0 {
				strVal, strOk := fieldValue.(string)
				if !strOk {
					return moerr.NewInternalError(ctx, "expected bytes or string type for JSON column %d but got %T", colIdx, fieldValue)
				}
				val = []byte(strVal)
			}
			err := vector.SetBytesAt(vec, rowIdx, val, mp)
			if err != nil {
				return err
			}

		case types.T_datetime:
			val, ok := fieldValue.(string)
			if !ok {
				return moerr.NewInternalError(ctx, "expected string type for Datetime column %d but got %T", colIdx, fieldValue)
			}
			cols := vector.MustFixedCol[types.Datetime](vec)
			if len(val) == 0 {
				cols[rowIdx] = types.Datetime(0)
			} else {
				d, err := types.ParseDatetime(val, vec.GetType().Scale)
				if err != nil {
					return moerr.NewInternalError(ctx, "the input value is not Datetime type for column %d: %v", colIdx, fieldValue)
				}
				cols[rowIdx] = d
			}

		default:
			return moerr.NewInternalError(ctx, "the value type %s is not supported now", *vec.GetType())
		}
	}
	return nil
}

func ConvertProtobufSchemaToMD(schema string, msgTypeName string) (*desc.MessageDescriptor, error) {
	files := map[string]string{
		"test.proto": schema,
	}

	parser := protoparse.Parser{
		Accessor: protoparse.FileContentsFromMap(files),
	}
	fds, err := parser.ParseFiles("test.proto")

	if err != nil {
		log.Fatalf("Failed to parse proto content: %v", err)
	}
	fd := fds[0]
	md := fd.FindMessage(msgTypeName)
	return md, nil
}

func DeserializeProtobuf(md *desc.MessageDescriptor, in []byte) (*dynamic.Message, error) {
	dm := dynamic.NewMessage(md)
	bytesRead, _, err := readMessageIndexes(in[5:])
	err = proto.Unmarshal(in[5+bytesRead:], dm)
	return dm, err
}

func readMessageIndexes(payload []byte) (int, []int, error) {
	arrayLen, bytesRead := binary.Varint(payload)
	if bytesRead <= 0 {
		return bytesRead, nil, fmt.Errorf("unable to read message indexes")
	}
	if arrayLen == 0 {
		// Handle the optimization for the first message in the schema
		return bytesRead, []int{0}, nil
	}
	msgIndexes := make([]int, arrayLen)
	for i := 0; i < int(arrayLen); i++ {
		idx, read := binary.Varint(payload[bytesRead:])
		if read <= 0 {
			return bytesRead, nil, fmt.Errorf("unable to read message indexes")
		}
		bytesRead += read
		msgIndexes[i] = int(idx)
	}
	return bytesRead, msgIndexes, nil
}
