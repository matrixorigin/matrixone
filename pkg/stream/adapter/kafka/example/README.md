# KafkaAdapter Usage Guide

This guide provides a comprehensive walkthrough on how to utilize the `mokafka` package to interact with Kafka. From initializing the adapter to producing and consuming messages, each step is detailed to ensure a smooth integration.

## 0. Start the Kafka Cluster local

1. Download the latest Kafka release and extract it:
   https://docs.confluent.io/platform/current/installation/installing_cp/zip-tar.html
2. Run the confluent cli 

## 1. Initialize the `NewKafkaAdapter`

### **Using `mokafka.NewKafkaAdapter` with Kafka Authentication**

The `mokafka.NewKafkaAdapter` function is your entry point to create a new Kafka adapter instance. Depending on your Kafka setup, you might need to set up authentication. This is achieved by configuring the `kafka.ConfigMap` appropriately.

### **Common Parameters:**

Before diving into the authentication specifics, it's essential to understand some commonly used parameters:

- **`bootstrap.servers`**: Specifies the broker addresses. For instance: `"localhost:9092"`.
- **`group.id`**: A unique identifier for the consumer group.
- **`auto.offset.reset`**: Determines the consumer's behavior if there's no initial offset. Typical values include `"earliest"` or `"latest"`.
- **`enable.auto.commit`**: When set to `true`, the consumer's offset will be periodically auto-committed.
- **`session.timeout.ms`**: This timeout is used to detect consumer failures when using Kafka's group management.

### **1. SASL/PLAIN:**

SASL/PLAIN is a straightforward username/password authentication mechanism.

```go
config := &kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "group.id":          "myGroup",
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanisms":   "PLAIN",
    "sasl.username":     "your-username",
    "sasl.password":     "your-password",
}

ka, err := mokafka.NewKafkaAdapter(config)
```

⚠️ **Note**: SASL/PLAIN transmits the password in plaintext. Always use it in conjunction with SSL/TLS to encrypt the connection.

### **2. SASL/SCRAM:**

SCRAM provides a more secure username/password authentication mechanism compared to PLAIN.

```go
config := &kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "group.id":          "myGroup",
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanisms":   "SCRAM-SHA-256", // or "SCRAM-SHA-512"
    "sasl.username":     "your-username",
    "sasl.password":     "your-password",
}

ka, err := mokafka.NewKafkaAdapter(config)
```

### **Future Implementations:**

In the pipeline, we're looking to support more authentication mechanisms, including:

- SSL/TLS for client authentication and data encryption.
- SASL/GSSAPI (Kerberos) for environments that use Kerberos authentication.
- SASL/OAUTHBEARER for OAuth 2.0 token-based authentication.

---

## 2. Create a Topic

After initializing the KafkaAdapter, you can proceed to create a new topic:

```go
err = ka.CreateTopic(context.Background(), topicName, partitions, replicationFactor)
if err != nil {
    fmt.Fprintf(os.Stderr, "Failed to create topic: %s\n", err)
}
```

## 3. Produce Messages

To send messages to Kafka, serialization is essential. In this example, Protocol Buffers (protobuf) is employed for this purpose:

```go
s, err := protobuf.NewSerializer(ka.SchemaRegistry, serde.ValueSerde, protobuf.NewSerializerConfig())
testMSG := test_v1.MOTestMessage{Value: 42}
testKey := uuid.New()
serialized, err := s.Serialize(topicName, &testMSG)
offset, err := ka.ProduceMessage(topicName, testKey[:], serialized)
```

## 4. Read Messages

To fetch messages from Kafka:

```go
msgs, err := ka.ReadMessagesFromTopic(topicName, 0, 1)
```

## 5. Parse Messages

After retrieving the messages, you can deserialize them to recover the original data:

```go
d, err := protobuf.NewDeserializer(ka.SchemaRegistry, serde.ValueSerde, protobuf.NewDeserializerConfig())
err = d.ProtoRegistry.RegisterMessage((&test_v1.MOTestMessage{}).ProtoReflect().Type())
for _, msg := range msgs {
    m, err := d.Deserialize(topicName, msg.Value)
    fmt.Printf("message %v with offset %d\n", m, offset)
}
```

For scenarios where the generated protobuf code isn't available, you can dynamically parse the proto message:

```go
res, err := mokafka.DeserializeProtobuf(schema, msgs[0].Value)
fmt.Printf("message %v with offset %d\n", res, offset)
```

---
