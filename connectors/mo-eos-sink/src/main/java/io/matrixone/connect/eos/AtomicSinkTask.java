package io.matrixone.connect.eos;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;

/** Synchronous Connect task: the MO checkpoint, never the broker offset, owns recovery. */
public final class AtomicSinkTask extends SinkTask {
    private record Owner(long epoch, long cursor, String topicId) {}

    private SinkTaskContext sinkContext;
    private SinkSettings settings;
    private SinkStore store;
    private Admin admin;
    private final Map<TopicPartition, Owner> owners = new HashMap<>();

    @Override
    public void initialize(SinkTaskContext context) {
        super.initialize(context);
        this.sinkContext = context;
    }

    @Override
    public void start(Map<String, String> props) {
        settings = SinkSettings.from(props);
        store = new SinkStore(settings);
        Properties kafka = new Properties();
        kafka.put("bootstrap.servers", settings.bootstrap);
        kafka.put("client.id", "mo-eos-sink-preflight");
        // The same security properties used by the worker consumer can be
        // supplied as mo.kafka.client.* for the broker retention preflight.
        props.forEach((key, value) -> {
            if (key.startsWith("mo.kafka.client.")) {
                kafka.put(key.substring("mo.kafka.client.".length()), value);
            }
        });
        admin = Admin.create(kafka);
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        if (sinkContext == null || store == null) {
            throw new ConnectException("sink task was not initialized");
        }
        for (TopicPartition tp : partitions) {
            String topicId = topicIdentity(tp);
            SinkStore.Checkpoint checkpoint = store.claim(tp, topicId);
            long next = checkpoint.offset() + 1;
            requireRetained(tp, next, topicId);
            owners.put(tp, new Owner(checkpoint.epoch(), checkpoint.offset(), topicId));
            sinkContext.offset(tp, next);
        }
    }

    private String topicIdentity(TopicPartition tp) {
        try {
            Uuid id = admin.describeTopics(List.of(tp.topic())).allTopicNames()
                .get(10, TimeUnit.SECONDS).get(tp.topic()).topicId();
            if (id == null || Uuid.RESERVED.contains(id)) {
                throw new ConnectException("broker returned unsupported Kafka topic identity for " + tp);
            }
            return id.toString();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Kafka topic identity query interrupted for " + tp, error);
        } catch (ExecutionException | TimeoutException error) {
            throw new ConnectException("Kafka topic identity query failed for " + tp, error);
        }
    }

    private void requireRetained(TopicPartition tp, long next, String expectedTopicId) {
        String actualTopicId = topicIdentity(tp);
        if (!expectedTopicId.equals(actualTopicId)) {
            throw new ConnectException("Kafka topic identity changed for " + tp);
        }
        try {
            ConfigResource topicConfig = new ConfigResource(ConfigResource.Type.TOPIC, tp.topic());
            String cleanup = admin.describeConfigs(List.of(topicConfig)).all()
                .get(10, TimeUnit.SECONDS).get(topicConfig).get("cleanup.policy").value();
            if (!"delete".equals(cleanup)) {
                throw new ConnectException("Kafka topic must use delete-only cleanup policy for " + tp);
            }
            long earliest = admin.listOffsets(Map.of(tp, OffsetSpec.earliest()))
                .partitionResult(tp).get(10, TimeUnit.SECONDS).offset();
            if (next < earliest) {
                throw new ConnectException("MO checkpoint " + next
                    + " is before Kafka retained beginning " + earliest + " for " + tp);
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Kafka retention preflight interrupted for " + tp, error);
        } catch (ExecutionException error) {
            throw new ConnectException("Kafka retention preflight failed for " + tp, error);
        } catch (TimeoutException error) {
            throw new ConnectException("Kafka retention preflight timed out for " + tp, error);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        // Parse the entire callback before any partition commits. A poison
        // event thus cannot cause an avoidable partial multi-partition call.
        Map<TopicPartition, List<SinkStore.Delivery>> byPartition = new LinkedHashMap<>();
        for (SinkRecord record : records) {
            TopicPartition tp = originalPartition(record);
            if (!owners.containsKey(tp)) {
                throw new ConnectException("record delivered without assigned owner for " + tp);
            }
            EventEnvelope event = EventEnvelope.parse(record.value(), settings.scopeBytes);
            byPartition.computeIfAbsent(tp, ignored -> new ArrayList<>())
                .add(new SinkStore.Delivery(record.originalKafkaOffset(), event));
        }
        for (Map.Entry<TopicPartition, List<SinkStore.Delivery>> partition : byPartition.entrySet()) {
            TopicPartition tp = partition.getKey();
            Owner owner = owners.get(tp);
            // Connect initializes a fresh consumer position before invoking
            // SinkTask.open. It needs auto.offset.reset=earliest, so detect a
            // later retention-driven reset here before any database effect.
            requireRetained(tp, owner.cursor() + 1, owner.topicId());
            List<SinkStore.Delivery> deliveries = partition.getValue();
            for (int start = 0; start < deliveries.size(); start += settings.batchSize) {
                int end = Math.min(start + settings.batchSize, deliveries.size());
                long durable = store.apply(tp, new SinkStore.Checkpoint(owner.cursor(), owner.epoch(), owner.topicId()),
                    owner.cursor(), deliveries.subList(start, end));
                owner = new Owner(owner.epoch(), durable, owner.topicId());
                owners.put(tp, owner);
            }
        }
    }

    private TopicPartition originalPartition(SinkRecord record) {
        String topic = record.originalTopic();
        Integer partition = record.originalKafkaPartition();
        if (topic == null || partition == null || record.originalKafkaOffset() < 0) {
            throw new ConnectException("record lacks original Kafka coordinates");
        }
        return new TopicPartition(topic, partition);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
        Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        Map<TopicPartition, OffsetAndMetadata> safe = new HashMap<>();
        for (Map.Entry<TopicPartition, Owner> entry : owners.entrySet()) {
            TopicPartition tp = entry.getKey();
            SinkStore.Checkpoint durable = store.read(tp);
            Owner owner = entry.getValue();
            if (durable.epoch() != owner.epoch() || durable.offset() != owner.cursor()
                || !owner.topicId().equals(durable.topicId())) {
                throw new ConnectException("owner/checkpoint changed before offset commit for " + tp);
            }
            OffsetAndMetadata current = currentOffsets.get(tp);
            if (current != null && current.offset() >= owner.cursor() + 1) {
                safe.put(tp, new OffsetAndMetadata(owner.cursor() + 1));
            }
        }
        return safe;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        // put is synchronous and each returned batch is already durable.
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        partitions.forEach(owners::remove);
    }

    @Override
    public void stop() {
        owners.clear();
        if (admin != null) {
            admin.close(Duration.ofSeconds(5));
            admin = null;
        }
        store = null;
    }

    @Override
    public String version() {
        return "0.1.0";
    }
}
