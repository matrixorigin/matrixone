package io.matrixone.connect.eos;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

/** Run against an isolated real broker + MatrixOne; no mocking of JDBC writes. */
public final class IntegrationProbe {
    private static final String TOPIC = "mo_eos_events";
    private static final String BATCH_TOPIC = "mo_eos_batch";
    private static final String RETAINED_ASSIGN_TOPIC = "mo_eos_retained_assign";
    private static final String RETAINED_PUT_TOPIC = "mo_eos_retained_put";
    private static final String HOLE_TOPIC = "mo_eos_hole";
    private static final String RECREATED_TOPIC = "mo_eos_recreated";
    private static final String IDENTITY_TOPIC = "mo_eos_identity";
    private static final String COMPACT_TOPIC = "mo_eos_compacted";
    private static final byte[] SCOPE = "orders".getBytes(StandardCharsets.UTF_8);

    private final String broker;
    private final String jdbc;
    private final String password;

    private IntegrationProbe(String broker, String jdbc, String password) {
        this.broker = broker;
        this.jdbc = jdbc;
        this.password = password;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2 || System.getenv("MO_TEST_PASSWORD") == null) {
            throw new IllegalArgumentException("usage: IntegrationProbe BROKER JDBC_URL; set MO_TEST_PASSWORD");
        }
        IntegrationProbe probe = new IntegrationProbe(args[0], args[1], System.getenv("MO_TEST_PASSWORD"));
        probe.run();
        System.out.println("PASS real Kafka/MO: discovery, seek, dedup, rollback, fencing, retention, hole, lineage, byte IDs, cleanup policy");
    }

    private void run() throws Exception {
        boolean found = java.util.ServiceLoader.load(Connector.class).stream()
            .anyMatch(provider -> provider.type().equals(AtomicSinkConnector.class));
        check(found, "Connect ServiceLoader did not discover plugin");
        createTopics();
        TopicPartition tp = new TopicPartition(TOPIC, 0);
        FakeContext context = new FakeContext(Set.of(tp));
        AtomicSinkTask task = task(context);
        task.open(List.of(tp));
        check(context.offsets.get(tp) == 0, "assignment did not seek from database checkpoint");

        byte[] first = event("A", 7, "提交确认😀");
        byte[] second = event("B", 3, "second");
        produce(TOPIC, first, first, second);
        List<SinkRecord> delivered = consume(TOPIC, 0, 3);
        task.put(delivered);
        check(count("mo_sink_ledger") == 2, "ledger missing IDs");
        check(total() == 10, "duplicate applied a second effect");
        check(checkpoint(TOPIC) == 2, "checkpoint did not cover committed batch");
        Map<TopicPartition, OffsetAndMetadata> safe = task.preCommit(
            Map.of(tp, new OffsetAndMetadata(3)));
        check(safe.get(tp).offset() == 3, "preCommit returned unsafe offset");
        task.close(List.of(tp));
        task.stop();

        task = task(context);
        task.open(List.of(tp));
        check(context.offsets.get(tp) == 3, "restart did not seek from MO checkpoint");
        produce(TOPIC, first);
        task.put(consume(TOPIC, 3, 1));
        check(total() == 10 && count("mo_sink_ledger") == 2 && checkpoint(TOPIC) == 3,
            "post-restart duplicate changed business effect");

        TopicPartition batchTp = new TopicPartition(BATCH_TOPIC, 0);
        insertCheckpoint(BATCH_TOPIC);
        task.open(List.of(batchTp));
        produce(BATCH_TOPIC, event("C", 2, "c"), event("D", 5, "d"), event("A", 100, "conflict"));
        AtomicSinkTask current = task;
        List<SinkRecord> batch = consume(BATCH_TOPIC, 0, 3);
        try {
            current.put(batch);
            throw new AssertionError("digest conflict was accepted");
        } catch (ConnectException expected) {
            check(expected.getMessage().contains("different digest"), "wrong conflict: " + expected);
        }
        check(total() == 10 && count("mo_sink_ledger") == 2 && checkpoint(BATCH_TOPIC) == -1,
            "failed microbatch partially committed");

        SinkStore store = new SinkStore(SinkSettings.from(settings()));
        store.claim(tp, store.read(tp).topicId()); // A later owner fences the old task's epoch.
        try {
            task.preCommit(Map.of(tp, new OffsetAndMetadata(4)));
            throw new AssertionError("old owner committed after epoch changed");
        } catch (ConnectException expected) {
            check(expected.getMessage().contains("owner/checkpoint changed"), "wrong fencing error");
        }
        task.stop();
        retentionAndHoleProbe();
    }

    private void retentionAndHoleProbe() throws Exception {
        long initialTotal = total();
        long initialRows = count("mo_sink_ledger");
        for (String topic : List.of(RETAINED_ASSIGN_TOPIC, RETAINED_PUT_TOPIC, HOLE_TOPIC,
                                    RECREATED_TOPIC, IDENTITY_TOPIC, COMPACT_TOPIC)) {
            insertCheckpoint(topic);
        }
        TopicPartition assignmentTp = new TopicPartition(RETAINED_ASSIGN_TOPIC, 0);
        produce(RETAINED_ASSIGN_TOPIC, event("retained-a", 1, "a"), event("retained-b", 1, "b"));
        deleteBefore(assignmentTp, 1);
        AtomicSinkTask task = task(new FakeContext(Set.of(assignmentTp)));
        try {
            task.open(List.of(assignmentTp));
            throw new AssertionError("assignment accepted expired checkpoint");
        } catch (ConnectException expected) {
            check(expected.getMessage().contains("before Kafka retained beginning"), "wrong assignment failure");
        } finally {
            task.stop();
        }
        check(checkpoint(RETAINED_ASSIGN_TOPIC) == -1, "assignment moved checkpoint");

        TopicPartition putTp = new TopicPartition(RETAINED_PUT_TOPIC, 0);
        task = task(new FakeContext(Set.of(putTp)));
        task.open(List.of(putTp));
        produce(RETAINED_PUT_TOPIC, event("retained-c", 1, "c"), event("retained-d", 1, "d"));
        List<SinkRecord> delivered = consume(RETAINED_PUT_TOPIC, 1, 1);
        deleteBefore(putTp, 1);
        try {
            task.put(delivered);
            throw new AssertionError("put accepted expired checkpoint");
        } catch (ConnectException expected) {
            check(expected.getMessage().contains("before Kafka retained beginning"), "wrong put failure");
        } finally {
            task.stop();
        }
        check(checkpoint(RETAINED_PUT_TOPIC) == -1, "put moved checkpoint");
        check(total() == initialTotal && count("mo_sink_ledger") == initialRows,
            "retention failure applied business SQL");

        TopicPartition compactTp = new TopicPartition(COMPACT_TOPIC, 0);
        task = task(new FakeContext(Set.of(compactTp)));
        try {
            task.open(List.of(compactTp));
            throw new AssertionError("compacted topic was accepted");
        } catch (ConnectException expected) {
            check(expected.getMessage().contains("delete-only cleanup policy"), "wrong cleanup failure");
        } finally {
            task.stop();
        }
        check(checkpoint(COMPACT_TOPIC) == -1, "compacted topic moved checkpoint");

        TopicPartition holeTp = new TopicPartition(HOLE_TOPIC, 0);
        task = task(new FakeContext(Set.of(holeTp)));
        task.open(List.of(holeTp));
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "mo-eos-hole-probe");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(config,
            new ByteArraySerializer(), new ByteArraySerializer())) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(HOLE_TOPIC, event("aborted", 100, "discard")))
                .get(30, TimeUnit.SECONDS);
            producer.abortTransaction();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(HOLE_TOPIC, event("committed-hole", 2, "keep")))
                .get(30, TimeUnit.SECONDS);
            producer.commitTransaction();
        }
        List<SinkRecord> visible = consume(HOLE_TOPIC, 0, 1);
        check(visible.get(0).originalKafkaOffset() > 0, "transaction did not create an offset hole");
        task.put(visible);
        check(checkpoint(HOLE_TOPIC) == visible.get(0).originalKafkaOffset(), "hole checkpoint wrong");
        check(total() == initialTotal + 2 && count("mo_sink_ledger") == initialRows + 1,
            "aborted event or hole applied incorrectly");
        task.stop();

        TopicPartition recreatedTp = new TopicPartition(RECREATED_TOPIC, 0);
        task = task(new FakeContext(Set.of(recreatedTp)));
        task.open(List.of(recreatedTp));
        produce(RECREATED_TOPIC, event("old-generation", 1, "old"));
        task.put(consume(RECREATED_TOPIC, 0, 1));
        task.stop();
        long beforeRecreateTotal = total();
        long beforeRecreateRows = count("mo_sink_ledger");
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", broker))) {
            admin.deleteTopics(List.of(RECREATED_TOPIC)).all().get(30, TimeUnit.SECONDS);
            admin.createTopics(List.of(new NewTopic(RECREATED_TOPIC, 1, (short) 1)))
                .all().get(30, TimeUnit.SECONDS);
        }
        produce(RECREATED_TOPIC, event("new-generation", 100, "new"));
        task = task(new FakeContext(Set.of(recreatedTp)));
        try {
            task.open(List.of(recreatedTp));
            throw new AssertionError("new Kafka topic generation reused old checkpoint");
        } catch (ConnectException expected) {
            check(expected.getMessage().contains("Kafka topic identity changed"),
                "wrong recreated-topic failure");
        } finally {
            task.stop();
        }
        check(total() == beforeRecreateTotal && count("mo_sink_ledger") == beforeRecreateRows
            && checkpoint(RECREATED_TOPIC) == 0,
            "recreated Kafka topic changed MO state");

        TopicPartition identityTp = new TopicPartition(IDENTITY_TOPIC, 0);
        task = task(new FakeContext(Set.of(identityTp)));
        task.open(List.of(identityTp));
        String[] ids = {"Case", "case", "Case ", "é", "e\u0301"};
        for (String id : ids) {
            produce(IDENTITY_TOPIC, event(id, 1, "identity"));
        }
        task.put(consume(IDENTITY_TOPIC, 0, ids.length));
        for (String id : ids) {
            check(ledgerHas(id), "VARBINARY ledger ID missing: " + id);
        }
        check(count("mo_sink_ledger") == beforeRecreateRows + ids.length
            && total() == beforeRecreateTotal + ids.length,
            "case, trailing-space, or Unicode IDs collided");
        task.stop();
    }

    private void deleteBefore(TopicPartition tp, long offset) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", broker))) {
            admin.deleteRecords(Map.of(tp, RecordsToDelete.beforeOffset(offset)))
                .all().get(30, TimeUnit.SECONDS);
        }
    }

    private AtomicSinkTask task(FakeContext context) {
        AtomicSinkTask task = new AtomicSinkTask();
        task.initialize(context);
        task.start(settings());
        return task;
    }

    private Map<String, String> settings() {
        return Map.of(
            "consumer.override.isolation.level", "read_committed",
            "consumer.override.auto.offset.reset", "earliest",
            AtomicSinkConnector.JDBC_URL, jdbc,
            AtomicSinkConnector.JDBC_USER, "root",
            AtomicSinkConnector.JDBC_PASSWORD, password,
            AtomicSinkConnector.SCOPE, "orders",
            AtomicSinkConnector.BATCH_SIZE, "64",
            AtomicSinkConnector.BOOTSTRAP, broker);
    }

    private void createTopics() throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", broker))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1),
                new NewTopic(BATCH_TOPIC, 1, (short) 1),
                new NewTopic(RETAINED_ASSIGN_TOPIC, 1, (short) 1),
                new NewTopic(RETAINED_PUT_TOPIC, 1, (short) 1),
                new NewTopic(HOLE_TOPIC, 1, (short) 1),
                new NewTopic(RECREATED_TOPIC, 1, (short) 1),
                new NewTopic(IDENTITY_TOPIC, 1, (short) 1),
                new NewTopic(COMPACT_TOPIC, 1, (short) 1)
                    .configs(Map.of("cleanup.policy", "compact")))).all().get(30, TimeUnit.SECONDS);
        }
    }

    private void produce(String topic, byte[]... values) throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(config,
            new ByteArraySerializer(), new ByteArraySerializer())) {
            for (byte[] value : values) {
                producer.send(new ProducerRecord<>(topic, value)).get(30, TimeUnit.SECONDS);
            }
        }
    }

    private List<SinkRecord> consume(String topic, long offset, int count) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "eos-probe-" + topic);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        List<SinkRecord> result = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config,
            new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            TopicPartition tp = new TopicPartition(topic, 0);
            consumer.assign(List.of(tp));
            consumer.seek(tp, offset);
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (result.size() < count && System.nanoTime() < deadline) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    result.add(new SinkRecord(record.topic(), record.partition(), null, null,
                        null, record.value(), record.offset(), record.timestamp(),
                        TimestampType.CREATE_TIME, List.of(), record.topic(),
                        record.partition(), record.offset()));
                }
            }
        }
        check(result.size() == count, "expected " + count + " Kafka records, got " + result.size());
        return result;
    }

    private void insertCheckpoint(String topic) throws Exception {
        try (Connection connection = connection(); PreparedStatement insert = connection.prepareStatement(
            "INSERT INTO mo_sink_checkpoint(scope,topic,partition_id,topic_id,last_offset,owner_epoch) "
                + "VALUES (?,?,0,NULL,-1,0)")) {
            insert.setBytes(1, SCOPE);
            insert.setBytes(2, topic.getBytes(StandardCharsets.UTF_8));
            check(insert.executeUpdate() == 1, "checkpoint insert failed");
        }
    }

    private Connection connection() throws Exception {
        return DriverManager.getConnection(jdbc, "root", password);
    }

    private long scalar(String sql) throws Exception {
        try (Connection connection = connection(); PreparedStatement query = connection.prepareStatement(sql)) {
            try (ResultSet rows = query.executeQuery()) {
                check(rows.next(), "query returned no rows: " + sql);
                return rows.getLong(1);
            }
        }
    }

    private long count(String table) throws Exception {
        return scalar("SELECT count(*) FROM " + table);
    }

    private long total() throws Exception {
        return scalar("SELECT n FROM mo_sink_totals");
    }

    private long checkpoint(String topic) throws Exception {
        try (Connection connection = connection(); PreparedStatement query = connection.prepareStatement(
            "SELECT last_offset FROM mo_sink_checkpoint WHERE scope=? AND topic=? AND partition_id=0")) {
            query.setBytes(1, SCOPE);
            query.setBytes(2, topic.getBytes(StandardCharsets.UTF_8));
            try (ResultSet rows = query.executeQuery()) {
                check(rows.next(), "checkpoint row missing");
                return rows.getLong(1);
            }
        }
    }

    private boolean ledgerHas(String id) throws Exception {
        try (Connection connection = connection(); PreparedStatement query = connection.prepareStatement(
            "SELECT COUNT(*) FROM mo_sink_ledger WHERE scope=? AND event_id=?")) {
            query.setBytes(1, SCOPE);
            query.setBytes(2, id.getBytes(StandardCharsets.UTF_8));
            try (ResultSet rows = query.executeQuery()) {
                return rows.next() && rows.getLong(1) == 1;
            }
        }
    }

    private static byte[] event(String id, long delta, String payload) {
        String json = "{\"version\":1,\"event_id\":\"" + id + "\",\"operation\":\"increment\","
            + "\"delta\":" + delta + ",\"payload\":\"" + payload + "\"}";
        return json.getBytes(StandardCharsets.UTF_8);
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static final class FakeContext implements SinkTaskContext {
        final Map<TopicPartition, Long> offsets = new HashMap<>();
        final Set<TopicPartition> assignment;
        FakeContext(Set<TopicPartition> assignment) { this.assignment = assignment; }
        @Override public Map<String, String> configs() { return Map.of(); }
        @Override public void offset(Map<TopicPartition, Long> positions) { offsets.putAll(positions); }
        @Override public void offset(TopicPartition tp, long position) { offsets.put(tp, position); }
        @Override public void timeout(long timeoutMs) {}
        @Override public Set<TopicPartition> assignment() { return assignment; }
        @Override public void pause(TopicPartition... partitions) {}
        @Override public void resume(TopicPartition... partitions) {}
        @Override public void requestCommit() {}
    }
}
