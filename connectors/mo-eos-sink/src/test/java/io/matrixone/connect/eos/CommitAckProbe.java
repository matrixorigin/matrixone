package io.matrixone.connect.eos;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.connect.errors.ConnectException;

/** Pair with drop_commit_ok_proxy.py against an isolated MatrixOne database. */
public final class CommitAckProbe {
    private CommitAckProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2 || System.getenv("MO_TEST_PASSWORD") == null) {
            throw new IllegalArgumentException("usage: CommitAckProbe DIRECT_JDBC PROXY_JDBC; set MO_TEST_PASSWORD");
        }
        Map<String, String> props = new HashMap<>();
        props.put("consumer.override.isolation.level", "read_committed");
        props.put("consumer.override.auto.offset.reset", "earliest");
        props.put(AtomicSinkConnector.JDBC_URL, args[0]);
        props.put(AtomicSinkConnector.JDBC_USER, "root");
        props.put(AtomicSinkConnector.JDBC_PASSWORD, System.getenv("MO_TEST_PASSWORD"));
        props.put(AtomicSinkConnector.SCOPE, "orders");
        props.put(AtomicSinkConnector.BOOTSTRAP, "unused:9092");
        SinkStore direct = new SinkStore(SinkSettings.from(props));
        TopicPartition tp = new TopicPartition("mo_eos_events", 0);
        SinkStore.Checkpoint owner = direct.claim(tp, Uuid.randomUuid().toString());
        EventEnvelope event = EventEnvelope.parse(
            "{\"version\":1,\"event_id\":\"lost-commit-ok\",\"operation\":\"increment\",\"delta\":5,\"payload\":\"once\"}"
                .getBytes(StandardCharsets.UTF_8), "orders".getBytes(StandardCharsets.UTF_8));
        props.put(AtomicSinkConnector.JDBC_URL, args[1]);
        SinkStore throughProxy = new SinkStore(SinkSettings.from(props));
        long offset = throughProxy.apply(tp, owner, -1, List.of(new SinkStore.Delivery(0, event)));
        check(offset == 0, "unknown commit did not resolve to durable offset");
        direct.apply(tp, owner, 0, List.of(new SinkStore.Delivery(0, event)));
        EventEnvelope zero = EventEnvelope.parse(
            "{\"version\":1,\"event_id\":\"zero-delta\",\"operation\":\"increment\",\"delta\":0,\"payload\":\"valid\"}"
                .getBytes(StandardCharsets.UTF_8), "orders".getBytes(StandardCharsets.UTF_8));
        direct.apply(tp, owner, 0, List.of(new SinkStore.Delivery(1, zero)));
        try (Connection connection = DriverManager.getConnection(args[0], "root", System.getenv("MO_TEST_PASSWORD"));
             Statement query = connection.createStatement();
             ResultSet rows = query.executeQuery("SELECT (SELECT n FROM mo_sink_totals),"
                 + "(SELECT COUNT(*) FROM mo_sink_ledger),"
                 + "(SELECT last_offset FROM mo_sink_checkpoint)")) {
            check(rows.next() && rows.getLong(1) == 5 && rows.getLong(2) == 2 && rows.getLong(3) == 1,
                "lost COMMIT OK, replay, or zero delta produced wrong MO state");
        }
        try (Connection connection = DriverManager.getConnection(args[0], "root", System.getenv("MO_TEST_PASSWORD"));
             Statement update = connection.createStatement()) {
            update.executeUpdate("DELETE FROM mo_sink_totals");
        }
        EventEnvelope missing = EventEnvelope.parse(
            "{\"version\":1,\"event_id\":\"missing-total\",\"operation\":\"increment\",\"delta\":0,\"payload\":\"reject\"}"
                .getBytes(StandardCharsets.UTF_8), "orders".getBytes(StandardCharsets.UTF_8));
        try {
            direct.apply(tp, owner, 1, List.of(new SinkStore.Delivery(2, missing)));
            throw new AssertionError("missing business row was accepted");
        } catch (ConnectException expected) {
            check(expected.getMessage().contains("missing or duplicate business total"),
                "wrong missing-row failure");
        }
        try (Connection connection = DriverManager.getConnection(args[0], "root", System.getenv("MO_TEST_PASSWORD"));
             Statement query = connection.createStatement();
             ResultSet rows = query.executeQuery("SELECT (SELECT COUNT(*) FROM mo_sink_ledger),"
                 + "(SELECT last_offset FROM mo_sink_checkpoint)")) {
            check(rows.next() && rows.getLong(1) == 2 && rows.getLong(2) == 1,
                "missing business row advanced ledger/checkpoint");
        }
        System.out.println("PASS real MO: dropped COMMIT OK, replay once, zero delta, missing row rollback");
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
