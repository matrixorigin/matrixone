package io.matrixone.connect.eos;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class SinkSettingsTest {
    @Test
    void unsafeConsumerModesAreRejectedBeforeTaskStarts() {
        Map<String, String> props = new HashMap<>(Map.of(
            "consumer.override.isolation.level", "read_committed",
            "consumer.override.auto.offset.reset", "earliest",
            AtomicSinkConnector.JDBC_URL, "jdbc:mysql://127.0.0.1/profile",
            AtomicSinkConnector.JDBC_USER, "sink",
            AtomicSinkConnector.JDBC_PASSWORD, "test",
            AtomicSinkConnector.SCOPE, "orders",
            AtomicSinkConnector.BOOTSTRAP, "127.0.0.1:9092"));
        props.put("consumer.override.isolation.level", "read_uncommitted");
        assertThrows(IllegalArgumentException.class, () -> SinkSettings.from(props));
        props.put("consumer.override.isolation.level", "read_committed");
        props.remove("consumer.override.auto.offset.reset");
        assertThrows(IllegalArgumentException.class, () -> SinkSettings.from(props));
        props.put("consumer.override.auto.offset.reset", "earliest");
        props.put("errors.tolerance", "all");
        assertThrows(IllegalArgumentException.class, () -> SinkSettings.from(props));
        props.put("errors.tolerance", "none");
        props.put("transforms", "dropRecords");
        assertThrows(IllegalArgumentException.class, () -> SinkSettings.from(props));
        props.remove("transforms");
        props.put("mo.kafka.client.bootstrap.servers", "other-broker:9092");
        assertThrows(IllegalArgumentException.class, () -> SinkSettings.from(props));
    }
}
