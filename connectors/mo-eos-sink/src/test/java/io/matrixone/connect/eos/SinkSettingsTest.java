package io.matrixone.connect.eos;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class SinkSettingsTest {
    static Map<String, String> validProps(String url) {
        return new HashMap<>(Map.of(
            "consumer.override.isolation.level", "read_committed",
            "consumer.override.auto.offset.reset", "earliest",
            AtomicSinkConnector.JDBC_URL, url,
            AtomicSinkConnector.JDBC_USER, "sink",
            AtomicSinkConnector.JDBC_PASSWORD, "test",
            AtomicSinkConnector.SCOPE, "orders",
            AtomicSinkConnector.BOOTSTRAP, "127.0.0.1:9092"));
    }

    @Test
    void jdbcDeadlinesAreFiniteAndCannotBeDisabledByUrl() {
        Map<String, String> props = validProps("jdbc:mysql://127.0.0.1/profile");
        SinkSettings defaults = SinkSettings.from(props);
        assertEquals(5000, defaults.connectTimeoutMs);
        assertEquals(30000, defaults.socketTimeoutMs);
        props.put(AtomicSinkConnector.JDBC_SOCKET_TIMEOUT, "0");
        assertThrows(IllegalArgumentException.class, () -> SinkSettings.from(props));
        props.put(AtomicSinkConnector.JDBC_SOCKET_TIMEOUT, "60001");
        assertThrows(IllegalArgumentException.class, () -> SinkSettings.from(props));
        props.remove(AtomicSinkConnector.JDBC_SOCKET_TIMEOUT);
        props.put(AtomicSinkConnector.JDBC_URL, "jdbc:mysql://127.0.0.1/profile?socketTimeout=0");
        assertThrows(IllegalArgumentException.class, () -> SinkSettings.from(props));

        props.put(AtomicSinkConnector.JDBC_URL,
            "jdbc:mysql://address=(host=127.0.0.1)(port=6001)(socketTimeout=0)/profile");
        assertThrows(IllegalArgumentException.class, () -> SinkSettings.from(props));
        props.put(AtomicSinkConnector.JDBC_URL,
            "jdbc:mysql://address=(host=127.0.0.1)(port=6001)(connectTimeout=0)/profile");
        assertThrows(IllegalArgumentException.class, () -> SinkSettings.from(props));
        props.put(AtomicSinkConnector.JDBC_URL,
            "jdbc:mysql://address=(host=127.0.0.1)(port=6001)(SOCKETTIMEOUT=0)/profile");
        assertThrows(IllegalArgumentException.class, () -> SinkSettings.from(props));
        props.put(AtomicSinkConnector.JDBC_URL,
            "jdbc:mysql:loadbalance://address=(host=127.0.0.1)(port=6001),"
                + "address=(host=127.0.0.2)(port=6001)(socketTimeout=0)/profile");
        assertThrows(IllegalArgumentException.class, () -> SinkSettings.from(props));
        props.put(AtomicSinkConnector.JDBC_URL,
            "jdbc:mysql://address=(host=127.0.0.1)(port=6001)/profile");
        assertEquals(30000, SinkSettings.from(props).socketTimeoutMs);
        props.put(AtomicSinkConnector.JDBC_URL,
            "jdbc:mysql:loadbalance://address=(host=127.0.0.1)(port=6001),"
                + "address=(host=127.0.0.2)(port=6001)/profile");
        assertEquals(30000, SinkSettings.from(props).socketTimeoutMs);
    }

    @Test
    void unsafeConsumerModesAreRejectedBeforeTaskStarts() {
        Map<String, String> props = validProps("jdbc:mysql://127.0.0.1/profile");
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
