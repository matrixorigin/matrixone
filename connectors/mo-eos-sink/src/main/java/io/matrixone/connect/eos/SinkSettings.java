package io.matrixone.connect.eos;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

final class SinkSettings {
    final String jdbcUrl;
    final String jdbcUser;
    final String jdbcPassword;
    final int connectTimeoutMs;
    final int socketTimeoutMs;
    final String scope;
    final byte[] scopeBytes;
    final int batchSize;
    final String bootstrap;

    private SinkSettings(String jdbcUrl, String jdbcUser, String jdbcPassword,
                         int connectTimeoutMs, int socketTimeoutMs,
                         String scope, byte[] scopeBytes, int batchSize, String bootstrap) {
        this.jdbcUrl = jdbcUrl;
        this.jdbcUser = jdbcUser;
        this.jdbcPassword = jdbcPassword;
        this.connectTimeoutMs = connectTimeoutMs;
        this.socketTimeoutMs = socketTimeoutMs;
        this.scope = scope;
        this.scopeBytes = scopeBytes;
        this.batchSize = batchSize;
        this.bootstrap = bootstrap;
    }

    static SinkSettings from(Map<String, String> props) {
        requireValue(props, "consumer.override.isolation.level", "read_committed");
        requireValue(props, "consumer.override.auto.offset.reset", "earliest");
        String tolerance = props.getOrDefault("errors.tolerance", "none");
        if (!"none".equals(tolerance) || !props.getOrDefault("transforms", "").isEmpty()) {
            throw new IllegalArgumentException("errors.tolerance must be none and transforms must be absent");
        }
        if (props.containsKey("mo.kafka.client.bootstrap.servers")) {
            throw new IllegalArgumentException("mo.kafka.client.bootstrap.servers must not override the preflight broker");
        }
        String url = required(props, AtomicSinkConnector.JDBC_URL);
        // Connector/J URL parameters override connection Properties. Keep the
        // deadlines owned by this profile so URL parameters cannot disable them.
        if (url.matches("(?i).*[?&](connectTimeout|socketTimeout)=.*")) {
            throw new IllegalArgumentException("configure JDBC deadlines with mo.jdbc.*.timeout.ms, not URL parameters");
        }
        int connectTimeout = timeout(props, AtomicSinkConnector.JDBC_CONNECT_TIMEOUT, 5000);
        int socketTimeout = timeout(props, AtomicSinkConnector.JDBC_SOCKET_TIMEOUT, 30000);
        String user = required(props, AtomicSinkConnector.JDBC_USER);
        String password = props.get(AtomicSinkConnector.JDBC_PASSWORD);
        if (password == null) {
            throw new IllegalArgumentException("missing " + AtomicSinkConnector.JDBC_PASSWORD);
        }
        String scope = required(props, AtomicSinkConnector.SCOPE);
        byte[] scopeBytes = encodeScope(scope);
        if (scopeBytes.length > 128) {
            throw new IllegalArgumentException("mo.scope exceeds 128 UTF-8 bytes");
        }
        int batch = Integer.parseInt(props.getOrDefault(AtomicSinkConnector.BATCH_SIZE, "64"));
        if (batch < 1 || batch > 1000) {
            throw new IllegalArgumentException("mo.batch.size must be 1..1000");
        }
        SinkSettings settings = new SinkSettings(url, user, password, connectTimeout, socketTimeout, scope, scopeBytes, batch,
            required(props, AtomicSinkConnector.BOOTSTRAP));
        settings.requireEffectiveJdbcDeadlines();
        return settings;
    }

    private static byte[] encodeScope(String scope) {
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .encode(CharBuffer.wrap(scope));
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException error) {
            throw new IllegalArgumentException("mo.scope contains malformed Unicode", error);
        }
    }

    Properties jdbcProperties() {
        Properties properties = new Properties();
        properties.setProperty("user", jdbcUser);
        properties.setProperty("password", jdbcPassword);
        properties.setProperty("connectTimeout", Integer.toString(connectTimeoutMs));
        properties.setProperty("socketTimeout", Integer.toString(socketTimeoutMs));
        return properties;
    }

    private void requireEffectiveJdbcDeadlines() {
        // Connector/J lets address=(...)(socketTimeout=0) override connection
        // Properties. Inspect every host with the same Properties used to connect.
        ConnectionUrl parsed;
        try {
            parsed = ConnectionUrl.getConnectionUrlInstance(jdbcUrl, jdbcProperties());
        } catch (RuntimeException error) {
            throw new IllegalArgumentException("invalid mo.jdbc.url", error);
        }
        if (parsed.getHostsList().isEmpty()) {
            throw new IllegalArgumentException("mo.jdbc.url has no JDBC host");
        }
        for (HostInfo host : parsed.getHostsList()) {
            Map<String, String> effective = host.getHostProperties();
            if (!Integer.toString(connectTimeoutMs).equals(effective.get("connectTimeout"))
                || !Integer.toString(socketTimeoutMs).equals(effective.get("socketTimeout"))) {
                throw new IllegalArgumentException("JDBC URL must not override mo.jdbc.*.timeout.ms");
            }
            for (Map.Entry<String, String> property : effective.entrySet()) {
                if ((property.getKey().equalsIgnoreCase("connectTimeout")
                    && !Integer.toString(connectTimeoutMs).equals(property.getValue()))
                    || (property.getKey().equalsIgnoreCase("socketTimeout")
                    && !Integer.toString(socketTimeoutMs).equals(property.getValue()))) {
                    throw new IllegalArgumentException("JDBC URL must not override mo.jdbc.*.timeout.ms");
                }
            }
        }
    }

    private static int timeout(Map<String, String> props, String key, int defaultValue) {
        int value = Integer.parseInt(props.getOrDefault(key, Integer.toString(defaultValue)));
        if (value < 1 || value > 60000) {
            throw new IllegalArgumentException(key + " must be 1..60000");
        }
        return value;
    }

    private static String required(Map<String, String> props, String name) {
        String value = props.get(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("missing " + name);
        }
        return value;
    }

    private static void requireValue(Map<String, String> props, String name, String expected) {
        if (!expected.equals(props.get(name))) {
            throw new IllegalArgumentException(name + " must be " + expected);
        }
    }
}
