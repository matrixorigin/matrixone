package io.matrixone.connect.eos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/** Kafka Connect entry point for the MatrixOne transactional event sink. */
public final class AtomicSinkConnector extends SinkConnector {
    static final String JDBC_URL = "mo.jdbc.url";
    static final String JDBC_USER = "mo.jdbc.user";
    static final String JDBC_PASSWORD = "mo.jdbc.password";
    static final String SCOPE = "mo.scope";
    static final String BATCH_SIZE = "mo.batch.size";
    static final String BOOTSTRAP = "mo.kafka.bootstrap.servers";
    private Map<String, String> properties;

    @Override
    public void start(Map<String, String> props) {
        config().parse(props);
        properties = new HashMap<>(props);
        SinkSettings.from(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return AtomicSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> result = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            result.add(new HashMap<>(properties));
        }
        return result;
    }

    @Override
    public void stop() {
        properties = null;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
            .define(JDBC_URL, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH, "MatrixOne JDBC URL")
            .define(JDBC_USER, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH, "MatrixOne user")
            .define(JDBC_PASSWORD, ConfigDef.Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH, "MatrixOne password")
            .define(SCOPE, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH, "Exact-byte business isolation scope")
            .define(BATCH_SIZE, ConfigDef.Type.INT, 64, ConfigDef.Range.between(1, 1000),
                ConfigDef.Importance.MEDIUM, "Maximum records in one MO transaction")
            .define(BOOTSTRAP, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH, "Kafka broker for beginning-offset preflight");
    }

    @Override
    public String version() {
        return "0.1.0";
    }
}
