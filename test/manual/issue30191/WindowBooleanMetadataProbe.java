import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;

/**
 * Connector/J regression probe for BOOLEAN values produced by window
 * functions. Run this against an isolated MatrixOne instance with
 * tinyInt1isBit=true, which is the client setting that exposes a wrongly
 * advertised computed BOOLEAN as java.lang.Boolean.
 */
public final class WindowBooleanMetadataProbe {
    private static final String TABLE = "issue_30191_window_boolean_probe";

    private static void check(boolean value, String message) {
        if (!value) {
            throw new AssertionError(message);
        }
    }

    private static ResultSetMetaData metadata(ResultSet rows, String phase, boolean computed) throws SQLException {
        ResultSetMetaData metadata = rows.getMetaData();
        check(metadata.getColumnCount() == 1, phase + ": expected one result column");
        int type = metadata.getColumnType(1);
        String className = metadata.getColumnClassName(1);
        System.out.println(phase + "=" + metadata.getColumnTypeName(1) + ":" + type + ":" + className);
        if (computed) {
            check(type != Types.BIT, phase + ": computed BOOLEAN must not be advertised as BIT");
            check(!"java.lang.Boolean".equals(className), phase + ": computed BOOLEAN must not map to Boolean");
        } else {
            check(type == Types.BIT, phase + ": stored BOOLEAN must remain BIT, got " + type);
            check("java.lang.Boolean".equals(className), phase + ": stored BOOLEAN class changed to " + className);
        }
        return metadata;
    }

    private static void checkComputedRows(ResultSet rows, String phase, long expected) throws SQLException {
        metadata(rows, phase, true);
        int count = 0;
        while (rows.next()) {
            Object value = rows.getObject(1);
            check(value instanceof Number, phase + ": expected numeric value, got " + value);
            check(((Number) value).longValue() == expected,
                    phase + ": expected " + expected + ", got " + value);
            count++;
        }
        check(count == 3, phase + ": expected three rows, got " + count);
    }

    private static void statementCases(Connection connection) throws SQLException {
        String[] expressions = {
                "MAX(n IS NULL) OVER ()",
                "MIN(n IS NULL) OVER ()",
                "FIRST_VALUE(n IS NULL) OVER (ORDER BY id)"
        };
        long[] expected = {1, 0, 0};
        for (int i = 0; i < expressions.length; i++) {
            String sql = "SELECT " + expressions[i] + " AS p FROM " + TABLE;
            try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(sql)) {
                checkComputedRows(rows, "statement-" + i, expected[i]);
            }
        }

        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery(
                     "SELECT flag, MAX(n IS NULL) OVER () AS p FROM " + TABLE)) {
            ResultSetMetaData metadata = rows.getMetaData();
            check(metadata.getColumnCount() == 2, "sibling: expected two result columns");
            check(metadata.getColumnType(1) == Types.BIT, "sibling: stored BOOLEAN changed type");
            check("java.lang.Boolean".equals(metadata.getColumnClassName(1)),
                    "sibling: stored BOOLEAN changed class");
            check(metadata.getColumnType(2) != Types.BIT, "sibling: window result remained BIT");
            check(!"java.lang.Boolean".equals(metadata.getColumnClassName(2)),
                    "sibling: window result remained Boolean");
        }
    }

    private static void preparedCase(Properties properties, boolean serverPrepared) throws SQLException {
        properties.setProperty("useServerPrepStmts", Boolean.toString(serverPrepared));
        try (Connection connection = DriverManager.getConnection(
                System.getenv().getOrDefault("DB_URL", "jdbc:mysql://127.0.0.1:6001/"), properties)) {
            String sql = "SELECT MAX(n IS NULL) OVER () AS p FROM " + TABLE + " WHERE id >= ?";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setInt(1, 1);
                try (ResultSet rows = statement.executeQuery()) {
                    checkComputedRows(rows, serverPrepared ? "server-prepared" : "client-prepared", 1);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", System.getenv().getOrDefault("DB_USER", "root"));
        properties.setProperty("password", System.getenv().getOrDefault("DB_PASSWORD", "111"));
        properties.setProperty("tinyInt1isBit", "true");
        properties.setProperty("useSSL", "false");
        properties.setProperty("connectTimeout", "10000");
        properties.setProperty("socketTimeout", "30000");
        Class.forName("com.mysql.cj.jdbc.Driver");

        String url = System.getenv().getOrDefault("DB_URL", "jdbc:mysql://127.0.0.1:6001/");
        try (Connection connection = DriverManager.getConnection(url, properties)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS " + TABLE);
                statement.execute("CREATE TABLE " + TABLE
                        + " (id INT PRIMARY KEY, flag BOOLEAN, n INT)");
                statement.execute("INSERT INTO " + TABLE
                        + " VALUES (1, TRUE, 7), (2, FALSE, NULL), (3, NULL, 0)");
            }
            try {
                statementCases(connection);
                preparedCase(properties, false);
                preparedCase(properties, true);
            } finally {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("DROP TABLE IF EXISTS " + TABLE);
                }
            }
        }

        System.out.println("PASS window BOOLEAN metadata");
    }
}
