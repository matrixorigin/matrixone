import java.sql.*;
import java.util.Properties;

/** Connector/J 8.4.0 public type-contract regression for WEEK and DAYOFYEAR. */
public final class CalendarMetadataProbe {
    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    private static void metadata(ResultSetMetaData metadata) throws SQLException {
        require(metadata != null && metadata.getColumnCount() == 3, "three result columns");
        for (int i = 1; i <= 3; i++) {
            require(metadata.getColumnType(i) == Types.BIGINT, "column " + i + " JDBC BIGINT");
            require("BIGINT".equals(metadata.getColumnTypeName(i)), "column " + i + " signed BIGINT name");
            require("java.lang.Long".equals(metadata.getColumnClassName(i)), "column " + i + " Long class");
            require(metadata.isSigned(i), "column " + i + " signed flag");
        }
    }

    private static void result(ResultSet rows, boolean nullInput) throws SQLException {
        try (ResultSet owned = rows) {
            metadata(owned.getMetaData());
            require(owned.next(), "one result row");
            long[] expected = {1, 366, 1};
            for (int i = 1; i <= 3; i++) {
                Object value = owned.getObject(i);
                if (nullInput) {
                    require(value == null, "NULL input column " + i);
                } else {
                    require(value instanceof Long, "getObject returns Long for column " + i);
                    require(((Long) value).longValue() == expected[i - 1], "value column " + i);
                }
            }
            require(!owned.next(), "no extra result row");
        }
    }

    public static void main(String[] args) throws Exception {
        String url = System.getenv().getOrDefault("DB_URL", "jdbc:mysql://127.0.0.1:29999/");
        for (boolean server : new boolean[]{false, true}) {
            Properties properties = new Properties();
            properties.setProperty("user", System.getenv().getOrDefault("DB_USER", "dump"));
            properties.setProperty("password", System.getenv().getOrDefault("DB_PASSWORD", "111"));
            properties.setProperty("useServerPrepStmts", Boolean.toString(server));
            properties.setProperty("emulateUnsupportedPstmts", "false");
            properties.setProperty("cachePrepStmts", "false");
            properties.setProperty("connectTimeout", "5000");
            properties.setProperty("socketTimeout", "30000");
            properties.setProperty("serverTimezone", "Asia/Shanghai");
            try (Connection connection = DriverManager.getConnection(url, properties)) {
                try (Statement statement = connection.createStatement()) {
                    statement.setQueryTimeout(30);
                    result(statement.executeQuery("select week('2024-12-31',3),dayofyear('2024-12-31'),weekofyear('2024-12-31')"), false);
                }
                try (PreparedStatement statement = connection.prepareStatement("select week(?,3),dayofyear(?),weekofyear(?)")) {
                    statement.setQueryTimeout(30);
                    boolean actualServer = statement.getClass().getName().contains("ServerPreparedStatement");
                    require(actualServer == server, "requested prepared protocol must be used");
                    metadata(statement.getMetaData());
                    for (String value : new String[]{"2024-12-31", null, "2024-12-31"}) {
                        for (int i = 1; i <= 3; i++) {
                            if (value == null) statement.setNull(i, Types.DATE);
                            else statement.setString(i, value);
                        }
                        result(statement.executeQuery(), value == null);
                        metadata(statement.getMetaData());
                    }
                }
                try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery("select 1")) {
                    require(rows.next() && rows.getInt(1) == 1, "connection remains usable");
                }
            }
            System.out.println("PASS serverPrepared=" + server);
        }
    }
}
