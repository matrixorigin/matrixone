import java.math.BigDecimal;
import java.sql.*;
import java.util.Properties;

public final class PrepareCoalesceMetadataProbe {
    private static void check(boolean value, String message) {
        if (!value) throw new AssertionError(message);
    }

    private static void decimalMetadata(ResultSetMetaData metadata, String phase) throws SQLException {
        check(metadata != null && metadata.getColumnCount() == 1, phase + ": missing scalar metadata");
        int type = metadata.getColumnType(1);
        System.out.println(phase + "=" + metadata.getColumnTypeName(1) + "(" + metadata.getPrecision(1)
                + "," + metadata.getScale(1) + "):" + metadata.getColumnClassName(1));
        check(type == Types.DECIMAL || type == Types.NUMERIC, phase + ": expected DECIMAL, got " + type);
        check("java.math.BigDecimal".equals(metadata.getColumnClassName(1)), phase + ": result class");
    }

    private static void numericResult(PreparedStatement statement, String expected) throws SQLException {
        try (ResultSet rows = statement.executeQuery()) {
            decimalMetadata(rows.getMetaData(), "post");
            check(rows.next(), "missing scalar row");
            BigDecimal actual = rows.getBigDecimal(1);
            check(expected == null ? actual == null : actual != null && actual.compareTo(new BigDecimal(expected)) == 0,
                    "expected " + expected + ", got " + actual);
            check(!rows.next(), "extra scalar row");
        }
    }

    private static void numericCase(Connection connection, String expression, boolean reverse) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT " + expression + " AS x")) {
            check(statement.getClass().getName().equals("com.mysql.cj.jdbc.ServerPreparedStatement"),
                    "requires ServerPreparedStatement: " + statement.getClass().getName());
            statement.setQueryTimeout(30);
            System.out.println("query=" + expression);
            decimalMetadata(statement.getMetaData(), "pre");
            statement.setBigDecimal(1, new BigDecimal("1.25"));
            decimalMetadata(statement.getMetaData(), "bound-before-execute");
            numericResult(statement, reverse ? "0" : "1.25");
            statement.setNull(1, Types.DECIMAL);
            numericResult(statement, expression.startsWith("COALESCE") ? "0" : null);
            statement.setString(1, "2.5");
            numericResult(statement, reverse ? "0" : "2.5");
            statement.setBigDecimal(1, new BigDecimal("-3.125"));
            numericResult(statement, reverse ? "0" : "-3.125");
        }
    }

    private static void stringTransition(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT COALESCE(?,CAST(0 AS DECIMAL(20,6))) AS x")) {
            statement.setQueryTimeout(30);
            decimalMetadata(statement.getMetaData(), "string-case-pre");
            statement.setString(1, "text");
            try (ResultSet rows = statement.executeQuery()) {
                check(rows.next(), "missing string row");
                System.out.println("nonnumeric-string-value=" + rows.getString(1));
                check(rows.getBigDecimal(1).compareTo(BigDecimal.ZERO) == 0, "nonnumeric string numeric-prefix conversion");
                decimalMetadata(rows.getMetaData(), "nonnumeric-string-post");
                check(!rows.next(), "extra string row");
            }
            statement.setBigDecimal(1, new BigDecimal("1.25"));
            numericResult(statement, "1.25");
        }
    }

    private static void controls(Connection connection) throws SQLException {
        for (String sql : new String[]{"SELECT COALESCE(?, 'text')", "SELECT COALESCE(?, ?)"}) {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setQueryTimeout(30);
                ResultSetMetaData metadata = statement.getMetaData();
                check("java.lang.String".equals(metadata.getColumnClassName(1)), "string/unknown PREPARE control");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", System.getenv().getOrDefault("DB_USER", "root"));
        properties.setProperty("password", System.getenv().getOrDefault("DB_PASSWORD", "111"));
        properties.setProperty("useServerPrepStmts", "true");
        properties.setProperty("emulateUnsupportedPstmts", "false");
        properties.setProperty("cachePrepStmts", "false");
        properties.setProperty("useSSL", "false");
        properties.setProperty("connectTimeout", "10000");
        properties.setProperty("socketTimeout", "30000");
        String url = System.getenv().getOrDefault("DB_URL", "jdbc:mysql://127.0.0.1:29977/");
        try (Connection connection = DriverManager.getConnection(url, properties)) {
            for (int run = 0; run < 2; run++) {
                numericCase(connection, "COALESCE(?,CAST(0 AS DECIMAL(20,6)))", false);
                numericCase(connection, "COALESCE(CAST(0 AS DECIMAL(20,6)),?)", true);
                numericCase(connection, "CAST(? AS DECIMAL(20,6))", false);
                stringTransition(connection);
                controls(connection);
                try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery("SELECT 1")) {
                    check(rows.next() && rows.getInt(1) == 1 && !rows.next(), "connection health");
                }
                System.out.println("PASS run=" + run);
            }
        }
    }
}
