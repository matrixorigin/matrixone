import java.math.BigDecimal;
import java.sql.*;

/** Run against an isolated instance with Connector/J 8.4.0 on the classpath. */
public final class NullFirstPreparedProbe {
    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    private static void scalar(PreparedStatement statement, String expected) throws SQLException {
        try (ResultSet rows = statement.executeQuery()) {
            check(rows.next(), "missing scalar row");
            String value = rows.getString(1);
            boolean isNull = rows.wasNull();
            check(expected == null ? isNull && value == null : !isNull && expected.equals(value),
                    "expected " + expected + ", got " + value + ", NULL=" + isNull);
            check(!rows.next(), "extra scalar row");
        }
    }

    private static long count(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(sql)) {
            check(rows.next(), "missing count row");
            long value = rows.getLong(1);
            check(!rows.next(), "extra count row");
            return value;
        }
    }

    private static void serverPrepared(PreparedStatement statement) throws SQLException {
        check(statement.getClass().getName().equals("com.mysql.cj.jdbc.ServerPreparedStatement"),
                "probe requires an actual server-prepared statement: " + statement.getClass().getName());
        statement.setQueryTimeout(30);
    }

    private static void selectTransitions(Connection connection) throws SQLException {
        for (int nullType : new int[]{Types.NULL, Types.DECIMAL, Types.INTEGER}) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT ?")) {
                serverPrepared(statement);
                statement.setNull(1, nullType);
                scalar(statement, null);
                statement.setBigDecimal(1, new BigDecimal("1.500"));
                scalar(statement, "1.500");
                statement.setNull(1, nullType);
                scalar(statement, null);
                statement.setString(1, "text");
                scalar(statement, "text");
                statement.setInt(1, 42);
                scalar(statement, "42");
                statement.clearParameters();
                boolean rejected = false;
                try (ResultSet ignored = statement.executeQuery()) {
                    throw new AssertionError("clearParameters executed an unbound query, row=" + ignored.next());
                } catch (SQLException expected) {
                    // Connector/J must reject an unset parameter before executing the statement.
                    check("07001".equals(expected.getSQLState()), "unexpected unbound SQLState: " + expected.getSQLState());
                    rejected = true;
                }
                check(rejected, "missing-parameter rejection");
                statement.setNull(1, nullType);
                scalar(statement, null);
            }
        }
        try (PreparedStatement statement = connection.prepareStatement("SELECT CAST(? AS DECIMAL(12,3))")) {
            serverPrepared(statement);
            statement.setNull(1, Types.DECIMAL);
            scalar(statement, null);
            statement.setBigDecimal(1, new BigDecimal("1.500"));
            scalar(statement, "1.500");
            statement.setNull(1, Types.DECIMAL);
            scalar(statement, null);
        }
    }

    private static void insertRollback(Connection writer, Connection observer) throws SQLException {
        try (Statement ddl = writer.createStatement()) {
            ddl.execute("CREATE DATABASE qa_null_first_29964");
        }
        try {
            try (Statement ddl = writer.createStatement()) {
                ddl.execute("CREATE TABLE qa_null_first_29964.bindings "
                        + "(id BIGINT AUTO_INCREMENT PRIMARY KEY, a INT NULL, b DECIMAL(12,3) NULL, INDEX idx_a(a))");
            }
            writer.setAutoCommit(false);
            try {
                try (PreparedStatement statement = writer.prepareStatement(
                        "INSERT INTO qa_null_first_29964.bindings(a,b) VALUES(?,?)")) {
                    serverPrepared(statement);
                    statement.setNull(1, Types.INTEGER);
                    statement.setNull(2, Types.DECIMAL);
                    check(statement.executeUpdate() == 1, "first all-NULL insert");
                    statement.setInt(1, 7);
                    statement.setBigDecimal(2, new BigDecimal("1.500"));
                    check(statement.executeUpdate() == 1, "numeric insert");
                    statement.setNull(1, Types.INTEGER);
                    statement.setNull(2, Types.DECIMAL);
                    check(statement.executeUpdate() == 1, "reused all-NULL insert");
                    statement.setInt(1, 8);
                    statement.setNull(2, Types.DECIMAL);
                    check(statement.executeUpdate() == 1, "mixed insert");
                    check(count(writer, "SELECT COUNT(*) FROM qa_null_first_29964.bindings") == 4, "transaction row count");
                    check(count(writer, "SELECT COUNT(*) FROM qa_null_first_29964.bindings WHERE a IS NULL AND b IS NULL") == 2,
                            "NULL rows");
                    check(count(writer, "SELECT COUNT(*) FROM qa_null_first_29964.bindings WHERE a=7 AND b=1.500") == 1,
                            "indexed numeric predicate");
                    check(count(writer, "SELECT COUNT(*) FROM qa_null_first_29964.bindings WHERE a=8 AND b IS NULL") == 1,
                            "indexed mixed predicate");
                    check(count(observer, "SELECT COUNT(*) FROM qa_null_first_29964.bindings") == 0,
                            "uncommitted rows visible to observer");
                }
            } finally {
                writer.rollback();
                writer.setAutoCommit(true);
            }
            check(count(observer, "SELECT COUNT(*) FROM qa_null_first_29964.bindings") == 0, "rollback row count");
        } finally {
            try (Statement ddl = writer.createStatement()) {
                ddl.execute("DROP DATABASE qa_null_first_29964");
            }
        }
        check(count(observer, "SELECT COUNT(*) FROM mo_catalog.mo_database WHERE datname='qa_null_first_29964'") == 0,
                "database teardown");
    }

    public static void main(String[] args) throws Exception {
        String baseUrl = System.getenv("DB_URL");
        check(baseUrl != null && !baseUrl.isEmpty(), "DB_URL is required");
        String url = baseUrl + (baseUrl.contains("?") ? "&" : "?")
                + "useServerPrepStmts=true&emulateUnsupportedPstmts=false&cachePrepStmts=false"
                + "&connectTimeout=10000&socketTimeout=30000";
        try (Connection connection = DriverManager.getConnection(url, System.getenv("DB_USER"), System.getenv("DB_PASSWORD"));
             Connection observer = DriverManager.getConnection(url, System.getenv("DB_USER"), System.getenv("DB_PASSWORD"))) {
            // Two bounded runs prove that teardown leaves the same instance reusable.
            for (int run = 1; run <= 2; run++) {
                selectTransitions(connection);
                insertRollback(connection, observer);
                System.out.println("PASS run=" + run + " NULL/value/type-change/clearParameters/insert/rollback/teardown");
            }
        }
    }
}
