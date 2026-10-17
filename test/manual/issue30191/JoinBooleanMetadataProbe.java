import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/** Real-wire regression for BOOLEAN provenance through JOIN projection slots. */
public final class JoinBooleanMetadataProbe {
    private static void check(boolean value, String message) {
        if (!value) {
            throw new AssertionError(message);
        }
    }

    private static void metadata(ResultSetMetaData metadata, boolean nativeFirst, String phase)
            throws Exception {
        check(metadata != null, phase + ": missing metadata");
        check(metadata.getColumnCount() == 2, phase + ": expected two columns");
        for (int i = 1; i <= 2; i++) {
            boolean nativeColumn = nativeFirst == (i == 1);
            Class<?> type = Class.forName(metadata.getColumnClassName(i));
            check(metadata.getColumnType(i) == (nativeColumn ? Types.BIT : Types.TINYINT),
                    phase + ": wrong SQL type at column " + i + ": " + metadata.getColumnTypeName(i));
            check(nativeColumn ? type == Boolean.class : Number.class.isAssignableFrom(type),
                    phase + ": wrong Java class at column " + i + ": " + type.getName());
        }
    }

    private static void rows(ResultSet rows, boolean nativeFirst, Map<String, Integer> expected)
            throws Exception {
        metadata(rows.getMetaData(), nativeFirst, "execute");
        Map<String, Integer> actual = new HashMap<>();
        while (rows.next()) {
            Object nativeValue = rows.getObject(nativeFirst ? 1 : 2);
            Object computedValue = rows.getObject(nativeFirst ? 2 : 1);
            check(nativeValue == null || nativeValue instanceof Boolean,
                    "stored BOOLEAN became " + nativeValue);
            check(computedValue == null || computedValue instanceof Number,
                    "computed BOOLEAN became " + computedValue);
            String key = String.valueOf(nativeValue) + ":"
                    + (computedValue == null ? "null" : ((Number) computedValue).longValue());
            actual.merge(key, 1, Integer::sum);
        }
        check(actual.equals(expected), "row multiset mismatch: expected " + expected + ", got " + actual);
    }

    private static String derived(String table, String side, String nativeSide, boolean grouped) {
        String expr = side.equals(nativeSide) ? "flag" : "n IS NULL";
        return "(SELECT " + (grouped ? "" : "DISTINCT ") + expr + " AS v FROM " + table
                + (grouped ? " GROUP BY " + expr : "") + ") " + side;
    }

    private static Map<String, Integer> expectedRows(String join, String nativeSide) {
        Map<String, Integer> expected = new HashMap<>();
        if (join.equals("CROSS JOIN")) {
            for (String nativeValue : new String[]{"true", "false", "null"}) {
                for (int computedValue = 0; computedValue <= 1; computedValue++) {
                    expected.put(nativeValue + ":" + computedValue, 1);
                }
            }
        } else {
            expected.put("true:1", 1);
            expected.put("false:0", 1);
            if ((join.equals("LEFT JOIN") && nativeSide.equals("a"))
                    || (join.equals("RIGHT JOIN") && nativeSide.equals("b"))) {
                expected.put("null:null", 1);
            }
        }
        return expected;
    }

    private static int cases(Connection connection, String table, String mode) throws Exception {
        int count = 0;
        for (boolean grouped : new boolean[]{false, true}) {
            for (String nativeSide : new String[]{"a", "b"}) {
                for (String join : new String[]{"CROSS JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN"}) {
                    for (boolean aFirst : new boolean[]{false, true}) {
                        for (boolean outerDistinct : new boolean[]{false, true}) {
                            String sql = "SELECT " + (outerDistinct ? "DISTINCT " : "")
                                    + (aFirst ? "a.v, b.v" : "b.v, a.v") + " FROM "
                                    + derived(table, "a", nativeSide, grouped) + " " + join + " "
                                    + derived(table, "b", nativeSide, grouped)
                                    + (join.equals("CROSS JOIN") ? "" : " ON a.v=b.v");
                            boolean nativeFirst = aFirst == nativeSide.equals("a");
                            try {
                                if (mode.equals("statement")) {
                                    try (Statement statement = connection.createStatement()) {
                                        statement.setQueryTimeout(30);
                                        try (ResultSet result = statement.executeQuery(sql)) {
                                            rows(result, nativeFirst, expectedRows(join, nativeSide));
                                        }
                                    }
                                } else {
                                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                                        statement.setQueryTimeout(30);
                                        if (mode.equals("server-prepared")) {
                                            check(statement.getClass().getName().equals(
                                                    "com.mysql.cj.jdbc.ServerPreparedStatement"),
                                                    "server prepare fell back: " + statement.getClass().getName());
                                        }
                                        metadata(statement.getMetaData(), nativeFirst, "prepare");
                                        try (ResultSet result = statement.executeQuery()) {
                                            rows(result, nativeFirst, expectedRows(join, nativeSide));
                                        }
                                    }
                                }
                            } catch (Exception | AssertionError error) {
                                throw new AssertionError(mode + ": " + sql, error);
                            }
                            count++;
                        }
                    }
                }
            }
        }
        System.out.println("PASS " + mode + ": " + count + " JOIN cases");
        return count;
    }

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", System.getenv().getOrDefault("DB_USER", "root"));
        properties.setProperty("password", System.getenv().getOrDefault("DB_PASSWORD", "111"));
        properties.setProperty("tinyInt1isBit", "true");
        properties.setProperty("useSSL", "false");
        properties.setProperty("connectTimeout", "10000");
        properties.setProperty("socketTimeout", "30000");
        properties.setProperty("emulateUnsupportedPstmts", "false");
        String url = System.getenv().getOrDefault("DB_URL", "jdbc:mysql://127.0.0.1:6001/");
        String database = "issue30191_join_" + UUID.randomUUID().toString().replace("-", "");
        String table = database + ".bools";
        Class.forName("com.mysql.cj.jdbc.Driver");
        try (Connection setup = DriverManager.getConnection(url, properties);
             Statement statement = setup.createStatement()) {
            statement.setQueryTimeout(30);
            statement.execute("CREATE DATABASE " + database);
            try {
                statement.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, flag BOOLEAN, n INT)");
                statement.execute("INSERT INTO " + table + " VALUES (1,TRUE,7),(2,FALSE,NULL),(3,NULL,0)");
                int total = 0;
                for (String mode : new String[]{"statement", "client-prepared", "server-prepared"}) {
                    properties.setProperty("useServerPrepStmts", Boolean.toString(mode.equals("server-prepared")));
                    try (Connection connection = DriverManager.getConnection(url, properties)) {
                        total += cases(connection, table, mode);
                    }
                }
                System.out.println("PASS " + total + " JOIN BOOLEAN metadata/value cases; driver "
                        + setup.getMetaData().getDriverVersion());
            } finally {
                statement.execute("DROP DATABASE " + database);
            }
        }
    }
}
