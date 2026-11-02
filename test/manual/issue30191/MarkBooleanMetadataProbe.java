import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/** Real-wire regression for the computed BOOLEAN owned by a MARK JOIN. */
public final class MarkBooleanMetadataProbe {
    private static final class Probe {
        final String sql;
        final boolean[] nativeColumns;
        final Object[][] expected;

        Probe(String sql, boolean[] nativeColumns, Object[][] expected) {
            this.sql = sql;
            this.nativeColumns = nativeColumns;
            this.expected = expected;
        }
    }

    private static void check(boolean value, String message) {
        if (!value) {
            throw new AssertionError(message);
        }
    }

    private static void metadata(ResultSetMetaData metadata, Probe probe, String phase) throws Exception {
        check(metadata != null, phase + ": missing metadata");
        check(metadata.getColumnCount() == probe.nativeColumns.length, phase + ": wrong column count");
        for (int i = 1; i <= probe.nativeColumns.length; i++) {
            boolean nativeColumn = probe.nativeColumns[i - 1];
            Class<?> type = Class.forName(metadata.getColumnClassName(i));
            check(metadata.getColumnType(i) == (nativeColumn ? Types.BIT : Types.TINYINT),
                    phase + ": column " + i + " has SQL type " + metadata.getColumnTypeName(i));
            check(nativeColumn ? type == Boolean.class : Number.class.isAssignableFrom(type),
                    phase + ": column " + i + " has Java class " + type.getName());
        }
    }

    private static Map<String, Integer> multiset(Object[][] rows) {
        Map<String, Integer> values = new HashMap<>();
        for (Object[] row : rows) {
            values.merge(Arrays.toString(row), 1, Integer::sum);
        }
        return values;
    }

    private static void rows(ResultSet rows, Probe probe) throws Exception {
        metadata(rows.getMetaData(), probe, "execute");
        Map<String, Integer> actual = new HashMap<>();
        while (rows.next()) {
            Object[] row = new Object[probe.nativeColumns.length];
            for (int i = 0; i < row.length; i++) {
                Object value = rows.getObject(i + 1);
                check(value == null || (probe.nativeColumns[i] ? value instanceof Boolean : value instanceof Number),
                        "wrong value class at column " + (i + 1) + ": " + value);
                row[i] = value instanceof Number ? ((Number) value).longValue() : value;
            }
            actual.merge(Arrays.toString(row), 1, Integer::sum);
        }
        check(actual.equals(multiset(probe.expected)), "row mismatch: " + actual
                + ", expected " + multiset(probe.expected));
    }

    private static Probe[] probes(String table, String right, String state) {
        String in = "n IN (SELECT x FROM " + right + ")";
        String any = "n = ANY (SELECT x FROM " + right + ")";
        String from = " FROM " + table;
        boolean empty = state.equals("empty");
        Long[] p = empty ? new Long[]{0L, 0L, 0L}
                : new Long[]{1L, null, state.equals("nullable") ? null : 0L};
        Long[] negated = empty ? new Long[]{1L, 1L, 1L}
                : new Long[]{0L, null, state.equals("nullable") ? null : 1L};
        Object[][] values = {{p[0]}, {p[1]}, {p[2]}};
        Object[][] siblings = {{true, p[0]}, {false, p[1]}, {null, p[2]}};
        return new Probe[]{
                new Probe("SELECT " + in + from, new boolean[]{false}, values),
                new Probe("SELECT " + any + from, new boolean[]{false}, values),
                new Probe("SELECT DISTINCT " + in + from, new boolean[]{false},
                        empty ? new Object[][]{{0L}} : state.equals("nullable")
                                ? new Object[][]{{1L}, {null}} : values),
                new Probe("SELECT flag, " + in + from, new boolean[]{true, false}, siblings),
                new Probe("SELECT " + in + ", flag" + from, new boolean[]{false, true},
                        new Object[][]{{p[0], true}, {p[1], false}, {p[2], null}}),
                new Probe("SELECT DISTINCT flag, " + in + from, new boolean[]{true, false}, siblings),
                new Probe("SELECT p FROM (SELECT " + in + " AS p" + from + ") d", new boolean[]{false}, values),
                new Probe("WITH d AS (SELECT flag, " + in + " AS p" + from + ") SELECT flag,p FROM d",
                        new boolean[]{true, false}, siblings),
                new Probe("SELECT " + in + ", flag, " + any + from, new boolean[]{false, true, false},
                        new Object[][]{{p[0], true, p[0]}, {p[1], false, p[1]}, {p[2], null, p[2]}}),
                new Probe("SELECT FIRST_VALUE(flag) OVER(ORDER BY id), FIRST_VALUE(p) OVER(ORDER BY id) "
                        + "FROM (SELECT id,flag," + in + " AS p" + from + ") d", new boolean[]{true, false},
                        new Object[][]{{true, p[0]}, {true, p[0]}, {true, p[0]}}),
                new Probe("SELECT a.flag,b.p FROM " + table + " a JOIN (SELECT id," + in + " AS p"
                        + from + ") b ON a.id=b.id", new boolean[]{true, false}, siblings),
                new Probe("SELECT n NOT IN (SELECT x FROM " + right + ")" + from, new boolean[]{false},
                        new Object[][]{{negated[0]}, {negated[1]}, {negated[2]}}),
                new Probe("SELECT n IN (7,9)" + from, new boolean[]{false}, new Object[][]{{1L}, {null}, {0L}}),
                new Probe("SELECT EXISTS(SELECT x FROM " + right + " WHERE x=n)" + from,
                        new boolean[]{false}, new Object[][]{{empty ? 0L : 1L}, {0L}, {0L}}),
                new Probe("SELECT flag" + from, new boolean[]{true}, new Object[][]{{true}, {false}, {null}}),
                new Probe("SELECT n IS NULL" + from, new boolean[]{false}, new Object[][]{{0L}, {1L}, {0L}})
        };
    }

    private static int run(Connection connection, Probe[] probes, String mode, String state) throws Exception {
        for (Probe probe : probes) {
            try {
                if (mode.equals("statement")) {
                    try (Statement statement = connection.createStatement()) {
                        statement.setQueryTimeout(30);
                        try (ResultSet result = statement.executeQuery(probe.sql)) {
                            rows(result, probe);
                        }
                    }
                } else {
                    try (PreparedStatement statement = connection.prepareStatement(probe.sql)) {
                        statement.setQueryTimeout(30);
                        if (mode.equals("server-prepared")) {
                            check(statement.getClass().getName().equals("com.mysql.cj.jdbc.ServerPreparedStatement"),
                                    "server prepare fell back to " + statement.getClass().getName());
                        }
                        metadata(statement.getMetaData(), probe, "prepare");
                        try (ResultSet result = statement.executeQuery()) {
                            rows(result, probe);
                        }
                    }
                }
            } catch (Exception | AssertionError error) {
                throw new AssertionError(state + "/" + mode + ": " + probe.sql, error);
            }
        }
        System.out.println("PASS " + state + "/" + mode + ": " + probes.length + " MARK cases");
        return probes.length;
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
        String database = "issue30191_mark_" + UUID.randomUUID().toString().replace("-", "");
        String table = database + ".bools";
        String right = database + ".r";
        Class.forName("com.mysql.cj.jdbc.Driver");
        try (Connection setup = DriverManager.getConnection(url, properties);
             Statement statement = setup.createStatement()) {
            statement.setQueryTimeout(30);
            statement.execute("CREATE DATABASE " + database);
            try {
                statement.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, flag BOOLEAN, n INT)");
                statement.execute("INSERT INTO " + table + " VALUES (1,TRUE,7),(2,FALSE,NULL),(3,NULL,0)");
                statement.execute("CREATE TABLE " + right + " (x INT)");
                statement.execute("INSERT INTO " + right + " VALUES (7),(9)");
                int count = 0;
                for (String state : new String[]{"nonnull", "nullable", "empty"}) {
                    if (state.equals("nullable")) {
                        statement.execute("INSERT INTO " + right + " VALUES(NULL)");
                    } else if (state.equals("empty")) {
                        statement.execute("TRUNCATE TABLE " + right);
                    }
                    for (String mode : new String[]{"statement", "client-prepared", "server-prepared"}) {
                        properties.setProperty("useServerPrepStmts", Boolean.toString(mode.equals("server-prepared")));
                        try (Connection connection = DriverManager.getConnection(url, properties)) {
                            count += run(connection, probes(table, right, state), mode, state);
                        }
                    }
                }
                System.out.println("PASS " + count + " MARK BOOLEAN metadata/value cases; driver "
                        + setup.getMetaData().getDriverVersion());
            } finally {
                statement.execute("DROP DATABASE " + database);
            }
        }
    }
}
