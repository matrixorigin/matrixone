import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;

/** Real-wire TIME_WINDOW provenance, including actual gap/FILL rows. */
public final class TimeWindowBooleanMetadataProbe {
    private static final int[] TYPES = {Types.BIT, Types.TINYINT, Types.TIMESTAMP, Types.TIMESTAMP};

    private static void check(boolean ok, String message) {
        if (!ok) throw new AssertionError(message);
    }

    private static void metadata(ResultSetMetaData md, int[] projection, String phase) throws Exception {
        check(md != null && md.getColumnCount() == projection.length, phase + ": missing/wrong metadata");
        for (int i = 0; i < projection.length; i++) {
            int type = TYPES[projection[i]];
            check(md.getColumnType(i+1) == type, phase + ": column " + (i+1)
                    + " expected JDBC " + type + ", got " + md.getColumnType(i+1));
            Class<?> cls = Class.forName(md.getColumnClassName(i+1));
            check(type == Types.BIT ? cls == Boolean.class
                    : type == Types.TINYINT ? Number.class.isAssignableFrom(cls) : cls == Timestamp.class,
                    phase + ": wrong class " + cls.getName());
        }
    }

    private static String boundary(int minute) {
        return "2026-01-01 00:0" + minute + ":00.0";
    }

    private static void add(Map<String,Integer> rows, int[] projection, String flag, String p, int minute) {
        String[] values = {flag, p, boundary(minute), boundary(minute+1)};
        StringBuilder row = new StringBuilder();
        for (int slot : projection) {
            if (!row.isEmpty()) row.append('|');
            row.append(values[slot]);
        }
        rows.merge(row.toString(), 1, Integer::sum);
    }

    private static Map<String,Integer> expected(int[] projection, String fill) {
        Map<String,Integer> rows = new HashMap<>();
        add(rows, projection, "true", "1", 0);
        add(rows, projection, "true", "0", 2);
        add(rows, projection, "false", fill.equals("value") || fill.equals("next") ? "0" : "null", 0);
        add(rows, projection, "false", "0", 2);
        add(rows, projection, "null", fill.equals("value") ? "0" : fill.equals("next") ? "1" : "null", 0);
        add(rows, projection, "null", "1", 2);
        if (!fill.equals("none")) {
            add(rows, projection, "true", fill.equals("prev") ? "1" : fill.equals("null") ? "null" : "0", 1);
            add(rows, projection, "false", fill.equals("value") || fill.equals("next") ? "0" : "null", 1);
            add(rows, projection, "null", fill.equals("value") ? "0" : fill.equals("next") ? "1" : "null", 1);
        }
        return rows;
    }

    private static void query(Connection connection, String mode, String sql, int[] projection,
                              Map<String,Integer> expected) throws Exception {
        try (Statement statement = mode.equals("statement") ? connection.createStatement()
                : connection.prepareStatement(sql)) {
            statement.setQueryTimeout(30);
            if (statement instanceof PreparedStatement) {
                check(!mode.equals("server-prepared") || statement.getClass().getName().equals(
                        "com.mysql.cj.jdbc.ServerPreparedStatement"), "server prepare fell back");
                metadata(((PreparedStatement) statement).getMetaData(), projection, "prepare");
            }
            try (ResultSet result = statement instanceof PreparedStatement
                    ? ((PreparedStatement) statement).executeQuery() : statement.executeQuery(sql)) {
                metadata(result.getMetaData(), projection, "execute");
                Map<String,Integer> actual = new HashMap<>();
                while (result.next()) {
                    StringBuilder row = new StringBuilder();
                    for (int i = 0; i < projection.length; i++) {
                        Object value = result.getObject(i+1);
                        int type = TYPES[projection[i]];
                        check(value == null || (type == Types.BIT ? value instanceof Boolean
                                : type == Types.TINYINT ? value instanceof Number : value instanceof Timestamp),
                                "wrong returned Java class at column " + (i+1));
                        if (i > 0) row.append('|');
                        row.append(value instanceof Number ? ((Number) value).longValue() : value);
                    }
                    actual.merge(row.toString(), 1, Integer::sum);
                }
                check(actual.equals(expected), "expected " + expected + ", got " + actual);
            }
        } catch (Exception | AssertionError error) {
            throw new AssertionError(mode + ": " + sql, error);
        }
    }

    private static int cases(Connection connection, String table, String mode) throws Exception {
        String[] selects = {"flag,MAX(n>0)", "MAX(n>0),flag",
                "MAX(n>0),flag,MAX(n>0),flag", "_wend,flag,MAX(n>0),_wstart,_wend",
                "MAX(n>0)", "flag,_wstart"};
        int[][] projections = {{0,1}, {1,0}, {1,0,1,0}, {3,0,1,2,3}, {1}, {0,2}};
        int count = 0;
        for (String fill : new String[]{"none", "null", "value", "prev", "next"}) {
            String suffix = " GROUP BY flag INTERVAL(ts,1,MINUTE)";
            if (!fill.equals("none")) {
                suffix += " GAPFILL(PARTITION) FILL(" + (fill.equals("value") ? "value,0" : fill) + ")";
            }
            for (int c = 0; c < selects.length; c++) {
                query(connection, mode, "SELECT " + selects[c] + " FROM " + table + suffix,
                        projections[c], expected(projections[c], fill));
                count++;
            }
        }
        Map<String,Integer> control = new HashMap<>();
        control.put("true|1", 1);
        control.put("false|0", 1);
        control.put("null|1", 1);
        query(connection, mode, "SELECT flag,MAX(n>0) FROM " + table + " GROUP BY flag", new int[]{0,1}, control);
        count++;
        // Prune the unused aggregate/boundaries and reorder through an outer projection.
        int[] repeated = {1,0,1,0};
        query(connection, mode, "SELECT p,b,p,b FROM (SELECT _wstart s,flag b,MAX(n>0) p,MIN(n IS NULL) q FROM "
                + table + " GROUP BY flag INTERVAL(ts,1,MINUTE)) d", repeated, expected(repeated, "none"));
        count++;
        System.out.println("PASS " + mode + ": " + count + " TIME_WINDOW/control cases");
        return count;
    }

    public static void main(String[] args) throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Properties properties = new Properties();
        properties.setProperty("user", System.getenv().getOrDefault("DB_USER", "root"));
        properties.setProperty("password", System.getenv().getOrDefault("DB_PASSWORD", "111"));
        properties.setProperty("tinyInt1isBit", "true");
        properties.setProperty("useSSL", "false");
        properties.setProperty("connectTimeout", "10000");
        properties.setProperty("socketTimeout", "30000");
        properties.setProperty("emulateUnsupportedPstmts", "false");
        properties.setProperty("connectionTimeZone", "UTC");
        String url = System.getenv().getOrDefault("DB_URL", "jdbc:mysql://127.0.0.1:6001/");
        String database = "issue30191_timewindow_" + UUID.randomUUID().toString().replace("-", "");
        String table = database + ".bools";
        Class.forName("com.mysql.cj.jdbc.Driver");
        try (Connection setup = DriverManager.getConnection(url, properties);
             Statement statement = setup.createStatement()) {
            statement.setQueryTimeout(30);
            statement.execute("CREATE DATABASE " + database);
            try {
                statement.execute("SET time_zone='+00:00'");
                statement.execute("CREATE TABLE " + table + " (ts TIMESTAMP, flag BOOLEAN, n INT)");
                statement.execute("INSERT INTO " + table + " VALUES "
                        + "('2026-01-01 00:00:10',TRUE,7),('2026-01-01 00:02:10',TRUE,0),"
                        + "('2026-01-01 00:00:10',FALSE,NULL),('2026-01-01 00:02:10',FALSE,-1),"
                        + "('2026-01-01 00:00:10',NULL,NULL),('2026-01-01 00:02:10',NULL,1)");
                int count = 0;
                for (String mode : new String[]{"statement", "client-prepared", "server-prepared"}) {
                    properties.setProperty("useServerPrepStmts", Boolean.toString(mode.equals("server-prepared")));
                    try (Connection connection = DriverManager.getConnection(url, properties);
                         Statement session = connection.createStatement()) {
                        session.setQueryTimeout(30);
                        session.execute("SET time_zone='+00:00'");
                        count += cases(connection, table, mode);
                    }
                }
                System.out.println("PASS " + count + " TIME_WINDOW metadata/value cases; driver "
                        + setup.getMetaData().getDriverVersion());
            } finally {
                statement.execute("DROP DATABASE " + database);
            }
        }
    }
}
