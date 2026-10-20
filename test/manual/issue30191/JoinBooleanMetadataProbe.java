import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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
        metadata(metadata, new boolean[]{nativeFirst, !nativeFirst}, phase);
    }

    private static void metadata(ResultSetMetaData metadata, boolean[] nativeColumns, String phase)
            throws Exception {
        int[] types = new int[nativeColumns.length];
        for (int i = 0; i < types.length; i++) {
            types[i] = nativeColumns[i] ? Types.BIT : Types.TINYINT;
        }
        metadata(metadata, types, phase);
    }

    private static void metadata(ResultSetMetaData metadata, int[] types, String phase)
            throws Exception {
        check(metadata != null, phase + ": missing metadata");
        check(metadata.getColumnCount() == types.length, phase + ": wrong column count");
        for (int i = 1; i <= types.length; i++) {
            boolean nativeColumn = types[i - 1] == Types.BIT;
            Class<?> type = Class.forName(metadata.getColumnClassName(i));
            check(metadata.getColumnType(i) == types[i - 1],
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

    // Compositions that used to confuse source identities and output slots.
    private static int composedCases(Connection connection, String table, String mode) throws Exception {
        String[] queries = {
            "SELECT MIN(n>0),flag FROM " + table + " GROUP BY flag",
            "SELECT DISTINCT p,b,p,b FROM (SELECT flag b,n>0 p FROM " + table + ") c",
            "SELECT flag FROM " + table + " MINUS SELECT n>0 FROM " + table,
            "SELECT flag FROM " + table + " INTERSECT SELECT n>0 FROM " + table,
            "WITH RECURSIVE c(n,b,p) AS (SELECT 1,flag,n>0 FROM " + table
                + " WHERE id=1 UNION ALL SELECT n+1,b,n<2 FROM c WHERE n<3) SELECT p,b,p,b FROM c",
            "SELECT LAG(flag,1,n IS NULL) OVER(ORDER BY id) FROM " + table,
            "SELECT LEAD(flag,1,flag) OVER(ORDER BY id) FROM " + table,
            "SELECT LAG(flag,1,NULL) OVER(ORDER BY id) FROM " + table,
            "SELECT SAMPLE(n>0,1 ROWS),flag FROM " + table + " GROUP BY flag",
            "SELECT SAMPLE(flag,1 ROWS) FROM " + table + " WHERE id=1",
            "VALUES ROW(1=1),ROW(1=0)",
            "SELECT p FROM (VALUES ROW(1=1),ROW(1=0)) d(p)",
            "SELECT DISTINCT p FROM (VALUES ROW(1=1),ROW(1=0)) d(p)",
            "SELECT p FROM (VALUES ROW(NULL),ROW(1=0)) d(p)",
            "SELECT q,p,q FROM (VALUES ROW(1=1,1=0),ROW(1=0,1=1)) d(p,q)",
            "SELECT p FROM (VALUES ROW(1=1),ROW(1=0)) d(p) UNION ALL SELECT flag FROM " + table,
            "SELECT FIRST_VALUE(p) OVER() FROM (VALUES ROW(1=1),ROW(1=0)) d(p)",
            "SELECT p FROM (SELECT SAMPLE(n,1 ROWS) s,n>0 p FROM " + table + " GROUP BY flag,n>0) d",
            "SELECT flag FROM (SELECT SAMPLE(n,1 ROWS) s,flag FROM " + table + " GROUP BY n>0,flag) d",
            "SELECT p FROM (SELECT SAMPLE(n,1 ROWS) s,n>0 p FROM " + table + " GROUP BY flag,n+1,n>0) d",
            "SELECT SAMPLE(n IS NULL,1 ROWS),flag FROM " + table + " GROUP BY n>0,flag"
        };
        boolean[][] nativeColumns = {{false,true},{false,true,false,true},{true},{true},{false,true,false,true},
            {false},{true},{true},{false,true},{true},
            {false},{false},{false},{false},{false,false,false},{false},{false},
            {false},{true},{false},{false,true}};
        String[][] expected = {
            {"1:true", "null:false", "0:null"},
            {"1:true:1:true", "null:false:null:false", "0:null:0:null"},
            {}, {"true", "false", "null"},
            {"1:true:1:true", "1:true:1:true", "0:true:0:true"},
            {"0","1","0"}, {"false","null","null"}, {"null","true","false"},
            {"1:true","null:false","0:null"}, {"true"},
            {"1","0"}, {"1","0"}, {"1","0"}, {"null","0"}, {"0:1:0","1:0:1"},
            {"1","0","1","0","null"}, {"1","1"},
            {"1","null","0"}, {"true","false","null"}, {"1","null","0"},
            {"0:true","1:false","0:null"}
        };
        int count = 0;
        for (int c = 0; c < queries.length; c++) {
            if (mode.equals("server-prepared") && queries[c].startsWith("VALUES ")) {
                // Bare VALUES is not in the existing PREPARE grammar. Keep that
                // limitation explicit; derived VALUES below must really prepare.
                try (PreparedStatement ignored = connection.prepareStatement(queries[c])) {
                    throw new AssertionError("bare VALUES PREPARE unexpectedly accepted; add positive coverage");
                } catch (SQLException error) {
                    check(error.getErrorCode() == 1064 && "42000".equals(error.getSQLState()),
                            "unexpected bare VALUES PREPARE failure: " + error);
                }
                System.out.println("CONTROL server-prepared: bare VALUES rejected with 1064/42000");
                continue;
            }
            try (Statement statement = mode.equals("statement") ? connection.createStatement()
                    : connection.prepareStatement(queries[c])) {
                statement.setQueryTimeout(30);
                if (statement instanceof PreparedStatement) {
                    check(!mode.equals("server-prepared") || statement.getClass().getName().equals(
                            "com.mysql.cj.jdbc.ServerPreparedStatement"), "server prepare fell back");
                    metadata(((PreparedStatement) statement).getMetaData(), nativeColumns[c], "prepare");
                }
                try (ResultSet result = statement instanceof PreparedStatement
                        ? ((PreparedStatement) statement).executeQuery() : statement.executeQuery(queries[c])) {
                    metadata(result.getMetaData(), nativeColumns[c], "execute");
                    Map<String,Integer> actual = new HashMap<>();
                    Map<String,Integer> wanted = new HashMap<>();
                    for (String row : expected[c]) wanted.merge(row,1,Integer::sum);
                    while (result.next()) {
                        StringBuilder row = new StringBuilder();
                        for (int col=1; col<=nativeColumns[c].length; col++) {
                            Object value=result.getObject(col);
                            check(value==null || (nativeColumns[c][col-1] ? value instanceof Boolean
                                    : value instanceof Number), "wrong value class: " + queries[c]);
                            if (col>1) row.append(':');
                            row.append(value instanceof Number ? ((Number)value).longValue() : value);
                        }
                        actual.merge(row.toString(),1,Integer::sum);
                    }
                    check(actual.equals(wanted), queries[c] + ": expected " + wanted + ", got " + actual);
                }
            } catch (Exception | AssertionError error) {
                throw new AssertionError(mode + ": " + queries[c],error);
            }
            count++;
        }
        System.out.println("PASS " + mode + ": " + count + " composed cases");
        return count;
    }

    private static int sampleForwardedCases(Connection connection, String table, String mode)
            throws Exception {
        String input = "(SELECT id,flag b,n>0 p FROM " + table
                + " UNION ALL SELECT id,flag b,n>0 p FROM " + table + ") d";
        String[] projections = {
            "SAMPLE(id+1,100 PERCENT),b,p", "SAMPLE(id+1,100 PERCENT),p,b",
            "p,b,SAMPLE(id+1,100 PERCENT)", "b,p,SAMPLE(id+1,100 PERCENT)",
            "SAMPLE(id+1,100 PERCENT),p,b,p,b",
            "SAMPLE(id+1,id+2,100 PERCENT),b,p"
        };
        int[][] types = {
            {Types.BIGINT,Types.BIT,Types.TINYINT}, {Types.BIGINT,Types.TINYINT,Types.BIT},
            {Types.TINYINT,Types.BIT,Types.BIGINT}, {Types.BIT,Types.TINYINT,Types.BIGINT},
            {Types.BIGINT,Types.TINYINT,Types.BIT,Types.TINYINT,Types.BIT},
            {Types.BIGINT,Types.BIGINT,Types.BIT,Types.TINYINT}
        };
        String[][] expected = {
            {"2:true:1", "3:false:null", "4:null:0"},
            {"2:1:true", "3:null:false", "4:0:null"},
            {"1:true:2", "null:false:3", "0:null:4"},
            {"true:1:2", "false:null:3", "null:0:4"},
            {"2:1:true:1:true", "3:null:false:null:false", "4:0:null:0:null"},
            {"2:3:true:1", "3:4:false:null", "4:5:null:0"}
        };
        for (int c = 0; c < projections.length; c++) {
            String sql = "SELECT " + projections[c] + " FROM " + input;
            try (Statement statement = mode.equals("statement") ? connection.createStatement()
                    : connection.prepareStatement(sql)) {
                statement.setQueryTimeout(30);
                if (statement instanceof PreparedStatement) {
                    check(!mode.equals("server-prepared") || statement.getClass().getName().equals(
                            "com.mysql.cj.jdbc.ServerPreparedStatement"), "server prepare fell back");
                    metadata(((PreparedStatement) statement).getMetaData(), types[c], "prepare");
                }
                try (ResultSet result = statement instanceof PreparedStatement
                        ? ((PreparedStatement) statement).executeQuery() : statement.executeQuery(sql)) {
                    metadata(result.getMetaData(), types[c], "execute");
                    Map<String,Integer> actual = new HashMap<>();
                    Map<String,Integer> wanted = new HashMap<>();
                    for (String row : expected[c]) wanted.put(row, 2);
                    while (result.next()) {
                        StringBuilder row = new StringBuilder();
                        for (int col = 1; col <= types[c].length; col++) {
                            Object value = result.getObject(col);
                            check(value == null || (types[c][col-1] == Types.BIT
                                    ? value instanceof Boolean : value instanceof Number),
                                    "wrong value class at column " + col);
                            if (col > 1) row.append(':');
                            row.append(value instanceof Number ? ((Number) value).longValue() : value);
                        }
                        actual.merge(row.toString(), 1, Integer::sum);
                    }
                    check(actual.equals(wanted), "expected " + wanted + ", got " + actual);
                }
            } catch (Exception | AssertionError error) {
                throw new AssertionError(mode + ": " + sql, error);
            }
        }
        System.out.println("PASS " + mode + ": " + projections.length + " forwarded SAMPLE cases");
        return projections.length;
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
                        total += composedCases(connection, table, mode);
                        total += sampleForwardedCases(connection, table, mode);
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
