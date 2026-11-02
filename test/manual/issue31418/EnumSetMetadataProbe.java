import java.sql.*;
import java.util.*;

/** ENUM/SET result metadata and label decoding through Connector/J's real wire paths. */
public final class EnumSetMetadataProbe {
    private static void check(boolean ok, String message) {
        if (!ok) throw new AssertionError(message);
    }

    private static void metadata(ResultSetMetaData meta, int type, boolean declared, String phase)
            throws SQLException {
        check(meta != null && meta.getColumnCount() == 2, phase + ": missing columns");
        for (int i = 1; i <= 2; i++) {
            check(meta.getColumnType(i) == type,
                    phase + ": column " + i + " expected JDBC " + type + ", got " + meta.getColumnType(i));
            check("java.lang.String".equals(meta.getColumnClassName(i)), phase + ": not a String");
            if (type == Types.CHAR) {
                check(meta.getPrecision(i) == (i == 1 ? 2 : 4), phase + ": wrong declared label width");
            }
            com.mysql.cj.result.Field field =
                    ((com.mysql.cj.jdbc.result.ResultSetMetaData) meta).getFields()[i - 1];
            int flags = field.getFlags() & (0x100 | 0x800);
            check(flags == (declared ? (i == 1 ? 0x100 : 0x800) : 0),
                    phase + ": wrong ENUM/SET flags: " + flags);
        }
    }

    private static void labelWidths(ResultSetMetaData meta) throws SQLException {
        check(meta.getPrecision(1) == 2 && meta.getPrecision(2) == 4, "wrong materialized label width");
    }

    private static void rows(ResultSet result, int type, boolean declared, List<List<String>> expectedRows, String phase)
            throws SQLException {
        metadata(result.getMetaData(), type, declared, phase);
        Map<List<String>, Integer> actual = new HashMap<>();
        while (result.next()) {
            actual.merge(Arrays.asList(result.getString(1), result.getString(2)), 1, Integer::sum);
        }
        Map<List<String>, Integer> expected = new HashMap<>();
        for (List<String> row : expectedRows) expected.merge(row, 1, Integer::sum);
        check(actual.equals(expected), phase + ": expected " + expected + ", got " + actual);
    }

    private static void query(Connection connection, String mode, String sql, int type,
                              boolean declared, int copies) throws Exception {
        List<List<String>> expectedRows = new ArrayList<>();
        for (int i = 0; i < copies; i++) {
            expectedRows.add(Arrays.asList("a", "x,yy"));
            expectedRows.add(Arrays.asList("", ""));
            expectedRows.add(Arrays.asList(null, null));
        }
        query(connection, mode, sql, type, declared, expectedRows);
    }

    private static void query(Connection connection, String mode, String sql, int type,
                              boolean declared, List<List<String>> expectedRows) throws Exception {
        query(connection, mode, sql, type, type, declared, expectedRows);
    }

    private static void query(Connection connection, String mode, String sql, int type, int prepareType,
                              boolean declared, List<List<String>> expectedRows) throws Exception {
        if (mode.equals("statement")) {
            try (Statement statement = connection.createStatement()) {
                statement.setQueryTimeout(30);
                try (ResultSet result = statement.executeQuery(sql)) {
                    if (type != prepareType) labelWidths(result.getMetaData());
                    rows(result, type, declared, expectedRows, mode + ": " + sql);
                }
            }
        } else {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setQueryTimeout(30);
                if (mode.equals("server-prepared") || mode.equals("cursor")) {
                    check(statement.getClass().getName().equals("com.mysql.cj.jdbc.ServerPreparedStatement"),
                            "server prepare silently fell back");
                }
                if (mode.equals("cursor")) statement.setFetchSize(1);
                metadata(statement.getMetaData(), mode.equals("server-prepared") || mode.equals("cursor") ? prepareType : type,
                        declared, mode + " prepare: " + sql);
                for (int i = 0; i < 2; i++) {
                    try (ResultSet result = statement.executeQuery()) {
                        if (type != prepareType) labelWidths(result.getMetaData());
                        rows(result, type, declared, expectedRows, mode + " execute: " + sql);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("JDBC_URL USER PASSWORD");
        for (String mode : new String[]{"statement", "client-prepared", "server-prepared", "cursor"}) {
            Properties properties = new Properties();
            properties.setProperty("user", args[1]);
            properties.setProperty("password", args[2]);
            properties.setProperty("useServerPrepStmts", String.valueOf(mode.equals("server-prepared") || mode.equals("cursor")));
            properties.setProperty("useCursorFetch", String.valueOf(mode.equals("cursor")));
            properties.setProperty("emulateUnsupportedPstmts", "false");
            properties.setProperty("socketTimeout", "30000");
            properties.setProperty("useSSL", "false");
            properties.setProperty("allowPublicKeyRetrieval", "true");
            String db = "claude_enum_metadata_" + UUID.randomUUID().toString().replace("-", "");
            try (Connection connection = DriverManager.getConnection(args[0], properties);
                 Statement setup = connection.createStatement()) {
                setup.setQueryTimeout(30);
                setup.execute("CREATE DATABASE " + db);
                try {
                    setup.execute("USE " + db);
                    setup.execute("CREATE TABLE t (id INT PRIMARY KEY, e ENUM('a','bb',''), s SET('x','yy'))");
                    setup.execute("INSERT INTO t VALUES (1,'a','x,yy'),(2,'',''),(3,NULL,NULL)");
                    List<List<String>> rollup = Arrays.asList(
                            Arrays.asList("a", "x,yy"), Arrays.asList("a", null),
                            Arrays.asList("", ""), Arrays.asList("", null),
                            Arrays.asList(null, null), Arrays.asList(null, null), Arrays.asList(null, null));
                    // Cursor coverage below uses its own phase oracle; the earlier
                    // non-cursor scalar-expression cases are not cursor contracts.
                    if (!mode.equals("cursor")) {
                    for (String sql : new String[]{
                            "SELECT e, s FROM t",
                            "SELECT e AS alias_e, s AS alias_s FROM t",
                            "SELECT DISTINCT e, s FROM t",
                            "SELECT e, s FROM t GROUP BY e, s",
                            "SELECT e, s FROM t GROUP BY id",
                            "SELECT e, s FROM (SELECT e, s FROM t GROUP BY id) d",
                            "SELECT (SELECT e), (SELECT s) FROM t",
                            "SELECT e, s FROM (SELECT e, s FROM t LIMIT 3) d",
                            "WITH c AS (SELECT e, s FROM t) SELECT e, s FROM c"}) {
                        query(connection, mode, sql, Types.CHAR, true, 1);
                    }
                    query(connection, mode, "SELECT e, s FROM t WHERE 1=0", Types.CHAR, true, 0);
                    query(connection, mode, "SELECT MIN(e), MAX(s) FROM t", Types.CHAR, false,
                            Collections.singletonList(Arrays.asList("", "x,yy")));
                    query(connection, mode, "SELECT MAX(e), MIN(s) FROM t", Types.CHAR, false,
                            Collections.singletonList(Arrays.asList("a", "")));
                    query(connection, mode, "SELECT e, s FROM (SELECT MIN(e) e, MAX(s) s FROM t) d",
                            Types.CHAR, true, Collections.singletonList(Arrays.asList("", "x,yy")));
                    query(connection, mode, "SELECT MIN(e), MAX(s) FROM t WHERE 1=0", Types.CHAR, false,
                            Collections.singletonList(Arrays.asList(null, null)));
                    query(connection, mode, "SELECT (SELECT e FROM t WHERE id=1), (SELECT s FROM t WHERE id=1)",
                            Types.CHAR, false, Collections.singletonList(Arrays.asList("a", "x,yy")));
                    query(connection, mode, "SELECT (SELECT e FROM t WHERE 1=0), (SELECT s FROM t WHERE 1=0)",
                            Types.CHAR, false, Collections.singletonList(Arrays.asList(null, null)));
                    query(connection, mode, "SELECT (SELECT b.e FROM t b WHERE b.id=a.id), "
                            + "(SELECT b.s FROM t b WHERE b.id=a.id) FROM t a", Types.CHAR, false, 1);
                    query(connection, mode, "SELECT CONCAT(MIN(e)), CAST(MAX(s) AS CHAR) FROM t", Types.VARCHAR, false,
                            Collections.singletonList(Arrays.asList("", "x,yy")));
                    query(connection, mode, "SELECT e, s FROM (SELECT (SELECT e FROM t WHERE id=1) e, "
                            + "(SELECT s FROM t WHERE id=1) s) d", Types.VARCHAR, false,
                            Collections.singletonList(Arrays.asList("a", "x,yy")));
                    query(connection, mode, "SELECT e, s FROM t GROUP BY e, s WITH ROLLUP",
                            Types.CHAR, false, rollup);
                    List<List<String>> rollupUnion = new ArrayList<>(rollup);
                    rollupUnion.addAll(Arrays.asList(Arrays.asList("a", "x,yy"),
                            Arrays.asList("", ""), Arrays.asList(null, null)));
                    query(connection, mode, "SELECT e, s FROM t GROUP BY e, s WITH ROLLUP UNION ALL SELECT e, s FROM t",
                            Types.CHAR, false, rollupUnion);
                    query(connection, mode, "SELECT MIN(e), MAX(s) FROM t GROUP BY id WITH ROLLUP",
                            Types.CHAR, false, Arrays.asList(Arrays.asList("a", "x,yy"),
                                    Arrays.asList("", ""), Arrays.asList(null, null), Arrays.asList("", "x,yy")));
                    query(connection, mode, "SELECT MIN(e) OVER (), MAX(s) OVER () FROM t",
                            Types.VARCHAR, Types.CHAR, false, Arrays.asList(Arrays.asList("", "x,yy"),
                                    Arrays.asList("", "x,yy"), Arrays.asList("", "x,yy")));
                    query(connection, mode, "SELECT DISTINCT e, s FROM t GROUP BY e, s WITH ROLLUP",
                            Types.VARCHAR, Types.CHAR, false, Arrays.asList(Arrays.asList("a", "x,yy"),
                                    Arrays.asList("a", null), Arrays.asList("", ""),
                                    Arrays.asList("", null), Arrays.asList(null, null)));
                    query(connection, mode, "SELECT e, s FROM (SELECT e, s FROM t GROUP BY e, s WITH ROLLUP) d",
                            Types.VARCHAR, false, rollup);
                    query(connection, mode, "SELECT e, s FROM (SELECT MIN(e) OVER () e, MAX(s) OVER () s FROM t) d",
                            Types.VARCHAR, false, Arrays.asList(Arrays.asList("", "x,yy"),
                                    Arrays.asList("", "x,yy"), Arrays.asList("", "x,yy")));
                    }
                    query(connection, mode, "SELECT MIN(e),MAX(s) FROM (SELECT e,s FROM t GROUP BY e,s WITH ROLLUP) d",
                            Types.VARCHAR, false, Collections.singletonList(Arrays.asList("", "x,yy")));
                    query(connection, mode, "SELECT (SELECT e FROM (SELECT e,s FROM t GROUP BY e,s WITH ROLLUP) d "
                            + "WHERE e='a' AND s IS NOT NULL LIMIT 1), (SELECT s FROM (SELECT e,s FROM t GROUP BY e,s WITH ROLLUP) d "
                            + "WHERE e='a' AND s IS NOT NULL LIMIT 1)", Types.VARCHAR, false,
                            Collections.singletonList(Arrays.asList("a", "x,yy")));
                    List<List<String>> widthOnlyWindow = new ArrayList<>();
                    for (int i=0;i<rollup.size();i++) widthOnlyWindow.add(Arrays.asList("", "x,yy"));
                    query(connection, mode, "SELECT MIN(e) OVER (),MAX(s) OVER () FROM "
                            + "(SELECT e,s FROM t GROUP BY e,s WITH ROLLUP) d", Types.VARCHAR, false, widthOnlyWindow);
                    List<List<String>> widthOnlyUnion = new ArrayList<>(rollup);
                    widthOnlyUnion.addAll(rollup);
                    query(connection, mode, "SELECT e,s FROM (SELECT e,s FROM t GROUP BY e,s WITH ROLLUP) d "
                            + "UNION ALL SELECT e,s FROM t GROUP BY e,s WITH ROLLUP", Types.VARCHAR, false, widthOnlyUnion);
                    List<List<String>> doubleRollup = new ArrayList<>(rollup);
                    doubleRollup.add(Arrays.asList("a", null)); doubleRollup.add(Arrays.asList("", null));
                    query(connection, mode, "SELECT e,s FROM (SELECT e,s FROM t GROUP BY e,s WITH ROLLUP) d "
                            + "GROUP BY e,s WITH ROLLUP", Types.VARCHAR, false, doubleRollup);
                    List<List<String>> distinctRollup = Arrays.asList(Arrays.asList("a", "x,yy"),
                            Arrays.asList("a", null), Arrays.asList("", ""), Arrays.asList("", null), Arrays.asList(null, null));
                    query(connection, mode, "SELECT e,s FROM (SELECT e,s FROM t GROUP BY e,s WITH ROLLUP) d UNION "
                            + "SELECT e,s FROM (SELECT e,s FROM t GROUP BY e,s WITH ROLLUP) d", Types.VARCHAR, false, distinctRollup);
                    for (String predicate : new String[]{"", " WHERE TRUE"}) {
                        query(connection, mode, "SELECT e,s FROM t GROUP BY e,s WITH ROLLUP UNION "
                                + "SELECT e,s FROM t"+predicate+" GROUP BY e,s WITH ROLLUP", Types.CHAR, false, distinctRollup);
                    }
                    for (String sql : new String[]{
                            "SELECT MIN(e) OVER (),MAX(s) OVER () FROM t LIMIT 0",
                            "SELECT MIN(e) OVER (),MAX(s) OVER () FROM t WHERE FALSE",
                            "SELECT MIN(e) OVER (),MAX(s) OVER () FROM t WHERE NULL",
                            "SELECT MIN(l.e) OVER (),MAX(l.s) OVER () FROM t l JOIN t r ON FALSE",
                            "SELECT MIN(e) OVER (),MAX(s) OVER () FROM t WHERE id=100",
                            "SELECT MIN(MIN(e)) OVER (),MAX(MAX(s)) OVER () FROM t WHERE FALSE HAVING COUNT(*)>0",
                            "SELECT DISTINCT e,s FROM t WHERE FALSE GROUP BY e,s WITH ROLLUP",
                            "SELECT DISTINCT e,s FROM t GROUP BY e,s WITH ROLLUP LIMIT 0"}) {
                        query(connection, mode, sql, mode.equals("cursor") ? Types.VARCHAR : Types.CHAR,
                                Types.CHAR, false, Collections.<List<String>>emptyList());
                    }
                    int constantWindowType = mode.equals("cursor") ? Types.VARCHAR : Types.CHAR;
                    query(connection, mode, "SELECT MIN(MIN(e)) OVER (),MAX(MAX(s)) OVER () FROM t WHERE FALSE",
                            constantWindowType, Types.CHAR, false, Collections.singletonList(Arrays.asList(null, null)));
                    query(connection, mode, "SELECT MIN(MIN(e)) OVER (),MAX(MAX(s)) OVER () FROM t",
                            constantWindowType, Types.CHAR, false, Collections.singletonList(Arrays.asList("", "x,yy")));
                    for (String sql : new String[]{
                            "SELECT MIN(e) OVER (),MAX(s) OVER () FROM t WHERE id=1",
                            "SELECT MIN(e) OVER (),MAX(s) OVER () FROM (SELECT e,s FROM t ORDER BY id LIMIT 1) d",
                            "SELECT MIN(l.e) OVER (),MAX(l.s) OVER () FROM t l LEFT JOIN t r ON FALSE WHERE l.id=1"}) {
                        query(connection, mode, sql, constantWindowType, Types.CHAR, false,
                                Collections.singletonList(Arrays.asList("a", "x,yy")));
                    }
                    query(connection, mode, "SELECT MIN(l.e) OVER (),MAX(l.s) OVER () FROM t l WHERE l.id=1 "
                            + "AND EXISTS(SELECT 1 FROM t r WHERE r.id%2=l.id)", Types.VARCHAR, Types.CHAR, false,
                            Collections.singletonList(Arrays.asList("a", "x,yy")));
                    query(connection, mode, "SELECT MIN(l.e) OVER (),MAX(l.s) OVER () FROM t l WHERE l.id=1 "
                            + "AND NOT EXISTS(SELECT 1 FROM t r WHERE r.id%2=l.id)", Types.VARCHAR, Types.CHAR, false,
                            Collections.<List<String>>emptyList());
                    query(connection, mode, "SELECT MIN(e) OVER (),MAX(s) OVER () FROM t LIMIT 1",
                            Types.VARCHAR, Types.CHAR, false, Collections.singletonList(Arrays.asList("", "x,yy")));
                    query(connection, mode, "SELECT MIN(e) OVER (),MAX(s) OVER () FROM t WHERE id<2",
                            Types.VARCHAR, Types.CHAR, false, Collections.singletonList(Arrays.asList("a", "x,yy")));
                    query(connection, mode, "SELECT MIN(e) OVER (),MAX(s) OVER () FROM (SELECT e,s FROM t ORDER BY id LIMIT 2) d",
                            Types.VARCHAR, Types.CHAR, false, Arrays.asList(Arrays.asList("", "x,yy"), Arrays.asList("", "x,yy")));
                    query(connection, mode, "SELECT MIN(l.e) OVER (),MAX(l.s) OVER () FROM t l LEFT JOIN t r ON FALSE",
                            Types.VARCHAR, Types.CHAR, false, Arrays.asList(Arrays.asList("", "x,yy"),
                                    Arrays.asList("", "x,yy"), Arrays.asList("", "x,yy")));
                    if (mode.equals("server-prepared") || mode.equals("cursor")) {
                        try (PreparedStatement limit = connection.prepareStatement("SELECT MIN(e) OVER (),MAX(s) OVER () FROM t LIMIT ?")) {
                            limit.setQueryTimeout(30);
                            check(limit.getClass().getName().equals("com.mysql.cj.jdbc.ServerPreparedStatement"),
                                    "parameterized prepare silently fell back");
                            if (mode.equals("cursor")) limit.setFetchSize(1);
                            metadata(limit.getMetaData(), Types.CHAR, false, mode + " parameterized prepare");
                            for (Object count : new Object[]{0, 1, null, false, true, 0}) {
                                if (count == null) limit.setNull(1, Types.INTEGER);
                                else if (count instanceof Boolean) limit.setBoolean(1, (Boolean) count);
                                else limit.setInt(1, (Integer) count);
                                boolean zero = count == null || Boolean.FALSE.equals(count) || Integer.valueOf(0).equals(count);
                                try (ResultSet result = limit.executeQuery()) {
                                    int expectedType = zero && !mode.equals("cursor") ? Types.CHAR : Types.VARCHAR;
                                    labelWidths(result.getMetaData());
                                    rows(result,expectedType,false,zero ? Collections.<List<String>>emptyList()
                                            : Collections.singletonList(Arrays.asList("", "x,yy")), mode+" LIMIT "+count);
                                }
                            }
                        }
                    }
                    query(connection, mode, "SELECT MIN(e) OVER (),MAX(s) OVER () FROM t WHERE id>100",
                            Types.VARCHAR, Types.CHAR, false, Collections.<List<String>>emptyList());
                    query(connection, mode, "SELECT MIN(e) OVER (),MAX(s) OVER () FROM "
                            + "(SELECT e,s FROM t GROUP BY e,s WITH ROLLUP) d LIMIT 0", Types.VARCHAR, false,
                            Collections.<List<String>>emptyList());
                    if (!mode.equals("cursor")) {
                    setup.execute("CREATE VIEW v AS SELECT e, s FROM t");
                    query(connection, mode, "SELECT e, s FROM v", Types.CHAR, true, 1);
                    query(connection, mode, "SELECT CONCAT(e), CAST(s AS CHAR) FROM t", Types.VARCHAR, false, 1);
                    query(connection, mode, "SELECT e, s FROM t UNION ALL SELECT e, s FROM t", Types.VARCHAR, false, 2);
                    }
                } finally {
                    setup.execute("DROP DATABASE " + db);
                }
                System.out.println("PASS " + mode);
            }
        }
    }
}
