import java.sql.*;
import java.util.*;
import java.math.BigDecimal;

/** ENUM/SET result metadata and label decoding through Connector/J's real wire paths. */
public final class EnumSetMetadataProbe {
    private static void check(boolean ok, String message) {
        if (!ok) throw new AssertionError(message);
    }

    private static void metadata(ResultSetMetaData meta, int type, boolean declared, String phase, int... columnCount)
            throws SQLException {
        check(meta != null && meta.getColumnCount() == (columnCount.length == 0 ? 2 : columnCount[0]), phase + ": missing columns");
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

    private static void rows(ResultSet result, int type, boolean declared, List<List<String>> expectedRows, String phase, int... columnCount)
            throws SQLException {
        metadata(result.getMetaData(), type, declared, phase, columnCount);
        Map<List<String>, Integer> actual = new HashMap<>();
        while (result.next()) {
            List<String> row = new ArrayList<>();
            for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) row.add(result.getString(i));
            actual.merge(row, 1, Integer::sum);
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
                              boolean declared, List<List<String>> expectedRows, int... columnCount) throws Exception {
        if (mode.equals("statement")) {
            try (Statement statement = connection.createStatement()) {
                statement.setQueryTimeout(30);
                try (ResultSet result = statement.executeQuery(sql)) {
                    if (type != prepareType) labelWidths(result.getMetaData());
                    rows(result, type, declared, expectedRows, mode + ": " + sql, columnCount);
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
                        declared, mode + " prepare: " + sql, columnCount);
                for (int i = 0; i < 2; i++) {
                    try (ResultSet result = statement.executeQuery()) {
                        if (type != prepareType) labelWidths(result.getMetaData());
                        rows(result, type, declared, expectedRows, mode + " execute: " + sql, columnCount);
                    }
                }
            }
        }
    }

    private static void rangeExecution(PreparedStatement statement,Integer[] range,boolean cursor) throws Exception {
        for (int i=0;i<2;i++) {
            if (range[i]==null) statement.setNull(i+1,Types.INTEGER); else statement.setInt(i+1,range[i]);
        }
        boolean empty=range[0]==null || range[1]==null || range[0]>range[1];
        List<List<String>> wanted=empty ? Collections.<List<String>>emptyList() : range[0].equals(range[1])
                ? Collections.singletonList(Arrays.asList("a", "x,yy"))
                : Arrays.asList(Arrays.asList("", "x,yy"),Arrays.asList("", "x,yy"));
        try (ResultSet result=statement.executeQuery()) {
            rows(result,cursor || !empty ? Types.VARCHAR : Types.CHAR,false,wanted,"range "+Arrays.toString(range));
            labelWidths(result.getMetaData());
        }
    }

    private static void decimalRangeExecution(PreparedStatement statement,String[] range,boolean cursor) throws Exception {
        for (int i=0;i<2;i++) {
            if (range[i]==null) statement.setNull(i+1,Types.DECIMAL); else statement.setBigDecimal(i+1,new BigDecimal(range[i]));
        }
        boolean empty=range[0]==null || range[1]==null || new BigDecimal(range[0]).compareTo(new BigDecimal(range[1]))>0;
        try (ResultSet result=statement.executeQuery()) {
            rows(result,cursor || !empty ? Types.VARCHAR : Types.CHAR,false,
                    empty ? Collections.<List<String>>emptyList() : Collections.singletonList(Arrays.asList("", "")),
                    "decimal range "+Arrays.toString(range));
            labelWidths(result.getMetaData());
        }
    }

    private static void sqlDecimalPrepared(Statement statement,String[][] ranges,boolean strings) throws Exception {
        statement.execute("PREPARE enum_decimal_range FROM 'SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id BETWEEN ? AND ?'");
        try {
            for (String[] range:ranges) {
                String lo=strings && !range[0].equals("NULL") ? "'"+range[0]+"'" : range[0];
                String hi=strings && !range[1].equals("NULL") ? "'"+range[1]+"'" : range[1];
                statement.execute("SET @enum_decimal_lo="+lo+",@enum_decimal_hi="+hi);
                boolean empty=range[0].equals("NULL") || range[1].equals("NULL") ||
                        new BigDecimal(range[0]).compareTo(new BigDecimal(range[1]))>0;
                try (ResultSet result=statement.executeQuery("EXECUTE enum_decimal_range USING @enum_decimal_lo,@enum_decimal_hi")) {
                    rows(result,empty ? Types.CHAR : Types.VARCHAR,false,
                            empty ? Collections.<List<String>>emptyList() : Collections.singletonList(Arrays.asList("","")),
                            "SQL "+(strings ? "string " : "decimal ")+Arrays.toString(range));
                    labelWidths(result.getMetaData());
                }
            }
        } finally {
            statement.execute("DEALLOCATE PREPARE enum_decimal_range");
        }
    }

    private static void sqlDecimalRanges(Connection connection) throws Exception {
        String[][] ranges={{"2.5","1.5"},{"1.5","2.5"},{"NULL","2.5"},{"1.5","NULL"},{"2.50","1.500"},{"1.5","2.5"}};
        try (Statement statement=connection.createStatement()) {
            statement.setQueryTimeout(30);
            try {
                for (boolean strings:new boolean[]{false,true}) {
                    for (String[] range:ranges) sqlDecimalPrepared(statement,new String[][]{range},strings);
                    sqlDecimalPrepared(statement,ranges,strings);
                }
            } finally {
                statement.execute("SET @enum_decimal_lo=NULL,@enum_decimal_hi=NULL");
            }
        }
    }

    private static void stringDoubleRange(PreparedStatement statement,String[] range,boolean strings,boolean cursor) throws Exception {
        for (int i=0;i<2;i++) {
            if (range[i]==null) statement.setNull(i+1,strings ? Types.VARCHAR : Types.DOUBLE);
            else if (strings) statement.setString(i+1,range[i]); else statement.setDouble(i+1,Double.parseDouble(range[i]));
        }
        boolean empty=range[0]==null || range[1]==null || new BigDecimal(range[0]).compareTo(new BigDecimal(range[1]))>0;
        try (ResultSet result=statement.executeQuery()) {
            rows(result,cursor || !empty ? Types.VARCHAR : Types.CHAR,false,
                    empty ? Collections.<List<String>>emptyList() : Arrays.asList(Arrays.asList("","x,yy"),Arrays.asList("","x,yy")),
                    (strings ? "String " : "Double ")+Arrays.toString(range));
            labelWidths(result.getMetaData());
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
                    setup.execute("CREATE TABLE t (id INT PRIMARY KEY, e ENUM('a','bb',''), s SET('x','yy'), u INT UNIQUE, n INT)");
                    setup.execute("INSERT INTO t VALUES (1,'a','x,yy',1,1),(2,'','',NULL,2),(3,NULL,NULL,NULL,3)");
                    setup.execute("CREATE TABLE c (a INT,b INT,e ENUM('a','bb',''),s SET('x','yy'),PRIMARY KEY(a,b))");
                    setup.execute("INSERT INTO c SELECT IF(id=3,2,1),IF(id=2,2,1),e,s FROM t");
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
                    if (mode.equals("cursor")) {
                        query(connection,mode,"SELECT e,s FROM t",Types.CHAR,true,1);
                        query(connection,mode,"SELECT MIN(e),MAX(s) FROM t",Types.CHAR,false,Collections.singletonList(Arrays.asList("","x,yy")));
                        query(connection,mode,"SELECT (SELECT e FROM t WHERE id=1),(SELECT s FROM t WHERE id=1)",
                                Types.VARCHAR,Types.CHAR,false,Collections.singletonList(Arrays.asList("a","x,yy")));
                        query(connection,mode,"SELECT (SELECT e FROM t WHERE FALSE),(SELECT s FROM t WHERE FALSE)",
                                Types.VARCHAR,Types.CHAR,false,Collections.singletonList(Arrays.asList(null,null)));
                        query(connection,mode,"SELECT e,s FROM t GROUP BY e,s WITH ROLLUP",Types.VARCHAR,Types.CHAR,false,rollup);
                        query(connection,mode,"SELECT MIN(e),MAX(s) FROM t GROUP BY id WITH ROLLUP",Types.CHAR,Types.CHAR,false,
                                Arrays.asList(Arrays.asList("a","x,yy"),Arrays.asList("",""),Arrays.asList(null,null),Arrays.asList("","x,yy")));
                    }
                    query(connection, mode, "SELECT MIN(e),MAX(s) FROM (SELECT e,s FROM t GROUP BY e,s WITH ROLLUP) d",
                            Types.VARCHAR, false, Collections.singletonList(Arrays.asList("", "x,yy")));
                    query(connection, mode, "SELECT (SELECT e FROM (SELECT e,s FROM t GROUP BY e,s WITH ROLLUP) d "
                            + "WHERE e='a' AND s IS NOT NULL LIMIT 1), (SELECT s FROM (SELECT e,s FROM t GROUP BY e,s WITH ROLLUP) d "
                            + "WHERE e='a' AND s IS NOT NULL LIMIT 1)", Types.VARCHAR, false,
                            Collections.singletonList(Arrays.asList("a", "x,yy")));
                    query(connection,mode,"SELECT e,s FROM(SELECT MIN(e)e,MAX(s)s FROM t GROUP BY id WITH ROLLUP)d",
                            Types.CHAR,true,Arrays.asList(Arrays.asList("a","x,yy"),Arrays.asList("",""),Arrays.asList(null,null),Arrays.asList("","x,yy")));
                    query(connection,mode,"SELECT e,s FROM(SELECT e,s FROM t GROUP BY e,s WITH ROLLUP UNION SELECT e,s FROM t GROUP BY e,s WITH ROLLUP)d",
                            Types.CHAR,false,Arrays.asList(Arrays.asList("a","x,yy"),Arrays.asList("a",null),Arrays.asList("",""),Arrays.asList("",null),Arrays.asList(null,null)));
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
                    for (String sql : new String[]{
                            "SELECT MIN(e) OVER(PARTITION BY id),MAX(s) OVER(PARTITION BY id) FROM t WHERE id=1",
                            "SELECT MIN(MIN(e)) OVER(),MAX(MAX(s)) OVER() FROM t WHERE id=1 GROUP BY id",
                            "SELECT MIN(e) OVER(),MAX(s) OVER() FROM (SELECT e,s,ROW_NUMBER() OVER(ORDER BY id)n FROM t ORDER BY id LIMIT 1)d WHERE n>0",
                            "SELECT MIN(l.e) OVER(),MAX(l.s) OVER() FROM t l WHERE l.id=1 AND EXISTS(SELECT 1 FROM t r WHERE r.id=1 AND r.id=l.id)",
                            "SELECT MIN(l.e) OVER(),MAX(l.s) OVER() FROM t l WHERE l.id=1 AND NOT EXISTS(SELECT 1 FROM t r WHERE r.id=100)",
                            "SELECT MIN(e) OVER(),MAX(s) OVER() FROM t WHERE id BETWEEN 1 AND 1",
                            "SELECT MIN(e) OVER(),MAX(s) OVER() FROM t WHERE id<=>1",
                            "SELECT MIN(e) OVER(),MAX(s) OVER() FROM t WHERE u<=>1"}) {
                        query(connection, mode, sql, constantWindowType, Types.CHAR, false,
                                Collections.singletonList(Arrays.asList("a", "x,yy")));
                    }
                    for (String sql : new String[]{
                            "SELECT MIN(e) OVER(PARTITION BY id),MAX(s) OVER(PARTITION BY id) FROM t WHERE FALSE",
                            "SELECT MIN(MIN(e)) OVER(),MAX(MAX(s)) OVER() FROM t WHERE FALSE GROUP BY id",
                            "SELECT MIN(l.e) OVER(),MAX(l.s) OVER() FROM t l WHERE l.id=1 AND NOT EXISTS(SELECT 1 FROM t r WHERE r.id=1)"}) {
                        query(connection, mode, sql, constantWindowType, Types.CHAR, false, Collections.<List<String>>emptyList());
                    }
                    query(connection, mode, "SELECT MIN(e) OVER(),MAX(s) OVER(),(SELECT COUNT(*) FROM t) FROM t WHERE id=1",
                            constantWindowType, Types.CHAR, false, Collections.singletonList(Arrays.asList("a", "x,yy", "3")), 3);
                    query(connection, mode, "SELECT MIN(e) OVER(),MAX(s) OVER(),(SELECT COUNT(*) FROM t) FROM t LIMIT 1",
                            Types.VARCHAR, Types.CHAR, false, Collections.singletonList(Arrays.asList("", "x,yy", "3")), 3);
                    query(connection, mode, "SELECT MIN(e) OVER(),MAX(s) OVER() FROM t WHERE u<=>NULL",
                            Types.VARCHAR, Types.CHAR, false, Arrays.asList(Arrays.asList("", ""),Arrays.asList("", "")));
                    query(connection, mode, "SELECT MIN(l.e) OVER(),MAX(l.s) OVER() FROM t l WHERE l.id=1 AND NOT EXISTS(SELECT 1 FROM t r WHERE r.id=1 AND r.id=l.id)",
                            Types.VARCHAR, Types.CHAR, false, Collections.<List<String>>emptyList());
                    for (String sql : new String[]{
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=1 AND EXISTS(SELECT COUNT(*) FROM t)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=1 AND EXISTS(SELECT COUNT(*) FROM t WHERE FALSE)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=1 AND EXISTS(SELECT COUNT(*) FROM t GROUP BY id)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=1 AND EXISTS(SELECT COUNT(*) FROM t HAVING COUNT(*)>0)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=(SELECT MIN(id) FROM t)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=(SELECT id FROM t WHERE id=1)"}) {
                        query(connection, mode, sql, constantWindowType, Types.CHAR, false,
                                Collections.singletonList(Arrays.asList("a", "x,yy")));
                    }
                    query(connection, mode, "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=1 AND NOT EXISTS(SELECT COUNT(*) FROM t)",
                            constantWindowType, Types.CHAR, false, Collections.<List<String>>emptyList());
                    query(connection, mode, "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=(SELECT COUNT(*) FROM t)",
                            constantWindowType, Types.CHAR, false, Collections.singletonList(Arrays.asList(null, null)));
                    for (String sql : new String[]{
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=1 AND EXISTS(SELECT 1 FROM (SELECT id FROM t WHERE id=1 LIMIT 1)d)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=1 AND EXISTS(SELECT 1 FROM (SELECT COUNT(*) n FROM t)d)"}) {
                        query(connection, mode, sql, Types.VARCHAR, Types.CHAR, false,
                                Collections.singletonList(Arrays.asList("a", "x,yy")));
                    }
                    query(connection, mode, "SELECT MIN(l.e)OVER(),MAX(l.s)OVER() FROM t l WHERE l.id=(SELECT MIN(r.id) FROM t r WHERE r.id>=l.id)",
                            Types.VARCHAR, Types.CHAR, false, Arrays.asList(Arrays.asList("", "x,yy"),Arrays.asList("", "x,yy"),Arrays.asList("", "x,yy")));
                    for (String sql : new String[]{
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=(SELECT COUNT(*) FROM t WHERE id<=1)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=1 AND NOT EXISTS(SELECT 1 FROM (SELECT id FROM t WHERE id=100 LIMIT 1)d)",
                            "SELECT MIN(l.e)OVER(),MAX(l.s)OVER() FROM t l WHERE l.id=1 AND EXISTS(SELECT COUNT(*) FROM t r WHERE r.id>=l.id)",
                            "SELECT MIN(l.e)OVER(),MAX(l.s)OVER() FROM t l WHERE l.id=1 AND EXISTS(SELECT COUNT(*) FROM t r WHERE r.id>=l.id HAVING COUNT(*)>0)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=(SELECT MIN(id) FROM t) AND id=(SELECT MIN(id) FROM t)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=COALESCE((SELECT MIN(id) FROM t),1)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=(SELECT MIN(id) FROM t WHERE id=(SELECT MIN(id) FROM t))",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=(SELECT MIN(id) FROM t) AND id=COALESCE((SELECT MIN(id) FROM t),1)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM c WHERE a=(SELECT MIN(id) FROM t) AND b=(SELECT MIN(id) FROM t)"}) {
                        query(connection, mode, sql, constantWindowType, Types.CHAR, false,
                                Collections.singletonList(Arrays.asList("a", "x,yy")));
                    }
                    for (String sql : new String[]{
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=1 AND NOT EXISTS(SELECT 1 FROM (SELECT id FROM t WHERE id=1 LIMIT 1)d)",
                            "SELECT MIN(l.e)OVER(),MAX(l.s)OVER() FROM t l WHERE l.id=1 AND NOT EXISTS(SELECT COUNT(*) FROM t r WHERE r.id>=l.id)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id BETWEEN 2 AND 1",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id BETWEEN NULL AND 1",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id BETWEEN 1 AND NULL"}) {
                        query(connection, mode, sql, constantWindowType, Types.CHAR, false, Collections.<List<String>>emptyList());
                    }
                    for (String sql : new String[]{
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE CAST(id AS SIGNED)=(SELECT COUNT(*) FROM t WHERE id<=1)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE CAST(id AS SIGNED)=1"}) {
                        query(connection, mode, sql, Types.VARCHAR, Types.CHAR, false,
                                Collections.singletonList(Arrays.asList("a", "x,yy")));
                    }
                    query(connection, mode, "SELECT MIN(e)OVER(),MAX(s)OVER() FROM c WHERE a=(SELECT MIN(id) FROM t)",
                            Types.VARCHAR, Types.CHAR, false, Arrays.asList(Arrays.asList("", "x,yy"),Arrays.asList("", "x,yy")));
                    query(connection, mode, "SELECT MIN(l.e)OVER(),MAX(l.s)OVER() FROM t l WHERE l.id=COALESCE((SELECT MIN(r.id) FROM t r WHERE r.id>=l.id),1)",
                            Types.VARCHAR, Types.CHAR, false, Arrays.asList(Arrays.asList("", "x,yy"),Arrays.asList("", "x,yy"),Arrays.asList("", "x,yy")));
                    for (String sql : new String[]{
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM (SELECT id,e,s FROM t ORDER BY id LIMIT 2)d WHERE id=(SELECT MIN(id) FROM t)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM (SELECT id,e,s FROM t ORDER BY id LIMIT 2)d WHERE id=1"}) {
                        query(connection, mode, sql, Types.VARCHAR, Types.CHAR, false,
                                Collections.singletonList(Arrays.asList("a", "x,yy")));
                    }
                    for (String sql : new String[]{
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM (SELECT id,e,s FROM t)d WHERE id=(SELECT MIN(id) FROM t)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM (SELECT id,e,s FROM t ORDER BY id LIMIT 1)d WHERE id=(SELECT MIN(id) FROM t)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=1 AND EXISTS(SELECT COUNT(*) FROM t b WHERE b.id=t.id GROUP BY b.e)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=1 AND EXISTS(SELECT COUNT(*) FROM t b WHERE b.id>=t.id GROUP BY b.e)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=1 AND EXISTS(SELECT 1 FROM t b WHERE b.id=(SELECT MIN(id) FROM t))",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=1 AND EXISTS(SELECT COUNT(*) FROM t b WHERE b.id=(SELECT MIN(id) FROM t) GROUP BY b.e)",
                            "SELECT MIN(a.e)OVER(),MAX(a.s)OVER() FROM t a JOIN (SELECT id FROM t ORDER BY id LIMIT 1)b ON a.id=b.id",
                            "SELECT MIN(a.e)OVER(),MAX(a.s)OVER() FROM t a JOIN (SELECT id FROM t ORDER BY id LIMIT 1)b ON TRUE WHERE a.id=b.id",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=1 OR FALSE",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=1 OR id=1",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=1 OR NULL",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE (id=1 OR FALSE) AND EXISTS(SELECT 1 FROM t b WHERE b.id=(SELECT MIN(id) FROM t))"}) {
                        query(connection, mode, sql, constantWindowType, Types.CHAR, false,
                                Collections.singletonList(Arrays.asList("a", "x,yy")));
                    }
                    for (String sql : new String[]{
                            "SELECT MIN(a.e)OVER(),MAX(a.s)OVER() FROM t a JOIN (SELECT id FROM t ORDER BY id LIMIT 2)b ON a.id=b.id",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=1 OR id=2"}) {
                        query(connection, mode, sql, Types.VARCHAR, Types.CHAR, false,
                                Arrays.asList(Arrays.asList("", "x,yy"),Arrays.asList("", "x,yy")));
                    }
                    query(connection, mode, "SELECT MIN(a.e)OVER(),MAX(a.s)OVER() FROM t a LEFT JOIN (SELECT id FROM t ORDER BY id LIMIT 1)b ON a.id=b.id",
                            Types.VARCHAR, Types.CHAR, false, Arrays.asList(Arrays.asList("", "x,yy"),Arrays.asList("", "x,yy"),Arrays.asList("", "x,yy")));
                    query(connection,mode,"SELECT MIN(a.e)OVER(),MAX(a.s)OVER() FROM t a LEFT JOIN (SELECT id FROM t WHERE id=1)b ON a.id=b.id WHERE a.id=COALESCE(b.id,2)",
                            Types.VARCHAR,Types.CHAR,false,Arrays.asList(Arrays.asList("","x,yy"),Arrays.asList("","x,yy")));
                    query(connection,mode,"SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE NOT(id=1 OR FALSE)",
                            Types.VARCHAR,Types.CHAR,false,Arrays.asList(Arrays.asList("",""),Arrays.asList("","")));
                    query(connection,mode,"SELECT MIN(a.e)OVER(),MAX(a.s)OVER() FROM t a LEFT JOIN (SELECT id FROM t WHERE id=1)b ON a.id=b.id WHERE a.id=1",
                            Types.VARCHAR,Types.CHAR,false,Collections.singletonList(Arrays.asList("a","x,yy")));
                    for (String sql:new String[]{
                            "SELECT MIN(a.e)OVER(),MAX(a.s)OVER() FROM t a JOIN (SELECT id FROM t WHERE id=1)b ON a.id=b.id WHERE a.id=COALESCE(b.id,2)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=1 AND EXISTS(SELECT 1 FROM t b WHERE b.id=t.id)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE NOT(NOT(id=1))",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE NOT(id<>1)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE NOT(id<>1 AND NULL)",
                            "SELECT MIN(a.e)OVER(),MAX(a.s)OVER() FROM t a LEFT JOIN (SELECT id FROM t WHERE id=1)b ON a.id=b.id WHERE a.id=b.id",
                            "SELECT MIN(a.e)OVER(),MAX(a.s)OVER() FROM t a LEFT JOIN (SELECT id FROM t WHERE id=1)b ON a.id=b.id WHERE b.id IS NOT NULL AND a.id=COALESCE(b.id,2)",
                            "SELECT MIN(a.e)OVER(),MAX(a.s)OVER() FROM t a LEFT JOIN (SELECT id FROM t WHERE id=1)b ON a.id=b.id WHERE a.id=COALESCE(b.id,2) AND LENGTH(b.id)>0"}) {
                        query(connection,mode,sql,constantWindowType,Types.CHAR,false,Collections.singletonList(Arrays.asList("a","x,yy")));
                    }
                    for (String sql:new String[]{
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE NOT(id=1 OR NULL)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id BETWEEN 2.5 AND 1.5",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id BETWEEN -1.5 AND -2.5",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id BETWEEN 2.50 AND 1.500",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id BETWEEN 1.2 AND 1.1",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE 2<=id AND 1>=id",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id>2 AND id<1",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id>=1 AND id<1",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE NOT(id<2 OR id>1)",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id>=2.0 AND id<=1.0",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id>2.0 AND id<1.0",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id>=CAST(2.0 AS DECIMAL(10,1)) AND id<=1",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id>=2 AND id<=1.1",
                            "SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id>=2.2 AND id<=1.2"}) {
                        query(connection,mode,sql,constantWindowType,Types.CHAR,false,Collections.<List<String>>emptyList());
                    }
                    query(connection,mode,"SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id=1 AND EXISTS(SELECT 1 FROM t b WHERE b.id>=t.id)",
                            Types.VARCHAR,Types.CHAR,false,Collections.singletonList(Arrays.asList("a","x,yy")));
                    query(connection,mode,"SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE EXISTS(SELECT 1 FROM t b WHERE b.id=t.id)",
                            Types.VARCHAR,Types.CHAR,false,Arrays.asList(Arrays.asList("","x,yy"),Arrays.asList("","x,yy"),Arrays.asList("","x,yy")));
                    query(connection,mode,"SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id BETWEEN 1.5 AND 2.5",
                            Types.VARCHAR,Types.CHAR,false,Collections.singletonList(Arrays.asList("","")));
                    query(connection,mode,"SELECT MIN(e)OVER(),MAX(s)OVER(),COUNT(*)OVER() FROM t WHERE id BETWEEN 1.5 AND 2.5",
                            Types.VARCHAR,Types.CHAR,false,Collections.singletonList(Arrays.asList("","","1")),3);
                    query(connection,mode,"SELECT MIN(a.e)OVER(),MAX(a.s)OVER() FROM t a LEFT JOIN (SELECT id FROM t WHERE id=1)b ON TRUE WHERE a.id=COALESCE(b.id,2)",
                            Types.VARCHAR,Types.CHAR,false,Collections.singletonList(Arrays.asList("a","x,yy")));
                    query(connection,mode,"SELECT MIN(a.e)OVER(),MAX(a.s)OVER() FROM t a LEFT JOIN (SELECT id FROM t WHERE id=100)b ON TRUE WHERE a.id=COALESCE(b.id,2)",
                            Types.VARCHAR,Types.CHAR,false,Collections.singletonList(Arrays.asList("","")));
                    for (String predicate:new String[]{"id>=1.2 AND id<=1.1","1.2<=id AND 1.1>=id","id>1.2 AND id<1.1",
                            "id>1 AND id<2","id>2.5 AND id<2.5","NOT(id<1.2 OR id>1.1)"}) {
                        query(connection,mode,"SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE "+predicate,
                                Types.VARCHAR,Types.CHAR,false,Collections.<List<String>>emptyList());
                    }
                    query(connection,mode,"SELECT MIN(a.e)OVER(),MAX(a.s)OVER() FROM t a LEFT JOIN (SELECT id FROM t WHERE id=1)b ON a.id=b.id WHERE a.id=COALESCE(b.id,2) AND (b.id IS NULL)+1>0",
                            Types.VARCHAR,Types.CHAR,false,Arrays.asList(Arrays.asList("","x,yy"),Arrays.asList("","x,yy")));
                    query(connection,mode,"SELECT MIN(a.e)OVER(),MAX(a.s)OVER() FROM t a LEFT JOIN (SELECT id FROM t WHERE id=1)b ON TRUE WHERE a.id=(b.id IS NULL)+1",
                            Types.VARCHAR,Types.CHAR,false,Collections.singletonList(Arrays.asList("a","x,yy")));
                    String unindexedRange="SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE n BETWEEN 2 AND 1";
                    query(connection,mode,unindexedRange,Types.VARCHAR,Types.CHAR,false,Collections.<List<String>>emptyList());
                    setup.execute("ALTER TABLE t ADD INDEX range_n(n)");
                    try {
                        query(connection,mode,unindexedRange,constantWindowType,Types.CHAR,false,Collections.<List<String>>emptyList());
                        query(connection,mode,"SELECT MIN(e)OVER(),MAX(s)OVER() FROM t IGNORE INDEX(range_n) WHERE n BETWEEN 2 AND 1",
                                Types.VARCHAR,Types.CHAR,false,Collections.<List<String>>emptyList());
                        setup.execute("ALTER TABLE t ALTER INDEX range_n INVISIBLE");
                        query(connection,mode,unindexedRange,Types.VARCHAR,Types.CHAR,false,Collections.<List<String>>emptyList());
                        setup.execute("ALTER TABLE t ALTER INDEX range_n VISIBLE");
                        query(connection,mode,unindexedRange,constantWindowType,Types.CHAR,false,Collections.<List<String>>emptyList());
                    } finally {
                        setup.execute("ALTER TABLE t DROP INDEX range_n");
                    }
                    query(connection,mode,unindexedRange,Types.VARCHAR,Types.CHAR,false,Collections.<List<String>>emptyList());
                    if (mode.equals("statement")) sqlDecimalRanges(connection);
                    if (mode.equals("server-prepared") || mode.equals("cursor")) {
                        String numericSql="SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id BETWEEN ? AND ?";
                        String[][] numericRanges={{"2","1"},{"1","2"},{"1.2","1.1"},{null,"1"},{"1",null},{"1","2"},{"2.5","1.5"},{"1","2"}};
                        for (boolean strings:new boolean[]{true,false}) {
                            for (String[] range:numericRanges) try (PreparedStatement fresh=connection.prepareStatement(numericSql)) {
                                fresh.setQueryTimeout(30);if(mode.equals("cursor"))fresh.setFetchSize(1);
                                check(fresh.getClass().getName().equals("com.mysql.cj.jdbc.ServerPreparedStatement"),"numeric range prepare fell back");
                                stringDoubleRange(fresh,range,strings,mode.equals("cursor"));
                            }
                            try (PreparedStatement repeated=connection.prepareStatement(numericSql)) {
                                repeated.setQueryTimeout(30);if(mode.equals("cursor"))repeated.setFetchSize(1);
                                for (String[] range:numericRanges) {
                                    metadata(repeated.getMetaData(),Types.CHAR,false,"numeric prepare cache");
                                    stringDoubleRange(repeated,range,strings,mode.equals("cursor"));
                                }
                                metadata(repeated.getMetaData(),Types.CHAR,false,"numeric execute must not taint prepare cache");
                            }
                        }
                        String[][] decimalRanges={{"2.5","1.5"},{"1.5","2.5"},{"2.50","1.500"},{null,"2.5"},{"1.5",null},{"1.5","2.5"},{"2.5","1.5"}};
                        String decimalSql="SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id BETWEEN ? AND ?";
                        for (String[] range:decimalRanges) {
                            try (PreparedStatement fresh=connection.prepareStatement(decimalSql)) {
                                fresh.setQueryTimeout(30);if(mode.equals("cursor"))fresh.setFetchSize(1);
                                check(fresh.getClass().getName().equals("com.mysql.cj.jdbc.ServerPreparedStatement"),"decimal range prepare fell back");
                                decimalRangeExecution(fresh,range,mode.equals("cursor"));
                            }
                        }
                        try (PreparedStatement repeated=connection.prepareStatement(decimalSql)) {
                            repeated.setQueryTimeout(30);if(mode.equals("cursor"))repeated.setFetchSize(1);
                            for(String[] range:decimalRanges) {
                                decimalRangeExecution(repeated,range,mode.equals("cursor"));
                                metadata(repeated.getMetaData(),Types.CHAR,false,"decimal prepare cache");
                            }
                        }
                        Integer[][] ranges={{2,1},{null,1},{1,null},{1,2},{1,1},{2,1},{1,2},{null,null},{1,2}};
                        String sql="SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE id BETWEEN ? AND ?";
                        for (Integer[] range:ranges) {
                            try (PreparedStatement fresh=connection.prepareStatement(sql)) {
                                fresh.setQueryTimeout(30);
                                if (mode.equals("cursor")) fresh.setFetchSize(1);
                                check(fresh.getClass().getName().equals("com.mysql.cj.jdbc.ServerPreparedStatement"),"range prepare fell back");
                                metadata(fresh.getMetaData(),Types.CHAR,false,"fresh range prepare");
                                rangeExecution(fresh,range,mode.equals("cursor"));
                            }
                        }
                        try (PreparedStatement repeated=connection.prepareStatement(sql)) {
                            repeated.setQueryTimeout(30);
                            if (mode.equals("cursor")) repeated.setFetchSize(1);
                            for (Integer[] range:ranges) {
                                rangeExecution(repeated,range,mode.equals("cursor"));
                                metadata(repeated.getMetaData(),Types.CHAR,false,"range prepare cache after execution");
                            }
                        }
                        for (String key : new String[]{"id", "u"}) {
                            try (PreparedStatement keyed = connection.prepareStatement("SELECT MIN(e)OVER(),MAX(s)OVER() FROM t WHERE " + key + "<=>?")) {
                                keyed.setQueryTimeout(30);
                                if (mode.equals("cursor")) keyed.setFetchSize(1);
                                check(keyed.getClass().getName().equals("com.mysql.cj.jdbc.ServerPreparedStatement"), "key prepare fell back");
                                metadata(keyed.getMetaData(), Types.CHAR, false, "key prepare");
                                for (int value : new int[]{1,2,1}) {
                                    keyed.setInt(1,value);
                                    try (ResultSet result=keyed.executeQuery()) {
                                        List<List<String>> wanted=value==1 ? Collections.singletonList(Arrays.asList("a", "x,yy"))
                                                : key.equals("id") ? Collections.singletonList(Arrays.asList("", "")) : Collections.<List<String>>emptyList();
                                        rows(result, constantWindowType, false, wanted, "key " + key + "=" + value);
                                        labelWidths(result.getMetaData());
                                    }
                                }
                            }
                        }
                        try (PreparedStatement nested = connection.prepareStatement("SELECT MIN(e) OVER(),MAX(s) OVER() FROM "
                                + "(SELECT e,s,ROW_NUMBER() OVER(ORDER BY id)n FROM t ORDER BY id LIMIT ?)d WHERE n>0")) {
                            nested.setQueryTimeout(30);
                            if (mode.equals("cursor")) nested.setFetchSize(1);
                            check(nested.getClass().getName().equals("com.mysql.cj.jdbc.ServerPreparedStatement"), "nested prepare fell back");
                            metadata(nested.getMetaData(), Types.CHAR, false, "nested prepare");
                            for (int count : new int[]{1,2,0,1}) {
                                nested.setInt(1,count);
                                try (ResultSet result = nested.executeQuery()) {
                                    int expectedType = count<=1 && !mode.equals("cursor") ? Types.CHAR : Types.VARCHAR;
                                    List<List<String>> wanted = count==0 ? Collections.<List<String>>emptyList()
                                            : count==1 ? Collections.singletonList(Arrays.asList("a", "x,yy"))
                                            : Arrays.asList(Arrays.asList("", "x,yy"),Arrays.asList("", "x,yy"));
                                    labelWidths(result.getMetaData());
                                    rows(result, expectedType, false, wanted, "nested LIMIT " + count);
                                }
                            }
                        }
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
