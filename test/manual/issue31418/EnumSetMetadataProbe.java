import java.sql.*;
import java.util.*;
import com.mysql.cj.result.Field;
import com.mysql.cj.jdbc.result.ResultSetImpl;

/** Direct-field metadata through real Connector/J text, prepared and cursor paths. */
public final class EnumSetMetadataProbe {
    private static void check(boolean ok, String message) {
        if (!ok) throw new AssertionError(message);
    }

    private static String metadata(ResultSetMetaData meta, int[] types, boolean declared,
                                   boolean widths, String phase) throws SQLException {
        check(meta != null && meta.getColumnCount() == 2, phase + ": missing columns");
        Field[] fields = ((com.mysql.cj.jdbc.result.ResultSetMetaData) meta).getFields();
        StringBuilder snapshot = new StringBuilder();
        for (int i = 0; i < 2; i++) {
            check(meta.getColumnType(i + 1) == types[i], phase + ": column " + i
                    + " expected JDBC " + types[i] + ", got " + meta.getColumnType(i + 1));
            check("java.lang.String".equals(meta.getColumnClassName(i + 1)), phase + ": not String");
            int flags = fields[i].getFlags() & (0x100 | 0x800);
            check(flags == (declared ? (i == 0 ? 0x100 : 0x800) : 0), phase + ": wrong flags " + flags);
            check(fields[i].getMysqlTypeId() == (types[i] == Types.CHAR ? 254 : 253), phase + ": wire type");
            if (widths) {
                check(meta.getPrecision(i + 1) == i + 2, phase + ": label character width");
                check(fields[i].getLength() == (i + 2) * 4, phase + ": label byte width");
            }
            snapshot.append(fields[i].getMysqlTypeId()).append(':').append(fields[i].getLength())
                    .append(':').append(fields[i].getFlags()).append(':').append(fields[i].getCollationIndex()).append(';');
        }
        return snapshot.toString();
    }

    private static Map<List<String>, Integer> expected(int copies) {
        Map<List<String>, Integer> rows = new HashMap<>();
        if (copies > 0) {
            rows.put(Arrays.asList("甲乙", "x,😀"), copies);
            rows.put(Arrays.asList("", ""), copies);
            rows.put(Arrays.asList(null, null), copies);
        }
        return rows;
    }

    private static void rows(ResultSet result, int[] types, boolean declared, boolean widths,
                             int copies, boolean cursor, String phase) throws SQLException {
        metadata(result.getMetaData(), types, declared, widths, phase);
        if (cursor && copies > 0) {
            check(((ResultSetImpl) result).getRows() instanceof com.mysql.cj.protocol.a.result.ResultsetRowsCursor,
                    phase + ": cursor silently fell back");
        }
        Map<List<String>, Integer> actual = new HashMap<>();
        while (result.next()) actual.merge(Arrays.asList(result.getString(1), result.getString(2)), 1, Integer::sum);
        check(actual.equals(expected(copies)), phase + ": wrong values " + actual);
    }

    private static void query(Connection c, String mode, String sql, int[] types, boolean declared,
                              boolean widths, int copies) throws Exception {
        String phase = mode + ": " + sql;
        if (mode.equals("statement")) {
            try (Statement s = c.createStatement()) {
                s.setQueryTimeout(30);
                try (ResultSet rs = s.executeQuery(sql)) { rows(rs, types, declared, widths, copies, false, phase); }
            }
        } else {
            try (PreparedStatement s = c.prepareStatement(sql)) {
                s.setQueryTimeout(30);
                boolean cursor = mode.equals("cursor");
                if (!mode.equals("client-prepared")) {
                    check(s instanceof com.mysql.cj.jdbc.ServerPreparedStatement, phase + ": server prepare fallback");
                }
                if (cursor) s.setFetchSize(1);
                ResultSetMetaData cached = s.getMetaData();
                String before = metadata(cached, types, declared, widths, phase + " prepare");
                for (int run = 0; run < 3; run++) {
                    boolean empty = sql.contains("?") && run == 1;
                    if (sql.contains("?")) s.setInt(1, empty ? 4 : 1);
                    try (ResultSet rs = s.executeQuery()) {
                        rows(rs, types, declared, widths, empty ? 0 : copies, cursor, phase);
                    }
                    check(before.equals(metadata(cached, types, declared, widths, phase + " cached")), phase + ": cache changed");
                    check(before.equals(metadata(s.getMetaData(), types, declared, widths, phase + " prepare again")), phase + ": prepare changed");
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("JDBC_URL USER PASSWORD");
        int[] declaredTypes = {Types.CHAR, Types.CHAR};
        int[] strings = {Types.VARCHAR, Types.VARCHAR};
        for (String mode : new String[]{"statement", "client-prepared", "server-prepared", "cursor"}) {
            Properties p = new Properties();
            p.setProperty("user", args[1]);
            p.setProperty("password", args[2]);
            p.setProperty("useServerPrepStmts", String.valueOf(mode.equals("server-prepared") || mode.equals("cursor")));
            p.setProperty("useCursorFetch", String.valueOf(mode.equals("cursor")));
            p.setProperty("emulateUnsupportedPstmts", "false");
            p.setProperty("characterEncoding", "UTF-8");
            p.setProperty("socketTimeout", "30000");
            p.setProperty("useSSL", "false");
            p.setProperty("allowPublicKeyRetrieval", "true");
            String db = "claude_enum_rebuild_" + UUID.randomUUID().toString().replace("-", "");
            try (Connection c = DriverManager.getConnection(args[0], p); Statement setup = c.createStatement()) {
                setup.setQueryTimeout(30);
                setup.execute("CREATE DATABASE " + db);
                try {
                    setup.execute("USE " + db);
                    setup.execute("CREATE TABLE t (id INT PRIMARY KEY, e ENUM('a','甲乙',''), s SET('x','😀'), c CHAR(2), v VARCHAR(3))");
                    setup.execute("INSERT INTO t VALUES (1,'甲乙','x,😀','甲乙','x,😀'),(2,'','','',''),(3,NULL,NULL,NULL,NULL)");
                    setup.execute("CREATE VIEW v AS SELECT e,s FROM t");
                    for (String sql : new String[]{
                            "SELECT e,s FROM t", "SELECT e AS alias_e,s AS alias_s FROM t ORDER BY id",
                            "SELECT DISTINCT e,s FROM t", "SELECT e,s FROM t GROUP BY e,s",
                            "SELECT e,s FROM t GROUP BY id", "SELECT e,s FROM (SELECT e,s FROM t GROUP BY id) d",
                            "SELECT e,s FROM (SELECT e,s FROM t LIMIT 3) d",
                            "WITH d AS (SELECT e,s FROM t) SELECT e,s FROM d", "SELECT e,s FROM v",
                            "SELECT a.e,b.s FROM t a LEFT JOIN t b ON a.id=b.id"}) {
                        query(c, mode, sql, declaredTypes, true, true, 1);
                    }
                    query(c, mode, "SELECT e,s FROM t WHERE FALSE", declaredTypes, true, true, 0);
                    query(c, mode, "SELECT CONCAT(e),CAST(s AS CHAR) FROM t", strings, false, false, 1);
                    query(c, mode, "SELECT e,s FROM t UNION ALL SELECT e,s FROM t", strings, false, false, 2);
                    query(c, mode, "SELECT c,v FROM t", new int[]{Types.CHAR, Types.VARCHAR}, false, true, 1);
                    if (!mode.equals("statement")) query(c, mode, "SELECT e,s FROM t WHERE id>=?", declaredTypes, true, true, 1);
                } finally {
                    setup.execute("DROP DATABASE " + db);
                }
            }
            System.out.println("PASS " + mode);
        }
    }
}
