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
            if (declared) {
                check(meta.getPrecision(i) == (i == 1 ? 2 : 4), phase + ": wrong declared label width");
            }
        }
    }

    private static void rows(ResultSet result, int type, boolean declared, int copies, String phase)
            throws SQLException {
        metadata(result.getMetaData(), type, declared, phase);
        Map<List<String>, Integer> actual = new HashMap<>();
        while (result.next()) {
            actual.merge(Arrays.asList(result.getString(1), result.getString(2)), 1, Integer::sum);
        }
        Map<List<String>, Integer> expected = new HashMap<>();
        if (copies > 0) {
            expected.put(Arrays.asList("a", "x,yy"), copies);
            expected.put(Arrays.asList("", ""), copies);
            expected.put(Arrays.asList(null, null), copies);
        }
        check(actual.equals(expected), phase + ": expected " + expected + ", got " + actual);
    }

    private static void query(Connection connection, String mode, String sql, int type,
                              boolean declared, int copies) throws Exception {
        if (mode.equals("statement")) {
            try (Statement statement = connection.createStatement()) {
                statement.setQueryTimeout(30);
                try (ResultSet result = statement.executeQuery(sql)) {
                    rows(result, type, declared, copies, mode + ": " + sql);
                }
            }
        } else {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setQueryTimeout(30);
                if (mode.equals("server-prepared")) {
                    check(statement.getClass().getName().equals("com.mysql.cj.jdbc.ServerPreparedStatement"),
                            "server prepare silently fell back");
                }
                metadata(statement.getMetaData(), type, declared, mode + " prepare: " + sql);
                for (int i = 0; i < 2; i++) {
                    try (ResultSet result = statement.executeQuery()) {
                        rows(result, type, declared, copies, mode + " execute: " + sql);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("JDBC_URL USER PASSWORD");
        for (String mode : new String[]{"statement", "client-prepared", "server-prepared"}) {
            Properties properties = new Properties();
            properties.setProperty("user", args[1]);
            properties.setProperty("password", args[2]);
            properties.setProperty("useServerPrepStmts", String.valueOf(mode.equals("server-prepared")));
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
                    setup.execute("CREATE TABLE t (e ENUM('a','bb',''), s SET('x','yy'))");
                    setup.execute("INSERT INTO t VALUES ('a','x,yy'),('',''),(NULL,NULL)");
                    for (String sql : new String[]{
                            "SELECT e, s FROM t",
                            "SELECT e AS alias_e, s AS alias_s FROM t",
                            "SELECT DISTINCT e, s FROM t",
                            "SELECT e, s FROM t GROUP BY e, s",
                            "SELECT e, s FROM (SELECT e, s FROM t LIMIT 3) d",
                            "WITH c AS (SELECT e, s FROM t) SELECT e, s FROM c"}) {
                        query(connection, mode, sql, Types.CHAR, true, 1);
                    }
                    query(connection, mode, "SELECT e, s FROM t WHERE 1=0", Types.CHAR, true, 0);
                    setup.execute("CREATE VIEW v AS SELECT e, s FROM t");
                    query(connection, mode, "SELECT e, s FROM v", Types.CHAR, true, 1);
                    query(connection, mode, "SELECT CONCAT(e), CAST(s AS CHAR) FROM t", Types.VARCHAR, false, 1);
                    query(connection, mode, "SELECT e, s FROM t UNION ALL SELECT e, s FROM t", Types.VARCHAR, false, 2);
                } finally {
                    setup.execute("DROP DATABASE " + db);
                }
                System.out.println("PASS " + mode);
            }
        }
    }
}
