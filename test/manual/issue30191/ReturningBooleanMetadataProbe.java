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

/** DML RETURNING reads declared columns, independently of their assignments. */
public final class ReturningBooleanMetadataProbe {
    private static void check(boolean ok, String message) {
        if (!ok) throw new AssertionError(message);
    }

    private static void metadata(ResultSetMetaData md, int[] types, String phase) throws Exception {
        check(md != null && md.getColumnCount() == types.length, phase + ": missing/wrong metadata");
        for (int i = 0; i < types.length; i++) {
            check(md.getColumnType(i+1) == types[i], phase + ": column " + (i+1)
                    + " expected JDBC " + types[i] + ", got " + md.getColumnType(i+1));
            Class<?> cls = Class.forName(md.getColumnClassName(i+1));
            check(types[i] == Types.BIT ? cls == Boolean.class : Number.class.isAssignableFrom(cls),
                    phase + ": wrong class " + cls.getName());
        }
    }

    private static void rows(ResultSet result, int[] types, String[] expected) throws Exception {
        metadata(result.getMetaData(), types, "execute");
        Map<String,Integer> wanted = new HashMap<>();
        Map<String,Integer> actual = new HashMap<>();
        for (String row : expected) wanted.merge(row, 1, Integer::sum);
        while (result.next()) {
            StringBuilder row = new StringBuilder();
            for (int i = 0; i < types.length; i++) {
                Object value = result.getObject(i+1);
                check(value == null || (types[i] == Types.BIT ? value instanceof Boolean : value instanceof Number),
                        "wrong returned Java class at column " + (i+1));
                if (i > 0) row.append('|');
                row.append(value instanceof Number ? ((Number) value).longValue() : value);
            }
            actual.merge(row.toString(), 1, Integer::sum);
        }
        check(actual.equals(wanted), "expected " + wanted + ", got " + actual);
    }

    private static int cases(Connection connection, String table, String mode) throws Exception {
        String[] sql = {
            "INSERT INTO %s VALUES(1,TRUE,7) RETURNING flag",
            "INSERT INTO %s VALUES(1,NULL,NULL) RETURNING flag",
            "INSERT INTO %1$s SELECT 1,n>0,n FROM %1$s WHERE id=10 RETURNING flag,n>0",
            "UPDATE %s SET flag=FALSE WHERE id=10 RETURNING flag",
            "UPDATE %s SET flag=n>0 RETURNING flag,n>0",
            "UPDATE %s SET flag=flag RETURNING flag",
            "DELETE FROM %s WHERE id=10 RETURNING flag",
            "INSERT INTO %s VALUES(1,TRUE,7) RETURNING n>0,flag,n>0,flag",
            "UPDATE %s SET flag=FALSE WHERE id=10 RETURNING NOT flag,flag,TRUE",
            "UPDATE %s SET flag=NULL WHERE id=20 RETURNING flag,flag IS NULL",
            "DELETE FROM %s RETURNING n>0,flag",
            "SELECT flag,n>0 FROM %s",
            "INSERT INTO %s(id,n) VALUES(1,7) RETURNING flag",
            "INSERT INTO %s VALUES(1,TRUE,7) RETURNING *",
            "UPDATE %s AS b SET flag=FALSE WHERE id=10 RETURNING b.flag"
        };
        int b = Types.BIT, p = Types.TINYINT;
        int[][] types = {{b},{b},{b,p},{b},{b,p},{b},{b},{p,b,p,b},{p,b,p},
                {b,p},{p,b},{b,p},{b},{Types.INTEGER,b,Types.INTEGER},{b}};
        String[][] expected = {{"true"},{"null"},{"true|1"},{"false"},
                {"true|1","false|0","null|null"},{"true","false","null"},{"true"},
                {"1|true|1|true"},{"1|false|1"},{"null|1"},
                {"1|true","0|false","null|null"},{"true|1","false|0","null|null"},
                {"true"},{"1|true|7"},{"false"}};
        for (int c = 0; c < sql.length; c++) {
            String query = String.format(sql[c], table);
            try (Statement reset = connection.createStatement()) {
                reset.setQueryTimeout(30);
                reset.execute("DELETE FROM " + table);
                reset.execute("INSERT INTO " + table + " VALUES(10,TRUE,7),(20,FALSE,0),(30,NULL,NULL)");
            }
            try (Statement statement = mode.equals("statement") ? connection.createStatement()
                    : connection.prepareStatement(query)) {
                statement.setQueryTimeout(30);
                if (mode.equals("server-prepared")) {
                    check(statement.getClass().getName().equals("com.mysql.cj.jdbc.ServerPreparedStatement"),
                            "server prepare fell back");
                    metadata(((PreparedStatement) statement).getMetaData(), types[c], "prepare");
                }
                // Do not invoke a client-emulated metadata probe before a DML:
                // driver metadata discovery must not execute the mutation twice.
                boolean hasResult = statement instanceof PreparedStatement
                        ? ((PreparedStatement) statement).execute() : statement.execute(query);
                check(hasResult, "RETURNING did not produce a result set");
                try (ResultSet result = statement.getResultSet()) {
                    rows(result, types[c], expected[c]);
                }
            } catch (Exception | AssertionError error) {
                throw new AssertionError(mode + ": " + query, error);
            }
        }
        System.out.println("PASS " + mode + ": " + sql.length + " RETURNING/control cases");
        return sql.length;
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
        String database = "issue30191_returning_" + UUID.randomUUID().toString().replace("-", "");
        String table = database + ".bools";
        Class.forName("com.mysql.cj.jdbc.Driver");
        try (Connection setup = DriverManager.getConnection(url, properties);
             Statement statement = setup.createStatement()) {
            statement.setQueryTimeout(30);
            statement.execute("CREATE DATABASE " + database);
            try {
                statement.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, flag BOOLEAN DEFAULT TRUE, n INT)");
                int count = 0;
                for (String mode : new String[]{"statement", "client-prepared", "server-prepared"}) {
                    properties.setProperty("useServerPrepStmts", Boolean.toString(mode.equals("server-prepared")));
                    try (Connection connection = DriverManager.getConnection(url, properties)) {
                        count += cases(connection, table, mode);
                    }
                }
                System.out.println("PASS " + count + " RETURNING metadata/value cases; driver "
                        + setup.getMetaData().getDriverVersion());
            } finally {
                statement.execute("DROP DATABASE " + database);
            }
        }
    }
}
