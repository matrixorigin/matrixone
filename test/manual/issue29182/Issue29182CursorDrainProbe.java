// Manual two-CN/Proxy regression for https://github.com/matrixorigin/matrixone/issues/29182.
// Run against an isolated LOG/TN/CN1/CN2/Proxy cluster with Connector/J:
//   javac -cp mysql-connector-java-8.0.27.jar Issue29182CursorDrainProbe.java
//   java -cp .:mysql-connector-java-8.0.27.jar Issue29182CursorDrainProbe
// Supply proxy URL, CN1 URL, CN1 UUID and CN2 UUID as optional arguments.
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public final class Issue29182CursorDrainProbe {
    private static final int ROWS = 5000;

    private static void workState(Connection admin, String cn, int state) throws SQLException {
        try (PreparedStatement stmt = admin.prepareStatement("select mo_ctl('cn', 'WORKSTATE', ?)")) {
            stmt.setString(1, cn + ":" + state);
            try (ResultSet result = stmt.executeQuery()) {
                if (!result.next()) {
                    throw new SQLException("WORKSTATE returned no result for " + cn);
                }
                System.out.println("CN " + cn + " work state " + state + ": " + result.getString(1));
            }
        }
    }

    private static void awaitRebalance() throws InterruptedException {
        Thread.sleep(16000); // default Proxy refresh/rebalance intervals are 5s/10s
    }

    public static void main(String[] args) throws Exception {
        String proxyURL = args.length > 0 ? args[0] : "jdbc:mysql://127.0.0.1:6001/";
        String cn1URL = args.length > 1 ? args[1] : "jdbc:mysql://127.0.0.1:16001/";
        String cn1 = args.length > 2 ? args[2] : "dd1dccb4-4d3c-41f8-b482-5251dc7a41bf";
        String cn2 = args.length > 3 ? args[3] : "dd2dccb4-4d3c-41f8-b482-5251dc7a41be";
        String table = "issue_29182.cursor_probe_" + System.currentTimeMillis();
        Properties props = new Properties();
        props.setProperty("user", "dump");
        props.setProperty("password", "111");
        props.setProperty("useServerPrepStmts", "true");
        props.setProperty("useCursorFetch", "true");
        props.setProperty("defaultFetchSize", "13");
        Class.forName("com.mysql.cj.jdbc.Driver");

        try (Connection admin = DriverManager.getConnection(cn1URL, props)) {
            try (Statement setup = admin.createStatement()) {
                setup.execute("create database if not exists issue_29182");
                setup.execute("create table " + table + " (id bigint primary key)");
                for (int start = 1; start <= ROWS; start += 500) {
                    StringBuilder insert = new StringBuilder("insert into " + table + " values ");
                    for (int id = start; id < start + 500; id++) {
                        if (id > start) insert.append(',');
                        insert.append('(').append(id).append(')');
                    }
                    setup.execute(insert.toString());
                }
            }

            try {
                // Force the new Proxy connection onto CN2, then make CN1 the
                // destination. Keep the server cursor open through an actual
                // drain attempt before reading its final FETCH batch.
                workState(admin, cn1, 2); // Draining
                awaitRebalance();
                try (Connection client = DriverManager.getConnection(proxyURL, props);
                     PreparedStatement query = client.prepareStatement(
                             "select id from " + table + " order by id")) {
                    query.setFetchSize(13);
                    try (ResultSet rows = query.executeQuery()) {
                        if (!rows.next() || rows.getLong(1) != 1) {
                            throw new SQLException("first cursor row missing or out of order");
                        }
                        workState(admin, cn1, 1); // Working
                        workState(admin, cn2, 2); // Draining
                        awaitRebalance();
                        int count = 1;
                        while (rows.next()) {
                            count++;
                            if (rows.getLong(1) != count) {
                                throw new SQLException("lost, duplicate or reordered row at " + count);
                            }
                        }
                        if (count != ROWS) {
                            throw new SQLException("expected " + ROWS + " rows, got " + count);
                        }
                        System.out.println("ordered cursor rows=" + count + "; no loss or duplication");
                    }
                    // Wait for Proxy to retry the pending migration after the
                    // cursor has been disposed, then reuse this same socket.
                    awaitRebalance();
                    try (Statement check = client.createStatement();
                         ResultSet count = check.executeQuery("select count(*) from " + table)) {
                        if (!count.next() || count.getLong(1) != ROWS) {
                            throw new SQLException("same-connection query after drain failed");
                        }
                        System.out.println("same-connection count=" + count.getLong(1));
                    }
                }
            } finally {
                workState(admin, cn1, 1);
                workState(admin, cn2, 1);
                try (Statement cleanup = admin.createStatement()) {
                    cleanup.execute("drop table " + table);
                }
                System.out.println("restored CN states and dropped " + table);
            }
        }
    }
}
