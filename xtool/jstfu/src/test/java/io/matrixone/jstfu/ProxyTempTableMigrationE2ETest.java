// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.matrixone.jstfu;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Acceptance coverage for #27602. It intentionally uses Connector/J rather
 * than a callback or a mock: {@link PreparedStatement} drives COM_STMT on the
 * same Proxy connection that owns the temporary table.
 *
 * <p>The normal jstfu unit-test environment has no MatrixOne cluster, so this
 * suite is enabled only by {@code MO_PROXY_TEMP_TABLE_E2E_URL}. A local run
 * against {@code mo-service -with-proxy -launch etc/launch-with-proxy/launch.toml}
 * additionally supplies the two CN UUIDs
 * with {@code MO_PROXY_TEMP_TABLE_E2E_CN1} and
 * {@code MO_PROXY_TEMP_TABLE_E2E_CN2}. The cleanup restores both CNs to
 * Working, making each execution independent.</p>
 */
class ProxyTempTableMigrationE2ETest {

    private static final Duration MIGRATION_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration WORK_STATE_TIMEOUT = Duration.ofSeconds(60);
    // Proxy retries a failed handoff every five seconds. Observe a complete
    // interval after success so this acceptance test catches a second attempt.
    private static final Duration RETRY_OBSERVATION_WINDOW = Duration.ofSeconds(6);
    private static final String DEFAULT_CN1 = "dd1dccb4-4d3c-41f8-b482-5251dc7a41bf";
    private static final String DEFAULT_CN2 = "dd1dccb5-4d3c-41f8-b482-5251dc7a41bf";

    private String url;
    private String cn1;
    private String cn2;

    @AfterEach
    void restoreCNWorkStates() throws SQLException {
        if (url == null) {
            return;
        }
        try (Connection admin = connect()) {
            setWorkState(admin, cn1, 1);
            setWorkState(admin, cn2, 1);
        }
    }

    @Test
    void preservesIndexedTemporaryTableAndPreparedStatementsAfterOneHandoff() throws Exception {
        enableTopology();
        String database = "proxy_temp_migration_" + UUID.randomUUID().toString().replace('-', '_');
        try (Connection admin = connect(); Connection client = connect()) {
            exec(admin, "create database " + database);
            try {
                exec(client, "use " + database);
                exec(client, "create temporary table tmp (id int primary key, grp int, v varchar(20), key idx_grp(grp))");
                exec(client, "insert into tmp values (1, 1, 'beta'), (2, 1, 'alpha'), (3, 2, 'gamma')");
                assertEquals("beta|alpha", queryString(client,
                        "select group_concat(v order by id separator '|') from tmp where grp = 1"));

                exec(client, "prepare sql_temp_stmt from 'select v from tmp where id = ?'");
                exec(client, "set @temp_id = 2");
                assertEquals("alpha", queryString(client, "execute sql_temp_stmt using @temp_id"));

                // Connector/J uses COM_STMT_PREPARE/COM_STMT_EXECUTE here.
                try (PreparedStatement binary = client.prepareStatement("select v from tmp where id = ?")) {
                    binary.setInt(1, 1);
                    assertEquals("beta", queryString(binary));

                    String source = serverID(client);
                    setWorkState(admin, source, 2);
                    waitForWorkState(admin, source, "Draining");
                    String target = waitForMigration(client, source);
                    assertNotEquals(source, target);

                    assertEquals("beta|alpha", queryString(client,
                            "select group_concat(v order by id separator '|') from tmp where grp = 1"));
                    assertIndexExists(client, "tmp", "idx_grp");
                    assertEquals("alpha", queryString(client, "execute sql_temp_stmt using @temp_id"));
                    binary.setInt(1, 1);
                    assertEquals("beta", queryString(binary));

                    // A completed migration must stay on the new CN for a full
                    // Proxy retry interval instead of re-entering its failed-
                    // migration loop.
                    Instant deadline = Instant.now().plus(RETRY_OBSERVATION_WINDOW);
                    while (Instant.now().isBefore(deadline)) {
                        assertEquals(target, serverID(client));
                        binary.setInt(1, 3);
                        assertEquals("gamma", queryString(binary));
                        Thread.sleep(100);
                    }
                }
            } finally {
                exec(admin, "drop database if exists " + database);
            }
        }
    }

    @Test
    void waitsForCommitBeforeMigrating() throws Exception {
        assertMigrationAfterTransactionBoundary("commit");
    }

    @Test
    void waitsForRollbackBeforeMigrating() throws Exception {
        assertMigrationAfterTransactionBoundary("rollback");
    }

    private void assertMigrationAfterTransactionBoundary(String boundary) throws Exception {
        enableTopology();
        try (Connection admin = connect(); Connection client = connect()) {
            client.setAutoCommit(false);
            String source = serverID(client);
            setWorkState(admin, source, 2);
            waitForWorkState(admin, source, "Draining");

            // An active client transaction is not transferable. Stay beyond a
            // Proxy scaling interval so a skipped immediate handoff cannot
            // satisfy this assertion.
            assertRemainsOnSourceDuringTransaction(client, source);
            exec(client, boundary);
            // With autocommit disabled, every probe would immediately begin a
            // new transaction and correctly keep the handoff unsafe. Restore
            // autocommit after the explicit boundary before polling for the
            // asynchronous Proxy transfer.
            client.setAutoCommit(true);
            assertNotEquals(source, waitForMigration(client, source));
        }
    }

    private void enableTopology() {
        url = System.getenv("MO_PROXY_TEMP_TABLE_E2E_URL");
        Assumptions.assumeTrue(url != null && !url.isEmpty(),
                "set MO_PROXY_TEMP_TABLE_E2E_URL to run the two-CN Proxy acceptance suite");
        cn1 = environmentOrDefault("MO_PROXY_TEMP_TABLE_E2E_CN1", DEFAULT_CN1);
        cn2 = environmentOrDefault("MO_PROXY_TEMP_TABLE_E2E_CN2", DEFAULT_CN2);
    }

    private Connection connect() throws SQLException {
        Properties properties = new Properties();
        // Do not rely on a URL supplied by the runner to select the binary
        // protocol: this test's PreparedStatement must exercise COM_STMT.
        properties.setProperty("useServerPrepStmts", "true");
        return DriverManager.getConnection(url, properties);
    }

    private static void assertIndexExists(Connection conn, String table, String expectedIndex)
            throws SQLException {
        try (Statement statement = conn.createStatement();
                ResultSet result = statement.executeQuery("show index from " + table)) {
            ResultSetMetaData metadata = result.getMetaData();
            int keyNameColumn = -1;
            for (int column = 1; column <= metadata.getColumnCount(); column++) {
                if ("Key_name".equalsIgnoreCase(metadata.getColumnLabel(column))) {
                    keyNameColumn = column;
                    break;
                }
            }
            if (keyNameColumn == -1) {
                throw new AssertionError("SHOW INDEX did not return Key_name for " + table);
            }
            while (result.next()) {
                if (expectedIndex.equals(result.getString(keyNameColumn))) {
                    return;
                }
            }
        }
        throw new AssertionError("index " + expectedIndex + " is missing from " + table);
    }

    private static void assertRemainsOnSourceDuringTransaction(Connection client, String source)
            throws SQLException, InterruptedException {
        Instant deadline = Instant.now().plus(RETRY_OBSERVATION_WINDOW);
        while (Instant.now().isBefore(deadline)) {
            assertEquals(source, serverID(client));
            Thread.sleep(100);
        }
    }

    private static String environmentOrDefault(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? fallback : value;
    }

    private static void exec(Connection conn, String sql) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            statement.execute(sql);
        }
    }

    private static String queryString(Connection conn, String sql) throws SQLException {
        try (Statement statement = conn.createStatement(); ResultSet result = statement.executeQuery(sql)) {
            result.next();
            return result.getString(1);
        }
    }

    private static String queryString(PreparedStatement statement) throws SQLException {
        try (ResultSet result = statement.executeQuery()) {
            result.next();
            return result.getString(1);
        }
    }

    private static String serverID(Connection conn) throws SQLException {
        return queryString(conn, "select @@server_id");
    }

    private static void setWorkState(Connection admin, String cnID, int state) throws SQLException {
        try (PreparedStatement statement = admin.prepareStatement(
                "select mo_ctl('cn', 'workstate', ?)")) {
            statement.setString(1, cnID + ":" + state);
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                String response = result.getString(1);
                if (!response.contains("\"result\": \"OK\"")) {
                    throw new AssertionError("workstate update was not accepted: " + response);
                }
            }
        }
    }

    private static void waitForWorkState(Connection admin, String cnID, String expected)
            throws SQLException, InterruptedException {
        Instant deadline = Instant.now().plus(WORK_STATE_TIMEOUT);
        while (Instant.now().isBefore(deadline)) {
            try (Statement statement = admin.createStatement();
                    ResultSet result = statement.executeQuery("show backend servers")) {
                ResultSetMetaData metadata = result.getMetaData();
                while (result.next()) {
                    boolean hasID = false;
                    boolean hasState = false;
                    for (int column = 1; column <= metadata.getColumnCount(); column++) {
                        String value = result.getString(column);
                        hasID |= cnID.equals(value);
                        hasState |= expected.equalsIgnoreCase(value);
                    }
                    if (hasID && hasState) {
                        return;
                    }
                }
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Proxy did not observe CN " + cnID + " in state " + expected);
    }

    private static String waitForMigration(Connection client, String source) throws SQLException, InterruptedException {
        Instant deadline = Instant.now().plus(MIGRATION_TIMEOUT);
        String current = source;
        while (Instant.now().isBefore(deadline)) {
            current = serverID(client);
            if (!source.equals(current)) {
                return current;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Proxy did not migrate the connection away from draining CN " + source
                + "; last observed CN=" + current);
    }
}
