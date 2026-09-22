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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Acceptance coverage for #29184.  Unlike the unit tests, this suite proves
 * that a real Connector/J connection observes LAST_INSERT_ID() after Proxy
 * moves it from a draining CN to the other CN.
 *
 * <p>The suite is skipped unless MO_PROXY_TEMP_TABLE_E2E_URL points at a
 * two-CN Proxy instance.  It is intentionally kept separate from the
 * temporary-table migration suite so a LAST_INSERT_ID failure is attributable
 * to the public session-state contract.</p>
 */
class ProxyLastInsertIdMigrationE2ETest {

    private static final Duration MIGRATION_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration WORK_STATE_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration RETRY_OBSERVATION_WINDOW = Duration.ofSeconds(6);
    private static final String DEFAULT_CN1 = "dd1dccb4-4d3c-41f8-b482-5251dc7a41bf";
    private static final String DEFAULT_CN2 = "dd1dccb5-4d3c-41f8-b482-5251dc7a41bf";

    private String url;
    private String cn1;
    private String cn2;

    @AfterEach
    void restoreCNWorkStates() throws SQLException {
        if (url == null || url.isEmpty()) {
            return;
        }
        try (Connection admin = connect(false)) {
            setWorkState(admin, cn1, 1);
            setWorkState(admin, cn2, 1);
        }
    }

    @Test
    void preservesLastInsertIdAfterHandoffWithClientPreparedStatements() throws Exception {
        assertLastInsertIdAfterHandoff(false);
    }

    @Test
    void preservesLastInsertIdAfterHandoffWithServerPreparedStatements() throws Exception {
        assertLastInsertIdAfterHandoff(true);
    }

    private void assertLastInsertIdAfterHandoff(boolean serverPrepared) throws Exception {
        enableTopology();
        String database = "proxy_lid_migration_" + UUID.randomUUID().toString().replace('-', '_');
        String firstPayload = "first_" + UUID.randomUUID().toString().replace('-', '_');
        String secondPayload = "second_" + UUID.randomUUID().toString().replace('-', '_');

        try (Connection admin = connect(false); Connection client = connect(serverPrepared)) {
            exec(admin, "create database " + database);
            try {
                exec(client, "use " + database);
                exec(client, "create table lid (id bigint auto_increment primary key, "
                        + "payload varchar(128) not null, unique key uk_payload(payload))");

                // Keep both prepared statements alive across the handoff.  The
                // connection itself is also retained and reconnect is disabled.
                try (PreparedStatement insert = client.prepareStatement(
                        "insert into lid (payload) values (?)");
                        PreparedStatement lookup = client.prepareStatement(
                                "select id from lid where payload = ?")) {
                    insert.setString(1, firstPayload);
                    assertEquals(1, insert.executeUpdate());
                    long firstID = queryLong(client,
                            "select id from lid where payload = '" + firstPayload + "'");
                    assertTrue(firstID > 0, "the generated id must be non-zero");
                    assertEquals(firstID, queryLong(client, "select last_insert_id()"));
                    assertEquals(firstID, queryLong(lookup, firstPayload));

                    String source = queryString(client, "select @@server_id");
                    String expectedTarget = otherCN(source);
                    setWorkState(admin, source, 2);
                    waitForWorkState(admin, source, "Draining");
                    String target = waitForMigration(client, source);
                    assertEquals(expectedTarget, target);

                    assertEquals(firstID, queryLong(client, "select last_insert_id()"));
                    assertEquals(firstID, queryLong(lookup, firstPayload));

                    // Observe a complete Proxy retry interval so a successful
                    // handoff cannot hide a second migration/replay attempt.
                    Instant deadline = Instant.now().plus(RETRY_OBSERVATION_WINDOW);
                    while (Instant.now().isBefore(deadline)) {
                        assertEquals(target, queryString(client, "select @@server_id"));
                        assertEquals(firstID, queryLong(client, "select last_insert_id()"));
                        assertEquals(firstID, queryLong(lookup, firstPayload));
                        Thread.sleep(100);
                    }

                    insert.setString(1, secondPayload);
                    assertEquals(1, insert.executeUpdate());
                    long secondID = queryLong(client,
                            "select id from lid where payload = '" + secondPayload + "'");
                    assertNotEquals(firstID, secondID);
                    assertEquals(secondID, queryLong(client, "select last_insert_id()"));
                }
            } finally {
                exec(admin, "drop database if exists " + database);
            }
        }
    }

    private void enableTopology() {
        String configuredURL = System.getenv("MO_PROXY_TEMP_TABLE_E2E_URL");
        if (configuredURL == null || configuredURL.isEmpty()) {
            url = null;
            Assumptions.assumeTrue(false,
                    "set MO_PROXY_TEMP_TABLE_E2E_URL to run the two-CN Proxy acceptance suite");
        }
        // The test creates a fresh database for each method.  Strip any
        // database component from the shared local-dev URL so the admin
        // connection does not depend on a pre-existing schema such as test.
        url = withoutDatabase(configuredURL);
        cn1 = environmentOrDefault("MO_PROXY_TEMP_TABLE_E2E_CN1", DEFAULT_CN1);
        cn2 = environmentOrDefault("MO_PROXY_TEMP_TABLE_E2E_CN2", DEFAULT_CN2);
    }

    private static String withoutDatabase(String configuredURL) {
        int authorityEnd = configuredURL.indexOf('/', "jdbc:mysql://".length());
        if (authorityEnd < 0) {
            return configuredURL + "/";
        }
        int queryStart = configuredURL.indexOf('?', authorityEnd);
        return queryStart < 0
                ? configuredURL.substring(0, authorityEnd + 1)
                : configuredURL.substring(0, authorityEnd + 1) + configuredURL.substring(queryStart);
    }

    private Connection connect(boolean serverPrepared) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("useServerPrepStmts", Boolean.toString(serverPrepared));
        properties.setProperty("autoReconnect", "false");
        properties.setProperty("reconnectAtTxEnd", "false");
        properties.setProperty("useSSL", "false");
        properties.setProperty("allowPublicKeyRetrieval", "true");
        return DriverManager.getConnection(url, properties);
    }

    private String otherCN(String source) {
        if (cn1.equals(source)) {
            return cn2;
        }
        if (cn2.equals(source)) {
            return cn1;
        }
        throw new AssertionError("Proxy selected an unknown source CN: " + source);
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

    private static long queryLong(Connection conn, String sql) throws SQLException {
        try (Statement statement = conn.createStatement(); ResultSet result = statement.executeQuery(sql)) {
            if (!result.next()) {
                throw new AssertionError("query returned no row: " + sql);
            }
            return result.getLong(1);
        }
    }

    private static long queryLong(PreparedStatement statement, String value) throws SQLException {
        statement.setString(1, value);
        try (ResultSet result = statement.executeQuery()) {
            if (!result.next()) {
                throw new AssertionError("prepared lookup returned no row for: " + value);
            }
            return result.getLong(1);
        }
    }

    private static String queryString(Connection conn, String sql) throws SQLException {
        try (Statement statement = conn.createStatement(); ResultSet result = statement.executeQuery(sql)) {
            if (!result.next()) {
                throw new AssertionError("query returned no row: " + sql);
            }
            return result.getString(1);
        }
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

    private static String waitForMigration(Connection client, String source)
            throws SQLException, InterruptedException {
        Instant deadline = Instant.now().plus(MIGRATION_TIMEOUT);
        String current = source;
        while (Instant.now().isBefore(deadline)) {
            current = queryString(client, "select @@server_id");
            if (!source.equals(current)) {
                return current;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Proxy did not migrate the connection away from draining CN " + source
                + "; last observed CN=" + current);
    }
}
