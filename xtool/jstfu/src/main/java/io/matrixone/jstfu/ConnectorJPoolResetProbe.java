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

import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;

import javax.sql.PooledConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Black-box Connector/J pool-borrow regression probe. It is launched by the
 * MatrixOne datastream E2E test against a real server; keeping it in the
 * existing Java fixture makes the exact client version and test command part
 * of the checked-in repository rather than an external manual recipe.
 */
public final class ConnectorJPoolResetProbe {
    private static final String TEMP_TABLE = "mo_pool_reset_temp";
    private static final String PREPARED_NAME = "mo_pool_reset_stmt";
    private static final String USER_VARIABLE = "@mo_pool_reset_var";

    private ConnectorJPoolResetProbe() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            throw new IllegalArgumentException("usage: <jdbc-url> <user> <password> <database> <table> <connectorj-version>");
        }
        String url = args[0];
        String user = args[1];
        String password = args[2];
        String database = args[3];
        String table = args[4];
        String connectorJVersion = args[5];

        MysqlConnectionPoolDataSource dataSource = new MysqlConnectionPoolDataSource();
        dataSource.setURL(url);
        PooledConnection pooled = dataSource.getPooledConnection(user, password);
        try {
            long connectionId;
            String defaultSqlMode;
            try (Connection first = pooled.getConnection()) {
                connectionId = longValue(first, "SELECT CONNECTION_ID()");
                requireEquals(database, stringValue(first, "SELECT DATABASE()"), "first borrow database");
                defaultSqlMode = stringValue(first, "SELECT @@session.sql_mode");

                first.setAutoCommit(false);
                try (Statement statement = first.createStatement()) {
                    statement.executeUpdate("INSERT INTO " + table + " VALUES (1)");
                    statement.execute("SET " + USER_VARIABLE + " = 'must_not_leak'");
                    statement.execute("CREATE TEMPORARY TABLE " + TEMP_TABLE + " (v INT)");
                    statement.execute("SET @mo_pool_reset_sql = 'SELECT 1'");
                    statement.execute("PREPARE " + PREPARED_NAME + " FROM @mo_pool_reset_sql");
                    statement.execute("SET SESSION sql_mode = 'ANSI'");
                }
                try (PreparedStatement statement = first.prepareStatement("SELECT 1")) {
                    statement.executeQuery();
                }
            }

            try (Connection second = pooled.getConnection()) {
                requireEquals(Long.valueOf(connectionId), Long.valueOf(longValue(second, "SELECT CONNECTION_ID()")),
                        "physical connection ID");
                requireEquals(database, stringValue(second, "SELECT DATABASE()"), "reset database");
                String serverAutocommit = stringValue(second, "SELECT @@session.autocommit");
                requireEquals("1", serverAutocommit,
                        "server autocommit was not restored");
                // Connector/J 8.0.15 retains a driver-local autoCommit flag
                // across its COM_CHANGE_USER fallback even against native
                // MySQL. The server-side state is the compatibility contract
                // for that version; newer COM_RESET_CONNECTION drivers must
                // expose the same clean state through their client API too.
                if (!"8.0.15".equals(connectorJVersion)) {
                    require(second.getAutoCommit(), "client autocommit was not restored; server reports " + serverAutocommit);
                }
                requireEquals(Long.valueOf(0), Long.valueOf(longValue(second, "SELECT COUNT(*) FROM " + table)),
                        "uncommitted transaction leaked");
                requireEquals(null, objectValue(second, "SELECT " + USER_VARIABLE), "user variable leaked");
                requireEquals(defaultSqlMode, stringValue(second, "SELECT @@session.sql_mode"), "session default leaked");
                requireStatementFails(second, "SELECT * FROM " + TEMP_TABLE, "temporary table leaked");
                requireStatementFails(second, "EXECUTE " + PREPARED_NAME, "server prepared statement leaked");
            }
        } finally {
            pooled.close();
        }

        verifyReplacementAfterResetFailure(dataSource, url, user, password, database);
    }

    private static void verifyReplacementAfterResetFailure(
            MysqlConnectionPoolDataSource dataSource,
            String url,
            String user,
            String password,
            String database) throws SQLException {
        PooledConnection failedPool = dataSource.getPooledConnection(user, password);
        long failedConnectionId;
        boolean resetFailed = false;
        try {
            try (Connection connection = failedPool.getConnection()) {
                failedConnectionId = longValue(connection, "SELECT CONNECTION_ID()");
            }

            try (Connection control = DriverManager.getConnection(url, user, password);
                 Statement statement = control.createStatement()) {
                statement.execute("KILL CONNECTION " + failedConnectionId);
            }

            try {
                failedPool.getConnection();
            } catch (SQLException expected) {
                // A failed server-side reset must make this pooled physical
                // connection unusable rather than exposing a stale generation.
                resetFailed = true;
            }
        } finally {
            failedPool.close();
        }

        require(resetFailed, "pooled connection remained borrowable after reset failure");

        PooledConnection replacementPool = dataSource.getPooledConnection(user, password);
        try {
            try (Connection replacement = replacementPool.getConnection()) {
                requireEquals(database, stringValue(replacement, "SELECT DATABASE()"),
                        "replacement borrow database");
                requireEquals(Long.valueOf(1), Long.valueOf(longValue(replacement, "SELECT 1")),
                        "replacement borrow query");
            }
        } finally {
            replacementPool.close();
        }
    }

    private static long longValue(Connection connection, String sql) throws SQLException {
        Object value = objectValue(connection, sql);
        if (!(value instanceof Number)) {
            throw new AssertionError("expected numeric result for " + sql + ", got " + value);
        }
        return ((Number) value).longValue();
    }

    private static String stringValue(Connection connection, String sql) throws SQLException {
        Object value = objectValue(connection, sql);
        return value == null ? null : value.toString();
    }

    private static Object objectValue(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet result = statement.executeQuery(sql)) {
            if (!result.next()) {
                throw new AssertionError("expected one result for " + sql);
            }
            return result.getObject(1);
        }
    }

    private static void requireStatementFails(Connection connection, String sql, String message) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException expected) {
            return;
        }
        throw new AssertionError(message);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void requireEquals(Object expected, Object actual, String message) {
        if (expected == null ? actual != null : !expected.equals(actual)) {
            throw new AssertionError(message + ": expected=" + expected + ", actual=" + actual);
        }
    }
}
