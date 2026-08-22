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

package io.matrixone.jstfu.source;

import io.matrixone.jstfu.Config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;

/**
 * Reads rows over JDBC and streams them as CSV chunks.  The configured SQL
 * may contain a {@code ${FILTER}} placeholder that is replaced with the
 * request's pushed-down filter text ({@code 1=1} when the request has none).
 *
 * <p>Security note: the substitution is textual by design — the filter comes
 * from the MatrixOne scan operator, but the SQL runs with the configured
 * credentials. The server binds to loopback by default (see {@link Config})
 * so it is not reachable off-box out of the box; before binding it to a
 * routable interface, put an authenticating trust boundary in front, since a
 * direct client could inject arbitrary SQL through {@code ${FILTER}}.</p>
 */
public class JdbcSource implements DataSource {
    static final String FILTER_PLACEHOLDER = "${FILTER}";

    private final Config.DataSourceConfig config;
    private final int chunkSize;
    private final int queryTimeoutSeconds;

    public JdbcSource(Config.DataSourceConfig config, int chunkSize, int queryTimeoutSeconds) {
        this.config = config;
        this.chunkSize = chunkSize;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
    }

    static String substituteFilter(String sql, String filter) {
        String effective = (filter == null || filter.isEmpty()) ? "1=1" : filter;
        return sql.replace(FILTER_PLACEHOLDER, effective);
    }

    @Override
    public void stream(String filter, StreamContext ctx) throws Exception {
        String sql = substituteFilter(config.sql, filter);
        try (Connection conn = DriverManager.getConnection(config.connectionString, config.user, config.password);
             Statement stmt = conn.createStatement()) {
            // register early so a cancelled MO query closes the connection even
            // while blocked in executeQuery/next, releasing the JDBC work
            ctx.registerForClose(conn);
            // bound a stalled query even when the client never cancels
            if (queryTimeoutSeconds > 0) {
                try {
                    stmt.setQueryTimeout(queryTimeoutSeconds);
                } catch (Exception ignored) {
                    // driver may not support it; connection-close on cancel remains
                }
            }
            // MySQL-protocol streaming mode: rows are fetched as they are read
            // instead of materializing the full result set in memory.
            try {
                stmt.setFetchSize(Integer.MIN_VALUE);
            } catch (Exception ignored) {
                // non-MySQL drivers may reject it; plain fetching still works
            }
            try (ResultSet rs = stmt.executeQuery(sql)) {
                ResultSetMetaData meta = rs.getMetaData();
                int columns = meta.getColumnCount();
                CsvChunker chunker = new CsvChunker(chunkSize, ctx);
                String[] values = new String[columns];
                while (rs.next()) {
                    if (ctx.isCancelled()) {
                        return;
                    }
                    for (int i = 0; i < columns; i++) {
                        values[i] = formatValue(rs.getObject(i + 1));
                    }
                    chunker.addRow(values);
                }
                chunker.finish();
            }
        }
    }

    static String formatValue(Object value) throws java.sql.SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof Timestamp) {
            return formatTimestamp((Timestamp) value);
        }
        if (value instanceof byte[]) {
            // Binary values cannot round-trip byte-for-byte through this
            // UTF-8 CSV stream: any encoding-then-decoding step (e.g. Latin-1
            // -> UTF-8) reshapes bytes >= 0x80, silently corrupting
            // BINARY/VARBINARY/BLOB before MO reads them. Fail loudly instead
            // of corrupting; byte-exact binary columns are out of scope for
            // the v1 CSV bridge (declare them as text or convert them in the
            // configured SQL, e.g. HEX(col)).
            throw new java.sql.SQLDataException(
                    "jstfu jdbc datasource cannot stream a binary column byte-for-byte "
                            + "over CSV; project it as text (e.g. HEX(col)) in the configured SQL");
        }
        return String.valueOf(value);
    }

    private static final java.time.format.DateTimeFormatter TS_FMT =
            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    static String formatTimestamp(Timestamp ts) {
        // yyyy-MM-dd HH:mm:ss[.ffffff] with the fraction only when non-zero;
        // Timestamp.toString() would emit a trailing ".0"
        String base = ts.toLocalDateTime().format(TS_FMT);
        int nanos = ts.getNanos();
        if (nanos == 0) {
            return base;
        }
        return String.format("%s.%06d", base, nanos / 1000);
    }
}
