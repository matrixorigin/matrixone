// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public final class PreparedOutfileReceiptProbe {
    private static final String CANARY = "OUTFILE30745_PRIVATE_CANARY";
    private static final long PUBLICATION_TIMEOUT = TimeUnit.SECONDS.toNanos(60);

    private static void check(boolean value, String message) {
        if (!value) throw new AssertionError(message);
    }

    private static String required(String name) {
        String value = System.getenv(name);
        check(value != null && !value.isEmpty(), name + " is required");
        return value;
    }

    private static String quote(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(15);
            statement.execute(sql);
        }
    }

    private static String scalar(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(15);
            try (ResultSet rows = statement.executeQuery(sql)) {
                check(rows.next(), "missing scalar row");
                String value = rows.getString(1);
                check(!rows.next(), "extra scalar row");
                return value;
            }
        }
    }

    private static void serverPrepared(PreparedStatement statement) throws SQLException {
        String type = statement.getClass().getName();
        check(type.equals("com.mysql.cj.jdbc.ServerPreparedStatement"),
                "binary protocol required, got " + type);
        statement.setQueryTimeout(15);
        System.out.println("protocol=" + type);
    }

    private static String field(String name) {
        return "json_unquote(json_extract(exec_plan,'$.outfile." + name + "'))";
    }

    private static int queryTimeout(long deadline) {
        long remaining = deadline - System.nanoTime();
        check(remaining > 0, "telemetry publication deadline expired");
        // JDBC query timeouts have whole-second granularity.
        return (int) Math.max(1, Math.min(15, TimeUnit.NANOSECONDS.toSeconds(remaining)));
    }

    private static final class Measurement {
        final long bytes;
        final long rows;

        Measurement(Path file, int value) throws IOException {
            List<String> lines = Files.readAllLines(file);
            check(lines.equals(List.of("value", Integer.toString(value))), "unexpected CSV content");
            bytes = Files.size(file);
            rows = lines.size() - 1;
        }
    }

    private static List<String> exportReceipts(Connection observer, long connectionId,
            String target, List<Measurement> expected) throws Exception {
        String sql = "select statement_id,aggr_count,statement,exec_plan,"
                + field("version") + "," + field("format") + "," + field("bytes_written")
                + "," + field("rows_written") + "," + field("complete") + "," + field("target_redacted")
                + " from system.statement_info where connection_id=" + connectionId
                + " and status='Success' and statement_type='Select' and " + field("target") + "=" + quote(target)
                + " order by request_at";
        long deadline = System.nanoTime() + PUBLICATION_TIMEOUT;
        do {
            List<String> ids = new ArrayList<>();
            List<String> plans = new ArrayList<>();
            Set<Long> actualBytes = new HashSet<>();
            // Each iteration closes its statement/rows before waiting for async telemetry.
            try (Statement statement = observer.createStatement()) {
                statement.setQueryTimeout(queryTimeout(deadline));
                try (ResultSet rows = statement.executeQuery(sql)) {
                    while (rows.next()) {
                        String id = rows.getString(1);
                        UUID.fromString(id);
                        check(!ids.contains(id), "duplicate terminal statement identity");
                        ids.add(id);
                        check(rows.getLong(2) == 0, "OUTFILE was aggregated");
                        check(!rows.getString(3).contains(CANARY), "statement retained target secret");
                        check(rows.getString(3).contains(target), "statement lost logical target");
                        String plan = rows.getString(4);
                        plans.add(plan);
                        check(!plan.contains(CANARY), "receipt retained target secret");
                        check("1".equals(rows.getString(5)), "receipt version");
                        check("csv".equals(rows.getString(6)), "receipt format");
                        long bytes = Long.parseLong(rows.getString(7));
                        long count = Long.parseLong(rows.getString(8));
                        check(expected.stream().anyMatch(e -> e.bytes == bytes && e.rows == count),
                                "receipt differs from independently read file: " + plan);
                        check(actualBytes.add(bytes), "duplicate receipt for one execution");
                        check("true".equals(rows.getString(9)), "successful sink incomplete");
                        check("true".equals(rows.getString(10)), "missing redaction marker");
                    }
                }
            }
            check(ids.size() <= expected.size(), "extra terminal export receipts");
            if (ids.size() == expected.size()) {
                for (int i = 0; i < ids.size(); i++) {
                    System.out.println("export statement_id=" + ids.get(i) + " exec_plan=" + plans.get(i));
                }
                return ids;
            }
            Thread.sleep(100); // Poll only the real asynchronous publication condition.
        } while (System.nanoTime() < deadline);
        throw new AssertionError("terminal export receipts not published within 60s");
    }

    private static List<String> ordinaryReceipts(Connection observer, long connectionId,
            String variable, int expected) throws Exception {
        String sql = "select statement_id,aggr_count,json_extract(exec_plan,'$.outfile'),exec_plan"
                + " from system.statement_info where connection_id=" + connectionId
                + " and status='Success' and statement_type='Set Var' and statement like "
                + quote("%@" + variable + "%") + " order by request_at";
        long deadline = System.nanoTime() + PUBLICATION_TIMEOUT;
        do {
            List<String> ids = new ArrayList<>();
            List<String> plans = new ArrayList<>();
            try (Statement statement = observer.createStatement()) {
                statement.setQueryTimeout(queryTimeout(deadline));
                try (ResultSet rows = statement.executeQuery(sql)) {
                    while (rows.next()) {
                        String id = rows.getString(1);
                        UUID.fromString(id);
                        check(!ids.contains(id), "duplicate ordinary statement identity");
                        ids.add(id);
                        check(rows.getLong(2) == 0, "ordinary SET should be individual");
                        String outfile = rows.getString(3);
                        check(outfile == null || outfile.equals("null"), "ordinary statement inherited receipt");
                        plans.add(rows.getString(4));
                    }
                }
            }
            check(ids.size() <= expected, "extra terminal ordinary receipts");
            if (ids.size() == expected) {
                for (int i = 0; i < ids.size(); i++) {
                    System.out.println("ordinary statement_id=" + ids.get(i) + " exec_plan=" + plans.get(i));
                }
                return ids;
            }
            Thread.sleep(100);
        } while (System.nanoTime() < deadline);
        throw new AssertionError("terminal ordinary statements not published within 60s");
    }

    private static final class Workspace implements AutoCloseable {
        final Path directory;
        final Path file;

        Workspace(Path parent) throws IOException {
            directory = Files.createTempDirectory(parent, "prepared-outfile30745-");
            file = directory.resolve("prepared.csv");
        }

        @Override
        public void close() throws IOException {
            IOException failure = null;
            try {
                Files.deleteIfExists(file);
            } catch (IOException error) {
                failure = error;
            }
            try {
                Files.delete(directory);
            } catch (IOException error) {
                if (failure == null) failure = error;
                else failure.addSuppressed(error);
            }
            if (failure != null) throw failure;
        }
    }

    private static final class Stage implements AutoCloseable {
        final Connection connection;
        final String name;

        Stage(Connection connection, String name) {
            this.connection = connection;
            this.name = name;
        }

        @Override
        public void close() throws SQLException {
            execute(connection, "drop stage if exists " + name);
        }
    }

    public static void main(String[] args) throws Exception {
        Path parent = Path.of(required("OUTFILE_PARENT")).toAbsolutePath().normalize();
        check(Files.isDirectory(parent), "OUTFILE_PARENT must exist and be visible to both client and server");
        String baseUrl = required("DB_URL");
        String url = baseUrl + (baseUrl.contains("?") ? "&" : "?")
                + "useServerPrepStmts=true&emulateUnsupportedPstmts=false&cachePrepStmts=false"
                + "&connectTimeout=10000&socketTimeout=15000";
        Properties properties = new Properties();
        properties.setProperty("user", required("DB_USER"));
        properties.setProperty("password", System.getenv().getOrDefault("DB_PASSWORD", ""));
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        String stageName = "qa_outfile30745_" + suffix;
        String variable = "qa_outfile30745_next_" + suffix;
        Path ownedDirectory;
        try (Workspace workspace = new Workspace(parent);
             Connection writer = DriverManager.getConnection(url, properties);
             Connection observer = DriverManager.getConnection(url, properties)) {
            ownedDirectory = workspace.directory;
            long connectionId = Long.parseLong(scalar(writer, "select connection_id()"));
            check("0".equals(scalar(writer, "select @@session.disable_agg_statement")),
                    "OUTFILE test requires ordinary session aggregation enabled");
            String target = "stage://" + stageName + "/prepared.csv";
            try (Stage stage = new Stage(writer, stageName)) {
                execute(writer, "create stage " + stage.name + " url=" + quote(workspace.directory.toUri().toString()));
                try (PreparedStatement export = writer.prepareStatement("select ? as value into outfile "
                            + quote(target + "?token=" + CANARY) + " format 'csv'");
                     PreparedStatement next = writer.prepareStatement("set @" + variable + " = ?")) {
                    serverPrepared(export);
                    serverPrepared(next);
                    List<Measurement> expected = new ArrayList<>();
                    for (int value : new int[]{73, 7301}) {
                        export.setInt(1, value);
                        next.setInt(1, value);
                        export.execute();
                        next.execute(); // Immediately next request: no intervening SQL, close or observer poll.
                        check(Integer.toString(value).equals(scalar(writer, "select @" + variable)),
                                "binary ordinary SET did not bind its parameter");
                        Measurement measured = new Measurement(workspace.file, value);
                        expected.add(measured);
                        List<String> exports = exportReceipts(observer, connectionId, target, expected);
                        List<String> controls = ordinaryReceipts(observer, connectionId, variable, expected.size());
                        check(exports.stream().noneMatch(controls::contains), "export and ordinary reused statement ID");
                        check("0".equals(scalar(writer, "select @@session.disable_agg_statement")),
                                "probe must not mask OUTFILE nonaggregation");
                        System.out.println("PASS run=" + expected.size() + " value=" + value + " target=" + target
                                + " bytes=" + measured.bytes + " rows=" + measured.rows
                                + " version=1 format=csv complete=true target_redacted=true aggr_count=0"
                                + " export_ids=" + exports + " ordinary_ids=" + controls + " ordinary_outfile=absent");
                        Files.delete(workspace.file);
                    }
                }
            }
            check("0".equals(scalar(observer, "select count(*) from mo_catalog.mo_stages where stage_name="
                    + quote(stageName))), "stage cleanup failed");
        }
        check(!Files.exists(ownedDirectory), "owned output directory remains");
        System.out.println("PASS cleanup stage=absent output_directory=absent");
    }
}
