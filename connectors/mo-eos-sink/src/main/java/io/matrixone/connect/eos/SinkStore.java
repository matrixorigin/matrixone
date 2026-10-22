package io.matrixone.connect.eos;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;

/** Owns all MatrixOne transactions and the unknown-COMMIT recovery decision. */
final class SinkStore {
    record Checkpoint(long offset, long epoch, String topicId) {}
    record Delivery(long offset, EventEnvelope event) {}
    private record Budget(long rows, long bytes, long maxRows, long maxBytes) {}
    private enum Resolution { COMMITTED, NOT_COMMITTED }

    private final SinkSettings settings;

    SinkStore(SinkSettings settings) {
        this.settings = settings;
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(settings.jdbcUrl, settings.jdbcProperties());
    }

    Checkpoint claim(TopicPartition tp, String topicId) {
        validateTopicId(topicId);
        try (Connection connection = connect()) {
            connection.setAutoCommit(false);
            try {
                Checkpoint previous = checkpoint(connection, tp, true);
                if (previous.topicId() == null) {
                    if (previous.offset() != -1 || previous.epoch() != 0) {
                        throw new ConnectException("unbound progressed Kafka topic identity for " + tp);
                    }
                } else if (!previous.topicId().equals(topicId)) {
                    throw new ConnectException("Kafka topic identity changed for " + tp);
                }
                if (previous.epoch() == Long.MAX_VALUE || previous.offset() == Long.MAX_VALUE) {
                    throw new ConnectException("checkpoint epoch/offset exhausted for " + tp);
                }
                try (PreparedStatement update = connection.prepareStatement(
                    "UPDATE mo_sink_checkpoint SET owner_epoch=?,topic_id=? WHERE scope=? AND topic=? AND partition_id=?")) {
                    update.setLong(1, previous.epoch() + 1);
                    update.setBytes(2, topicId.getBytes(StandardCharsets.US_ASCII));
                    update.setBytes(3, settings.scopeBytes);
                    update.setBytes(4, topicBytes(tp));
                    update.setInt(5, tp.partition());
                    requireOne(update.executeUpdate(), "claim checkpoint");
                }
                connection.commit();
                return new Checkpoint(previous.offset(), previous.epoch() + 1, topicId);
            } catch (SQLException | RuntimeException error) {
                rollback(connection, error);
                throw error;
            }
        } catch (SQLException error) {
            throw new ConnectException("claim MatrixOne checkpoint for " + tp, error);
        }
    }

    Checkpoint read(TopicPartition tp) {
        try (Connection connection = connect()) {
            return checkpoint(connection, tp, false);
        } catch (SQLException error) {
            throw new ConnectException("read MatrixOne checkpoint for " + tp, error);
        }
    }

    long apply(TopicPartition tp, Checkpoint owner, long base, List<Delivery> records) {
        if (records.isEmpty()) {
            return base;
        }
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                return applyOnce(tp, owner.epoch(), owner.topicId(), base, records);
            } catch (RetryAfterRollback error) {
                if (attempt == 3) {
                    throw new ConnectException("MatrixOne transaction retry limit for " + tp, error);
                }
            } catch (SQLException error) {
                if (isKnownAbort(error) && attempt < 3) {
                    continue;
                }
                throw new ConnectException("MatrixOne transaction failed for " + tp, error);
            }
        }
        throw new AssertionError("retry loop terminated unexpectedly");
    }

    private long applyOnce(TopicPartition tp, long epoch, String topicId, long base,
                           List<Delivery> records) throws SQLException, RetryAfterRollback {
        long staged = base;
        boolean commitAttempted = false;
        try (Connection connection = connect()) {
            connection.setAutoCommit(false);
            try {
                Checkpoint actual = checkpoint(connection, tp, true);
                if (actual.epoch() != epoch || actual.offset() != base
                    || !topicId.equals(actual.topicId())) {
                    throw new ConnectException("stale owner or checkpoint for " + tp);
                }
                Budget budget = budget(connection);
                long newRows = budget.rows();
                long newBytes = budget.bytes();
                for (int i = 0; i < records.size(); i++) {
                    Delivery delivery = records.get(i);
                    long offset = delivery.offset();
                    EventEnvelope event = delivery.event();
                    if (offset < staged || (offset == staged && !(i == 0 && offset == base))) {
                        throw new ConnectException("out-of-order Kafka delivery for " + tp);
                    }
                    String existing = ledgerDigest(connection, event.eventIdBytes);
                    if (offset == base && existing == null) {
                        throw new ConnectException("checkpoint replay has no ledger row for " + tp);
                    }
                    if (existing != null) {
                        if (!existing.equals(event.digest)) {
                            throw new ConnectException("event ID has a different digest for " + tp);
                        }
                    } else {
                        long bytes = logicalLedgerBytes(tp, event);
                        newRows = Math.addExact(newRows, 1);
                        newBytes = Math.addExact(newBytes, bytes);
                        if (newRows > budget.maxRows() || newBytes > budget.maxBytes()) {
                            throw new ConnectException("ledger capacity exhausted for scope " + settings.scope);
                        }
                        insertLedger(connection, tp, delivery);
                        updateTotal(connection, event.delta);
                    }
                    staged = offset;
                }
                if (newRows != budget.rows()) {
                    try (PreparedStatement update = connection.prepareStatement(
                        "UPDATE mo_sink_budget SET used_rows=?,used_bytes=? WHERE scope=?")) {
                        update.setLong(1, newRows);
                        update.setLong(2, newBytes);
                        update.setBytes(3, settings.scopeBytes);
                        requireOne(update.executeUpdate(), "update ledger budget");
                    }
                }
                if (staged > base) {
                    try (PreparedStatement update = connection.prepareStatement(
                        "UPDATE mo_sink_checkpoint SET last_offset=? WHERE scope=? AND topic=? AND partition_id=?")) {
                        update.setLong(1, staged);
                        update.setBytes(2, settings.scopeBytes);
                        update.setBytes(3, topicBytes(tp));
                        update.setInt(4, tp.partition());
                        requireOne(update.executeUpdate(), "advance checkpoint");
                    }
                }
                commitAttempted = true;
                try {
                    connection.commit();
                    return staged;
                } catch (SQLException unknown) {
                    // Never reuse the connection whose COMMIT outcome is unknown.
                    try { connection.close(); } catch (SQLException closeError) { unknown.addSuppressed(closeError); }
                    Resolution outcome = resolveCommit(tp, epoch, topicId, base, staged, records);
                    if (outcome == Resolution.COMMITTED) {
                        return staged;
                    }
                    throw new RetryAfterRollback(unknown);
                }
            } catch (SQLException | RuntimeException | RetryAfterRollback error) {
                if (!commitAttempted) {
                    rollback(connection, error);
                }
                throw error;
            }
        }
    }

    private Resolution resolveCommit(TopicPartition tp, long epoch, String topicId, long base,
                                     long staged, List<Delivery> records) {
        try (Connection connection = connect()) {
            Checkpoint actual = checkpoint(connection, tp, false);
            if (actual.epoch() != epoch || !topicId.equals(actual.topicId())) {
                throw new ConnectException("owner changed during unknown COMMIT for " + tp);
            }
            if (actual.offset() == base && staged != base) {
                return Resolution.NOT_COMMITTED;
            }
            if (actual.offset() != staged) {
                throw new ConnectException("inconsistent checkpoint after unknown COMMIT for " + tp);
            }
            for (Delivery delivery : records) {
                String digest = ledgerDigest(connection, delivery.event().eventIdBytes);
                if (!delivery.event().digest.equals(digest)) {
                    throw new ConnectException("inconsistent ledger after unknown COMMIT for " + tp);
                }
            }
            return Resolution.COMMITTED;
        } catch (SQLException error) {
            throw new ConnectException("cannot resolve unknown MatrixOne COMMIT for " + tp, error);
        }
    }

    private Checkpoint checkpoint(Connection connection, TopicPartition tp,
                                  boolean lock) throws SQLException {
        String sql = "SELECT last_offset,owner_epoch,topic_id FROM mo_sink_checkpoint "
            + "WHERE scope=? AND topic=? AND partition_id=?" + (lock ? " FOR UPDATE" : "");
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            query.setBytes(1, settings.scopeBytes);
            query.setBytes(2, topicBytes(tp));
            query.setInt(3, tp.partition());
            try (ResultSet rows = query.executeQuery()) {
                if (!rows.next()) {
                    throw new ConnectException("missing pre-created checkpoint for " + tp);
                }
                byte[] rawTopicId = rows.getBytes(3);
                String topicId = rawTopicId == null ? null : new String(rawTopicId, StandardCharsets.US_ASCII);
                if (rawTopicId != null && (!Arrays.equals(rawTopicId, topicId.getBytes(StandardCharsets.US_ASCII))
                    || rawTopicId.length != 22)) {
                    throw new ConnectException("malformed stored Kafka topic identity for " + tp);
                }
                Checkpoint result = new Checkpoint(rows.getLong(1), rows.getLong(2), topicId);
                if (rows.next()) {
                    throw new ConnectException("duplicate checkpoint for " + tp);
                }
                return result;
            }
        }
    }

    private Budget budget(Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement(
            "SELECT used_rows,used_bytes,max_rows,max_bytes FROM mo_sink_budget WHERE scope=? FOR UPDATE")) {
            query.setBytes(1, settings.scopeBytes);
            try (ResultSet rows = query.executeQuery()) {
                if (!rows.next()) {
                    throw new ConnectException("missing pre-created ledger budget for scope " + settings.scope);
                }
                Budget result = new Budget(rows.getLong(1), rows.getLong(2), rows.getLong(3), rows.getLong(4));
                if (result.rows() < 0 || result.bytes() < 0 || result.maxRows() <= 0 || result.maxBytes() <= 0) {
                    throw new ConnectException("invalid ledger budget for scope " + settings.scope);
                }
                return result;
            }
        }
    }

    private String ledgerDigest(Connection connection, byte[] eventId) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement(
            "SELECT digest_version,payload_digest FROM mo_sink_ledger WHERE scope=? AND event_id=? FOR UPDATE")) {
            query.setBytes(1, settings.scopeBytes);
            query.setBytes(2, eventId);
            try (ResultSet rows = query.executeQuery()) {
                if (!rows.next()) {
                    return null;
                }
                String version = rows.getString(1);
                String digest = rows.getString(2);
                if (!EventEnvelope.DIGEST_VERSION.equals(version) || rows.next()) {
                    throw new ConnectException("ledger digest version or uniqueness conflict");
                }
                return digest;
            }
        }
    }

    private void insertLedger(Connection connection, TopicPartition tp,
                              Delivery delivery) throws SQLException {
        try (PreparedStatement insert = connection.prepareStatement(
            "INSERT INTO mo_sink_ledger(scope,event_id,digest_version,payload_digest,first_topic,first_partition,first_offset) "
                + "VALUES (?,?,?,?,?,?,?)")) {
            insert.setBytes(1, settings.scopeBytes);
            insert.setBytes(2, delivery.event().eventIdBytes);
            insert.setString(3, EventEnvelope.DIGEST_VERSION);
            insert.setString(4, delivery.event().digest);
            insert.setBytes(5, topicBytes(tp));
            insert.setInt(6, tp.partition());
            insert.setLong(7, delivery.offset());
            requireOne(insert.executeUpdate(), "insert ledger");
        }
    }

    private void updateTotal(Connection connection, long delta) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement(
            "SELECT n FROM mo_sink_totals WHERE scope=? FOR UPDATE")) {
            query.setBytes(1, settings.scopeBytes);
            try (ResultSet rows = query.executeQuery()) {
                if (!rows.next() || rows.next()) {
                    throw new ConnectException("missing or duplicate business total for scope " + settings.scope);
                }
            }
        }
        try (PreparedStatement update = connection.prepareStatement(
            "UPDATE mo_sink_totals SET n=n+? WHERE scope=?")) {
            update.setLong(1, delta);
            update.setBytes(2, settings.scopeBytes);
            int changed = update.executeUpdate();
            if (changed != 1 && !(delta == 0 && changed == 0)) {
                throw new ConnectException("update business total affected " + changed + " rows");
            }
        }
    }

    private long logicalLedgerBytes(TopicPartition tp, EventEnvelope event) {
        return (long) settings.scopeBytes.length + event.eventIdBytes.length
            + EventEnvelope.DIGEST_VERSION.length() + 64 + topicBytes(tp).length + 4 + 8;
    }

    private static byte[] topicBytes(TopicPartition tp) {
        byte[] bytes = tp.topic().getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0 || bytes.length > 249) {
            throw new ConnectException("Kafka topic exceeds 249 UTF-8 bytes");
        }
        return bytes;
    }

    private static void validateTopicId(String topicId) {
        try {
            Uuid parsed = Uuid.fromString(topicId);
            if (topicId.length() != 22 || Uuid.RESERVED.contains(parsed)
                || !parsed.toString().equals(topicId)) {
                throw new IllegalArgumentException("unsupported Kafka topic identity");
            }
        } catch (RuntimeException error) {
            throw new ConnectException("missing or invalid Kafka topic identity", error);
        }
    }

    private static void requireOne(int count, String operation) {
        if (count != 1) {
            throw new ConnectException(operation + " affected " + count + " rows, expected one");
        }
    }

    private static void rollback(Connection connection, Exception cause) {
        try { connection.rollback(); } catch (SQLException error) { cause.addSuppressed(error); }
    }

    private static boolean isKnownAbort(SQLException error) {
        int code = error.getErrorCode();
        return code == 1062 || code == 1205 || code == 1213 || code == 20619
            || code == 20628 || code == 20631 || code == 20701 || code == 20702;
    }

    private static final class RetryAfterRollback extends Exception {
        RetryAfterRollback(SQLException cause) { super(cause); }
    }
}
