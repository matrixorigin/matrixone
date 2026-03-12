-- Test: IVFFLAT search with large COMMITTED in-memory data should not panic
-- This test covers the bug where Reader.Read() appended a distVec to the
-- reused output batch, and the stale distVec from the previous InMem batch
-- was included in the Shuffle operation, causing slice bounds panic.
--
-- This case targets the filterInMemCommittedInserts path (PartitionState).

DROP TABLE IF EXISTS t_ivfflat_committed;
CREATE TABLE t_ivfflat_committed (
    id INT PRIMARY KEY,
    embedding vecf32(4) DEFAULT NULL
);

CREATE INDEX idx_ivf_committed USING ivfflat ON t_ivfflat_committed(embedding) lists=2 op_type 'vector_l2_ops';

-- Insert > 8192 rows using multiple small auto-committed statements.
-- This ensures the data stays in PartitionState memory and spans multiple batches.
INSERT INTO t_ivfflat_committed SELECT result+0, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+500, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+1000, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+1500, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+2000, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+2500, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+3000, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+3500, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+4000, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+4500, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+5000, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+5500, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+6000, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+6500, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+7000, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+7500, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+8000, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+8500, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+9000, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+9500, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+10000, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+10500, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+11000, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+11500, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;
INSERT INTO t_ivfflat_committed SELECT result+12000, '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 500) g;

-- Insert target vectors
INSERT INTO t_ivfflat_committed VALUES (12501, '[0.5, 0.5, 0.5, 0.5]');
INSERT INTO t_ivfflat_committed VALUES (12502, '[0.5, 0.5, 0.5, 0.5]');
INSERT INTO t_ivfflat_committed VALUES (12503, '[0.5, 0.5, 0.5, 0.5]');

-- Query on committed data: goes through filterInMemCommittedInserts path.
-- Before the fix, the stale distVec from batch 1 was shuffled in batch 2,
-- causing index out of range panic.
SELECT id, L2_DISTANCE(embedding, '[0.5, 0.5, 0.5, 0.5]') AS dist
FROM t_ivfflat_committed
ORDER BY dist ASC
LIMIT 3;

DROP TABLE IF EXISTS t_ivfflat_committed;
