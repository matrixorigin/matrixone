-- Test: IVFFLAT search with large in-memory data should not panic
-- This test covers the bug where Reader.Read() appended a distVec to the
-- reused output batch in the InMem path, but did not handle the case where
-- the batch was reused across multiple InMem batches (> BlockMaxRows=8192).

DROP TABLE IF EXISTS t_ivfflat_inmem;
CREATE TABLE t_ivfflat_inmem (
    id INT PRIMARY KEY,
    embedding vecf32(4) DEFAULT NULL
);

CREATE INDEX idx_ivf_inmem USING ivfflat ON t_ivfflat_inmem(embedding) lists=2 op_type 'vector_l2_ops';

BEGIN;

-- Insert 12000 identical vectors
INSERT INTO t_ivfflat_inmem SELECT result,       '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 7000) g;
INSERT INTO t_ivfflat_inmem SELECT result+7000,  '[0.1, 0.2, 0.3, 0.4]' FROM generate_series(1, 5000) g;

-- Query before commit triggers inMemUncommitted
SELECT id, L2_DISTANCE(embedding, '[0.5, 0.5, 0.5, 0.5]') AS dist
FROM t_ivfflat_inmem
ORDER BY dist ASC
LIMIT 5;

COMMIT;

DROP TABLE IF EXISTS t_ivfflat_inmem;
