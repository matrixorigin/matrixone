-- @suit
-- @case
-- @desc:ALTER TABLE ALGORITHM/LOCK syntax compatibility and validation
-- @label:bvt

-- Setup
drop table if exists t_alg_lock;
create table t_alg_lock(a INT, b INT);
insert into t_alg_lock values (1, 2);

-- ============================================================
-- Success cases
-- ============================================================

-- S1: ALGORITHM=COPY with COPY-required operation (ADD COLUMN)
ALTER TABLE t_alg_lock ALGORITHM=COPY, ADD COLUMN c1 INT;

-- S2: ALGORITHM=INPLACE with INPLACE-capable operation (ADD INDEX)
ALTER TABLE t_alg_lock ALGORITHM=INPLACE, ADD INDEX idx_s2(a);

-- S3: ALGORITHM=DEFAULT with COPY-required operation
ALTER TABLE t_alg_lock ALGORITHM=DEFAULT, ADD COLUMN c2 INT;

-- S4: LOCK=SHARED with INPLACE operation
ALTER TABLE t_alg_lock LOCK=SHARED, ADD INDEX idx_s4(b);

-- S5: LOCK=EXCLUSIVE with COPY operation
ALTER TABLE t_alg_lock LOCK=EXCLUSIVE, ADD COLUMN c3 INT;

-- S6: Both ALGORITHM=COPY and LOCK=SHARED with COPY operation
ALTER TABLE t_alg_lock ALGORITHM=COPY, LOCK=SHARED, ADD COLUMN c4 INT;

-- S7: LOCK=DEFAULT with COPY operation
ALTER TABLE t_alg_lock LOCK=DEFAULT, ADD COLUMN c5 INT;

-- S8: ALGORITHM=INSTANT with RENAME COLUMN (dispatched to INPLACE path)
ALTER TABLE t_alg_lock ALGORITHM=INSTANT, LOCK=DEFAULT, RENAME COLUMN c5 TO c5_new;

-- S9: ALGORITHM=COPY with INPLACE-capable operation (COPY stricter than needed, allowed)
ALTER TABLE t_alg_lock ALGORITHM=COPY, ADD INDEX idx_s9(c4);

-- Verify table structure after all success operations
SHOW COLUMNS FROM t_alg_lock;

-- ============================================================
-- Failure cases
-- ============================================================

-- F1: ALGORITHM=INPLACE rejected for COPY-required operation (ADD COLUMN)
ALTER TABLE t_alg_lock ALGORITHM=INPLACE, ADD COLUMN cf1 INT;

-- F2: ALGORITHM=INSTANT rejected for COPY-required operation (ADD COLUMN)
ALTER TABLE t_alg_lock ALGORITHM=INSTANT, ADD COLUMN cf2 INT;

-- F3: ALGORITHM=INSTANT rejected for COPY-required operation (DROP COLUMN)
ALTER TABLE t_alg_lock ALGORITHM=INSTANT, DROP COLUMN c1;

-- F4: LOCK=NONE rejected when COPY is required
ALTER TABLE t_alg_lock LOCK=NONE, ADD COLUMN cf3 INT;

-- F5: ALGORITHM=INPLACE + LOCK=NONE rejected for DROP COLUMN (algorithm check fires first)
ALTER TABLE t_alg_lock ALGORITHM=INPLACE, LOCK=NONE, DROP COLUMN c1;

-- Verify table is unchanged after all failures (c1 still exists, no cf* columns)
SHOW COLUMNS FROM t_alg_lock;

-- Cleanup
drop table if exists t_alg_lock;
