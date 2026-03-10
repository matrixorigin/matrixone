-- Minimal repro: ColumnExpressionExecutor.Eval panic
--   "index out of range [2] with length 2"
--   at evalExpression.go:690
--
-- Root cause: NULL vectors in IVF-Flat entries table +
--   L2_DISTANCE ORDER BY LIMIT triggers Top operator with
--   relIndex=2 on a 2-element batches slice.

CREATE DATABASE IF NOT EXISTS vec_null_panic_db;
USE vec_null_panic_db;

-- 1) Create table with ivfflat index (small dimension to keep case short).
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (
  id varchar(64) NOT NULL PRIMARY KEY,
  embedding vecf32(4) DEFAULT NULL,
  KEY idx_emb USING ivfflat (embedding) lists = 2 op_type 'vector_l2_ops'
);

-- 2) Insert enough rows to build persisted ivfflat entries.
INSERT INTO t1 (id, embedding)
SELECT concat('r_', result), '[0.1,0.1,0.1,0.1]'
FROM generate_series(1, 100) g;

SELECT mo_ctl('dn', 'flush', 'vec_null_panic_db.t1');
SELECT mo_ctl('dn', 'checkpoint', '');

-- 3) Locate the hidden ivfflat entries table.
set @tbl_id = (
  SELECT rel_id FROM mo_catalog.mo_tables
  WHERE relname = 't1' AND reldatabase = 'vec_null_panic_db'
  LIMIT 1
);
set @entries_tbl = (
  SELECT index_table_name FROM mo_catalog.mo_indexes
  WHERE table_id = @tbl_id
    AND name = 'idx_emb' AND algo = 'ivfflat' AND algo_table_type = 'entries'
  LIMIT 1
);

-- 4) Inject rows with NULL vector into entries table.
set @ins_sql = concat(
  'INSERT INTO `', @entries_tbl,
  '`(`__mo_index_centroid_fk_version`,`__mo_index_centroid_fk_id`,`__mo_index_pri_col`,`__mo_index_centroid_fk_entry`) ',
  'SELECT 922337203685477500, 1, concat(''null_'', result), NULL FROM generate_series(1, 100) g'
);
prepare p_ins from @ins_sql;
execute p_ins;
deallocate prepare p_ins;

-- 5) Query entries table with L2_DISTANCE + ORDER BY + LIMIT → triggers panic.
set @q_sql = concat(
  'SELECT __mo_index_pri_col, ',
  'L2_DISTANCE(__mo_index_centroid_fk_entry, ''[0.5,0.5,0.5,0.5]'') AS d ',
  'FROM `', @entries_tbl, '` ORDER BY d LIMIT 1'
);
prepare p_q from @q_sql;
execute p_q;
deallocate prepare p_q;

-- Cleanup.
DROP TABLE IF EXISTS t1;
DROP DATABASE vec_null_panic_db;
