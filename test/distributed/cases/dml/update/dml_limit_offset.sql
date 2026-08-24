-- @suit

-- @case
-- @desc: reject MySQL-invalid LIMIT offset forms before UPDATE or DELETE can mutate rows
-- @label:bvt

DROP TABLE IF EXISTS dml_limit_offset_t;
CREATE TABLE dml_limit_offset_t(id INT PRIMARY KEY, touched INT DEFAULT 0);
INSERT INTO dml_limit_offset_t VALUES (1, 0), (2, 0), (3, 0);

UPDATE dml_limit_offset_t SET touched = 1 ORDER BY id LIMIT 1 OFFSET 1;
SELECT id, touched FROM dml_limit_offset_t ORDER BY id;

UPDATE dml_limit_offset_t SET touched = 1 ORDER BY id LIMIT 1, 1;
SELECT id, touched FROM dml_limit_offset_t ORDER BY id;

DELETE FROM dml_limit_offset_t ORDER BY id LIMIT 1 OFFSET 1;
SELECT id, touched FROM dml_limit_offset_t ORDER BY id;

DELETE FROM dml_limit_offset_t ORDER BY id LIMIT 1, 1;
SELECT id, touched FROM dml_limit_offset_t ORDER BY id;

-- Legal count-only forms retain their existing row-selection semantics.
UPDATE dml_limit_offset_t SET touched = 2 ORDER BY id LIMIT 1;
SELECT id, touched FROM dml_limit_offset_t ORDER BY id;
DELETE FROM dml_limit_offset_t ORDER BY id LIMIT 1;
SELECT id, touched FROM dml_limit_offset_t ORDER BY id;

-- Boundary and failure controls leave the remaining rows unchanged and the
-- session usable.
UPDATE dml_limit_offset_t SET touched = 3 ORDER BY id LIMIT 0;
DELETE FROM dml_limit_offset_t ORDER BY id LIMIT 0;
SELECT id, touched FROM dml_limit_offset_t ORDER BY id;
UPDATE dml_limit_offset_t SET touched = 3 ORDER BY id LIMIT -1;
SELECT id, touched FROM dml_limit_offset_t ORDER BY id;
DELETE FROM dml_limit_offset_t ORDER BY missing_col LIMIT 1;
SELECT id, touched FROM dml_limit_offset_t ORDER BY id;

DROP TABLE dml_limit_offset_t;
