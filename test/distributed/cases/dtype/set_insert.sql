-- @suit
-- @case
-- @desc: insert / select for MySQL SET columns (literal VALUES only)
-- @label:bvt
--
-- Note: 3.0-dev stores SET as T_uint64 and lacks the runtime
-- cast_set_index_to_value function that main uses to format SELECT
-- output as member names. Consequently SELECT returns the raw bitmask
-- integer (e.g. 3 instead of 'read,write'). This is tracked as a
-- follow-up for full SET semantics (expression INSERT, subquery,
-- SELECT formatting). Update this .result when those are backported.

DROP DATABASE IF EXISTS test_set;
CREATE DATABASE test_set;
USE test_set;

CREATE TABLE t_set (
    id INT PRIMARY KEY,
    options SET('read', 'write', 'execute', 'delete'),
    colors SET('red', 'green', 'blue')
);

INSERT INTO t_set VALUES (1, 'read,write', 'red');
INSERT INTO t_set VALUES (2, 'execute', 'green,blue');
INSERT INTO t_set VALUES (3, 'read,write,execute,delete', 'red,green,blue');
INSERT INTO t_set VALUES (4, '', 'red');

SELECT * FROM t_set ORDER BY id;

UPDATE t_set SET colors = 'blue' WHERE id = 1;
SELECT id, colors FROM t_set WHERE id = 1;

DELETE FROM t_set WHERE id = 4;
SELECT id FROM t_set ORDER BY id;

DROP DATABASE test_set;
