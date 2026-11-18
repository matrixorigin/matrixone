-- @case
-- @desc: mixed UNION/INTERSECT/EXCEPT chains keep every collected operand and edge when the chain begins with two textually identical UNION DISTINCT branches (#31760)
-- @label:bvt

DROP DATABASE IF EXISTS union_identical_31760;
CREATE DATABASE union_identical_31760;
USE union_identical_31760;

-- Wrong-rowset witnesses before the collector fix; the baseline rows are
-- measured by the baseline run and are never asserted from this golden.
SELECT 1 AS v UNION SELECT 1 AS v INTERSECT SELECT 2;
SELECT 1 AS v UNION SELECT 1 AS v INTERSECT SELECT 2 UNION SELECT 3 ORDER BY v;
SELECT 1 AS v UNION SELECT 1 AS v INTERSECT SELECT 2 UNION ALL SELECT 3 ORDER BY v;
SELECT 1 AS v UNION (SELECT 1 AS v) INTERSECT (SELECT 2);
SELECT NULL AS v UNION SELECT NULL AS v INTERSECT SELECT 1;
SELECT 'a' AS v UNION SELECT 'a' AS v INTERSECT SELECT 'b';
-- @sortkey:0
SELECT 1 AS v UNION SELECT 1 AS v INTERSECT SELECT 2 UNION SELECT 3 ORDER BY v DESC LIMIT 2;
SELECT 1 AS v UNION SELECT 1 AS v INTERSECT ALL SELECT 2;
SELECT 1 AS v UNION SELECT 1 AS v INTERSECT SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 3 ORDER BY v;

-- Shape-only boundary: the rowset is {1} on both binaries, so the plan-level
-- shapes carry the distinction, not this public rowset.
SELECT 1 AS v UNION SELECT 1 EXCEPT SELECT 2;

-- Standard-correct controls (issue-thread literals and nearest neighbors).
SELECT 1 AS v UNION SELECT 2 INTERSECT SELECT 2 UNION SELECT 3 ORDER BY v;
SELECT 1 AS v UNION (SELECT 2) INTERSECT (SELECT 2) ORDER BY v;
(SELECT 1 AS v UNION SELECT 1) INTERSECT SELECT 1;
SELECT 1 AS v UNION SELECT 2 ORDER BY v;
SELECT 1 AS v UNION ALL SELECT 2 ORDER BY v;
SELECT 1 AS v UNION SELECT 1 AS v;
SELECT 1 AS v UNION ALL SELECT 1;

-- Duplicate multiplicity and NULL rows through a materialized table.
CREATE TABLE t_setops_31760 (a INT);
INSERT INTO t_setops_31760 VALUES (1), (1), (2), (NULL);
SELECT a AS v FROM t_setops_31760 UNION SELECT a AS v FROM t_setops_31760 INTERSECT SELECT 2 ORDER BY v;
SELECT a AS v FROM t_setops_31760 UNION SELECT a FROM t_setops_31760 INTERSECT SELECT a FROM t_setops_31760 ORDER BY v;
SELECT a AS v FROM t_setops_31760 UNION ALL SELECT a FROM t_setops_31760 INTERSECT SELECT 2 ORDER BY v;

-- Prepared parameter typing and same-session reuse through one connection.
PREPARE st_31760 FROM 'SELECT 1 AS v UNION SELECT 1 AS v INTERSECT SELECT 2 UNION SELECT ? ORDER BY 1';
SET @p_31760 = 3;
EXECUTE st_31760 USING @p_31760;
SET @p_31760 = 2;
EXECUTE st_31760 USING @p_31760;

-- Column arity is still checked on a collected chain, and the same
-- connection stays usable after the error.
-- @regex(".*different number of columns.*", true)
SELECT 1 AS v UNION SELECT 1 INTERSECT SELECT 2, 3;
SELECT 1 AS v;

DEALLOCATE PREPARE st_31760;
DROP DATABASE IF EXISTS union_identical_31760;
