-- @case
-- @desc: Keep the Top ordering-key layout stable for NULL UNION ALL batches (#30708)
-- @label:bvt

DROP DATABASE IF EXISTS issue_30708_bvt;
CREATE DATABASE issue_30708_bvt;
USE issue_30708_bvt;

DROP TABLE IF EXISTS issue_30708_top;
CREATE TABLE issue_30708_top (x INT);
INSERT INTO issue_30708_top VALUES (1), (2);

-- Reported shape, with full sorted result and a following same-session health check.
SELECT NULL AS x
UNION ALL
SELECT x FROM issue_30708_top WHERE x = 2
ORDER BY x DESC
LIMIT 2;
SELECT COUNT(*) AS source_rows FROM issue_30708_top;

-- Reverse the UNION branches to cover the other batch arrival order.
SELECT x FROM issue_30708_top WHERE x = 2
UNION ALL
SELECT NULL AS x
ORDER BY x DESC
LIMIT 2;

-- Nearest semantic controls: full sort, UNION DISTINCT, and ordinary table Top.
SELECT NULL AS x
UNION ALL
SELECT x FROM issue_30708_top WHERE x = 2
ORDER BY x DESC;
SELECT NULL AS x
UNION
SELECT x FROM issue_30708_top WHERE x = 2
ORDER BY x DESC;
SELECT x FROM issue_30708_top ORDER BY x DESC LIMIT 2;

-- LIMIT boundaries and an offset that crosses from the non-NULL row to NULL.
SELECT NULL AS x
UNION ALL
SELECT x FROM issue_30708_top WHERE x = 2
ORDER BY x DESC
LIMIT 0;
SELECT NULL AS x
UNION ALL
SELECT x FROM issue_30708_top WHERE x = 2
ORDER BY x DESC
LIMIT 1;
SELECT NULL AS x
UNION ALL
SELECT x FROM issue_30708_top WHERE x = 2
ORDER BY x DESC
LIMIT 3;
SELECT NULL AS x
UNION ALL
SELECT x FROM issue_30708_top WHERE x = 2
ORDER BY x DESC
LIMIT 1 OFFSET 1;

-- Default NULL placement and an explicit NULL-first expression.
SELECT NULL AS x
UNION ALL
SELECT x FROM issue_30708_top WHERE x = 2
ORDER BY x ASC
LIMIT 2;
SELECT NULL AS x
UNION ALL
SELECT x FROM issue_30708_top WHERE x = 2
ORDER BY x IS NULL DESC, x DESC
LIMIT 2;

-- The literal-only report also passes on the fresh baseline and stays a control.
SELECT NULL AS x UNION ALL SELECT 1 AS x;

-- Reuse the prepared query with different selected rows and LIMITs.
PREPARE issue_30708_stmt FROM
    'SELECT NULL AS x UNION ALL
     SELECT x FROM issue_30708_top WHERE x = ?
     ORDER BY x DESC LIMIT ?';
SET @issue_30708_key = 2;
SET @issue_30708_limit = 2;
EXECUTE issue_30708_stmt USING @issue_30708_key, @issue_30708_limit;
SET @issue_30708_key = 1;
SET @issue_30708_limit = 1;
EXECUTE issue_30708_stmt USING @issue_30708_key, @issue_30708_limit;
SET @issue_30708_key = 99;
SET @issue_30708_limit = 2;
EXECUTE issue_30708_stmt USING @issue_30708_key, @issue_30708_limit;
DEALLOCATE PREPARE issue_30708_stmt;

DROP TABLE issue_30708_top;
DROP DATABASE issue_30708_bvt;
