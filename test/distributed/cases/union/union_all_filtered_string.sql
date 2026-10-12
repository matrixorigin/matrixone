-- @case
-- @desc: Filtered UNION ALL keeps the right string column and the connection usable (#30010)
-- @label:bvt

SELECT v AS value
FROM (SELECT 1 AS k, 'ten' AS v UNION ALL SELECT 2, 'twenty') AS u
WHERE k > 0
ORDER BY v;
SELECT 1 AS after_direct;

PREPARE issue_30010_stmt FROM
    'SELECT v AS value FROM
       (SELECT 1 AS k, "ten" AS v UNION ALL SELECT 2, "twenty") AS u
     WHERE k > ? ORDER BY v';
SET @issue_30010_threshold = 0;
EXECUTE issue_30010_stmt USING @issue_30010_threshold;
SELECT 1 AS after_prepared;

SET @issue_30010_threshold = 1;
EXECUTE issue_30010_stmt USING @issue_30010_threshold;
SELECT 1 AS after_reuse;
DEALLOCATE PREPARE issue_30010_stmt;
