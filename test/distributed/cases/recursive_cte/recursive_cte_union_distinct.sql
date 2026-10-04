WITH RECURSIVE r(n) AS (
    SELECT 1
    UNION
    SELECT n + 1 FROM r WHERE n < 10
)
SELECT COUNT(*), SUM(n), MAX(n) FROM r;

WITH RECURSIVE r(n) AS (
    SELECT 1
    UNION DISTINCT
    SELECT n + 1 FROM r WHERE n < 10
)
SELECT COUNT(*), SUM(n), MAX(n) FROM r;

WITH RECURSIVE stable(n) AS (
    SELECT 1
    UNION
    SELECT n FROM stable
)
SELECT COUNT(*), SUM(n) FROM stable;

WITH RECURSIVE pairs(n, label) AS (
    SELECT 1, CAST(NULL AS VARCHAR)
    UNION DISTINCT
    SELECT
        CASE WHEN n < 3 THEN n + 1 ELSE n END,
        CASE WHEN n < 3 THEN CAST(n + 1 AS VARCHAR) ELSE label END
    FROM pairs
)
SELECT n, label FROM pairs ORDER BY n;

WITH RECURSIVE all_rows(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM all_rows WHERE n < 3
)
SELECT COUNT(*), SUM(n), MAX(n) FROM all_rows;
