-- @case
-- @desc:Independent scalar aggregates in recursive-member subqueries
-- @label:bvt

DROP DATABASE IF EXISTS cte_recursive_scalar_agg_29135;
CREATE DATABASE cte_recursive_scalar_agg_29135;
USE cte_recursive_scalar_agg_29135;

CREATE TABLE sales_29135(dt DATE, price DECIMAL(8, 2));
INSERT INTO sales_29135 VALUES
    ('2024-02-27', 10),
    ('2024-03-01', 20);

-- The documented recursive date-series shape: MAX() belongs to an independent
-- scalar query block, not to the recursive SELECT itself.
WITH RECURSIVE dates(dt) AS (
    SELECT MIN(dt) FROM sales_29135
    UNION ALL
    SELECT DATE_ADD(dt, INTERVAL 1 DAY)
    FROM dates
    WHERE DATE_ADD(dt, INTERVAL 1 DAY) <= (SELECT MAX(dt) FROM sales_29135)
)
SELECT dt FROM dates ORDER BY dt;

-- COUNT(*) in a scalar projection of the recursive member.
WITH RECURSIVE seq(n, row_count) AS (
    SELECT 1, 0
    UNION ALL
    SELECT n + 1, (SELECT COUNT(*) FROM sales_29135)
    FROM seq
    WHERE n < 3
)
SELECT n, row_count FROM seq ORDER BY n;

CREATE TABLE empty_bounds_29135(dt DATE);
CREATE TABLE null_bounds_29135(dt DATE);
INSERT INTO null_bounds_29135 VALUES (NULL), (NULL);

-- COUNT(*) over an empty input is still a valid independent scalar aggregate.
WITH RECURSIVE empty_count(n, row_count) AS (
    SELECT 1, 0
    UNION ALL
    SELECT n + 1, (SELECT COUNT(*) FROM empty_bounds_29135)
    FROM empty_count
    WHERE n < 2
)
SELECT n, row_count FROM empty_count ORDER BY n;

-- MIN() over all-NULL input returns NULL without preventing the recursive
-- query block from being planned or executed.
WITH RECURSIVE null_min(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1
    FROM null_min
    WHERE n < 2 AND (SELECT MIN(dt) FROM null_bounds_29135) IS NULL
)
SELECT n FROM null_min ORDER BY n;

CREATE TABLE limits_29135(limit_n INT);
INSERT INTO limits_29135 VALUES (2), (4);

-- A correlated aggregate remains tied to the current recursive row.
WITH RECURSIVE correlated_count(n, row_count) AS (
    SELECT 1, 0
    UNION ALL
    SELECT n + 1,
           (SELECT COUNT(*)
            FROM limits_29135
            WHERE limit_n = correlated_count.n)
    FROM correlated_count
    WHERE n < 4
)
SELECT n, row_count FROM correlated_count ORDER BY n;

-- The aggregate argument itself contains an independent scalar subquery.
WITH RECURSIVE nested_arg(n, value) AS (
    SELECT 1, 0
    UNION ALL
    SELECT n + 1,
           (SELECT MAX(limit_n + (SELECT COUNT(*) FROM sales_29135))
            FROM limits_29135)
    FROM nested_arg
    WHERE n < 2
)
SELECT n, value FROM nested_arg ORDER BY n;

-- Prepared executions must recompute the independent scalar aggregate for each
-- parameter value rather than retaining the first bound.
PREPARE p_29135 FROM 'WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < (SELECT MAX(limit_n) FROM limits_29135 WHERE limit_n <= ?)) SELECT n FROM r ORDER BY n';
SET @limit_29135 = 2;
EXECUTE p_29135 USING @limit_29135;
SET @limit_29135 = 4;
EXECUTE p_29135 USING @limit_29135;
DEALLOCATE PREPARE p_29135;

-- Aggregation directly in the recursive query block remains rejected.
-- @regex("not support aggregate function recursive cte", true)
WITH RECURSIVE rejected_29135(n) AS (
    SELECT 1
    UNION ALL
    SELECT MAX(n) FROM rejected_29135 WHERE n < 3
)
SELECT * FROM rejected_29135;

SELECT 'after direct aggregate error' AS service_status;

DROP DATABASE cte_recursive_scalar_agg_29135;
