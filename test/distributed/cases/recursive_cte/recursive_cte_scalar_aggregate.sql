-- @case
-- @desc:Independent scalar aggregates, GROUP BY and HAVING in recursive-member subqueries
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

-- GROUP BY and HAVING in the anchor are legal; the recursive member itself
-- only propagates the grouped values.
WITH RECURSIVE grouped_anchor(n, row_count) AS (
    SELECT limit_n, COUNT(*) FROM limits_29135
    WHERE limit_n = 2 GROUP BY limit_n HAVING COUNT(*) > 0
    UNION ALL
    SELECT n + 1, row_count FROM grouped_anchor WHERE n < 4
)
SELECT n, row_count FROM grouped_anchor ORDER BY n;

-- Both sales dates belong to the same year, so the grouped scalar is one row.
WITH RECURSIVE grouped_projection(n, row_count) AS (
    SELECT 1, 0
    UNION ALL
    SELECT n + 1, (SELECT COUNT(*) FROM sales_29135 GROUP BY YEAR(dt))
    FROM grouped_projection WHERE n < 3
)
SELECT n, row_count FROM grouped_projection ORDER BY n;

-- HAVING without GROUP BY must work independently of the grouping guard.
-- A filtered-out aggregate row becomes scalar NULL, rather than COUNT's zero.
WITH RECURSIVE scalar_having(n, present_count, absent_count) AS (
    SELECT 1, 0, 0
    UNION ALL
    SELECT n + 1,
           (SELECT COUNT(*) FROM sales_29135 HAVING COUNT(*) > 0),
           (SELECT COUNT(*) FROM sales_29135 HAVING COUNT(*) > 2)
    FROM scalar_having WHERE n < 2
)
SELECT n, present_count, absent_count FROM scalar_having ORDER BY n;

-- A grouped scalar predicate with HAVING controls recursive termination.
WITH RECURSIVE grouped_predicate(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM grouped_predicate
    WHERE n < (SELECT MAX(limit_n) FROM limits_29135
               GROUP BY limit_n HAVING limit_n = 2)
)
SELECT n FROM grouped_predicate ORDER BY n;

-- Correlation reads the current recursive row, while grouping belongs to the
-- child SELECT. No matching group returns NULL (the ungrouped control above
-- returns zero instead).
WITH RECURSIVE correlated_group(n, row_count) AS (
    SELECT 1, 0
    UNION ALL
    SELECT n + 1, (SELECT COUNT(*) FROM limits_29135
                   WHERE limit_n = correlated_group.n GROUP BY limit_n)
    FROM correlated_group WHERE n < 4
)
SELECT n, row_count FROM correlated_group ORDER BY n;

-- NULL keys form a group; an empty input produces no grouped scalar row.
WITH RECURSIVE group_boundaries(n, null_count, empty_count) AS (
    SELECT 1, 0, 0
    UNION ALL
    SELECT n + 1,
           (SELECT COUNT(*) FROM null_bounds_29135 GROUP BY dt),
           (SELECT COUNT(*) FROM empty_bounds_29135 GROUP BY dt)
    FROM group_boundaries WHERE n < 2
)
SELECT n, null_count, empty_count FROM group_boundaries ORDER BY n;

-- Every nested SELECT owns its grouping independently.
WITH RECURSIVE nested_group(n, row_count) AS (
    SELECT 1, 0
    UNION ALL
    SELECT n + 1, (SELECT (SELECT COUNT(*) FROM sales_29135 GROUP BY YEAR(dt)))
    FROM nested_group WHERE n < 2
)
SELECT n, row_count FROM nested_group ORDER BY n;

-- Rebinding a grouped predicate must handle a value, another value, and then
-- an empty group on successive executions of the same prepared statement.
PREPARE grouped_23077 FROM 'WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < (SELECT MAX(limit_n) FROM limits_29135 WHERE limit_n = ? GROUP BY limit_n HAVING COUNT(*) > 0)) SELECT n FROM r ORDER BY n';
SET @grouped_limit = 2;
EXECUTE grouped_23077 USING @grouped_limit;
SET @grouped_limit = 4;
EXECUTE grouped_23077 USING @grouped_limit;
SET @grouped_limit = 3;
EXECUTE grouped_23077 USING @grouped_limit;
DEALLOCATE PREPARE grouped_23077;

-- More than one group must retain the ordinary scalar cardinality error.
-- @regex("Subquery returns more than 1 row", true)
WITH RECURSIVE multiple_groups(n, row_count) AS (
    SELECT 1, 0
    UNION ALL
    SELECT n + 1, (SELECT COUNT(*) FROM limits_29135 GROUP BY limit_n)
    FROM multiple_groups WHERE n < 2
)
SELECT n, row_count FROM multiple_groups;

-- Direct recursive grouping remains prohibited.
-- @regex("not support group by in recursive cte", true)
WITH RECURSIVE direct_group(n) AS (
    SELECT 1 UNION ALL SELECT n + 1 FROM direct_group WHERE n < 3 GROUP BY n
)
SELECT * FROM direct_group;

-- Grouping cannot make a nested recursive source legal.
-- @regex("not in any subquery", true)
WITH RECURSIVE nested_source(n, row_count) AS (
    SELECT 1, 0
    UNION ALL
    SELECT n + 1, (SELECT COUNT(*) FROM nested_source GROUP BY n)
    FROM nested_source WHERE n < 2
)
SELECT * FROM nested_source;

SELECT 'after grouped scalar errors' AS service_status;

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
