-- @suit

-- @case
-- @desc:direct insert sources with set operations
-- @label:bvt
DROP DATABASE IF EXISTS insert_set_operation;
CREATE DATABASE insert_set_operation;
USE insert_set_operation;

CREATE TABLE source_left(i INT, s VARCHAR(10));
CREATE TABLE source_right(i INT, s VARCHAR(10));
CREATE TABLE destination(i BIGINT, s VARCHAR(20));

INSERT INTO source_left VALUES
    (1, 'a'), (1, 'a'), (2, 'b'), (3, 'c'), (NULL, 'n');
INSERT INTO source_right VALUES
    (1, 'a'), (1, 'a'), (1, 'a'), (2, 'b'), (4, 'd'), (NULL, 'n'), (NULL, 'n');

INSERT INTO destination(i, s)
SELECT i, s FROM source_left
UNION ALL
SELECT i, s FROM source_right;
SELECT i, s, COUNT(*) AS duplicate_count
FROM destination
GROUP BY i, s
ORDER BY i IS NOT NULL, i, s;

TRUNCATE TABLE destination;
INSERT INTO destination(i, s)
SELECT i, s FROM source_left
UNION DISTINCT
SELECT i, s FROM source_right;
SELECT COUNT(*) AS total_count, COUNT(i) AS nonnull_count FROM destination;
SELECT i, s FROM destination ORDER BY i IS NOT NULL, i, s;

TRUNCATE TABLE destination;
INSERT INTO destination(i, s)
SELECT i, s FROM source_left
INTERSECT
SELECT i, s FROM source_right;
SELECT i, s FROM destination ORDER BY i IS NOT NULL, i, s;

TRUNCATE TABLE destination;
INSERT INTO destination(i, s)
SELECT i, s FROM source_left
INTERSECT ALL
SELECT i, s FROM source_right;
SELECT i, s, COUNT(*) AS duplicate_count
FROM destination
GROUP BY i, s
ORDER BY i IS NOT NULL, i, s;

TRUNCATE TABLE destination;
INSERT INTO destination(i, s)
SELECT i, s FROM source_left
EXCEPT
SELECT i, s FROM source_right;
SELECT i, s FROM destination ORDER BY i IS NOT NULL, i, s;

TRUNCATE TABLE destination;
INSERT INTO destination(i, s)
SELECT i, s FROM source_left
MINUS
SELECT i, s FROM source_right;
SELECT i, s FROM destination ORDER BY i IS NOT NULL, i, s;

TRUNCATE TABLE destination;
INSERT INTO destination(i, s)
SELECT i, s FROM source_left
UNION ALL
SELECT i, s FROM source_right
ORDER BY i, s
LIMIT 3;
SELECT i, s, COUNT(*) AS duplicate_count
FROM destination
GROUP BY i, s
ORDER BY i IS NOT NULL, i, s;

TRUNCATE TABLE destination;
INSERT INTO destination(i, s)
(SELECT i, s FROM source_left
 UNION ALL
 SELECT i, s FROM source_right);
SELECT COUNT(*) AS parenthesized_count FROM destination;

TRUNCATE TABLE destination;
WITH source_cte AS (
    SELECT i, s FROM source_left WHERE i <= 2
)
INSERT INTO destination(i, s)
SELECT i, s FROM source_cte
UNION ALL
SELECT i, s FROM source_right WHERE i = 4;
SELECT i, s, COUNT(*) AS duplicate_count
FROM destination
GROUP BY i, s
ORDER BY i, s;

CREATE TABLE ctas_control AS
SELECT i, s FROM source_left
UNION
SELECT i, s FROM source_right;
SELECT COUNT(*) AS ctas_count, COUNT(i) AS ctas_nonnull_count FROM ctas_control;

CREATE TABLE coercion_destination(
    id BIGINT,
    amount DECIMAL(10, 2),
    label VARCHAR(20)
);
INSERT INTO coercion_destination(id, amount, label)
SELECT CAST(1 AS SMALLINT), CAST(1.25 AS DECIMAL(5, 2)), 'left'
UNION ALL
SELECT 2, 2, 'right';
SELECT id, amount, label FROM coercion_destination ORDER BY id;

TRUNCATE TABLE destination;
INSERT INTO destination(i, s)
SELECT i, s FROM source_left
UNION ALL
SELECT i FROM source_right;
SELECT COUNT(*) AS branch_width_failure_count FROM destination;

TRUNCATE TABLE destination;
INSERT INTO destination(i, s)
SELECT i FROM source_left
UNION ALL
SELECT i FROM source_right;
SELECT COUNT(*) AS target_width_failure_count FROM destination;

CREATE TABLE integer_destination(i INT);
INSERT INTO integer_destination
SELECT i FROM source_left
UNION ALL
SELECT CAST('not-an-integer' AS INT) FROM source_right;
SELECT COUNT(*) AS conversion_failure_count FROM integer_destination;

DROP DATABASE insert_set_operation;
