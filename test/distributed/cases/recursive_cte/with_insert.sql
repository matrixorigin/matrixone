-- Test WITH clause support for INSERT statement (Issue #22583)

DROP DATABASE IF EXISTS test_with_insert;
CREATE DATABASE test_with_insert;
USE test_with_insert;

-- Create test table
CREATE TABLE t1 (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    value INT
);

-- Insert initial data
INSERT INTO t1 VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30);

-- Test 1: Basic WITH ... INSERT ... SELECT
WITH cte AS (SELECT * FROM t1 WHERE id = 1)
INSERT INTO t1 SELECT id + 10, name, value FROM cte;

SELECT * FROM t1 ORDER BY id;

-- Test 2: WITH ... INSERT ... SELECT with WHERE clause in CTE
WITH cte AS (SELECT * FROM t1 WHERE value > 15)
INSERT INTO t1 SELECT id + 20, name, value * 2 FROM cte;

SELECT * FROM t1 ORDER BY id;

-- Test 3: WITH ... INSERT ... SELECT with JOIN in CTE
CREATE TABLE t2 (id INT, category VARCHAR(20));
INSERT INTO t2 VALUES (1, 'cat1'), (2, 'cat2'), (3, 'cat3');

WITH cte AS (
    SELECT t1.id, t1.name, t1.value, t2.category 
    FROM t1 JOIN t2 ON t1.id = t2.id 
    WHERE t1.id <= 3
)
INSERT INTO t1 SELECT id + 30, name, value FROM cte;

SELECT * FROM t1 ORDER BY id;

-- Test 4: Multiple CTEs
WITH 
    cte1 AS (SELECT * FROM t1 WHERE id = 1),
    cte2 AS (SELECT * FROM cte1 WHERE value = 10)
INSERT INTO t1 SELECT id + 40, name, value FROM cte2;

SELECT * FROM t1 ORDER BY id;

-- Test 5: WITH ... INSERT with column list
WITH cte AS (SELECT id, name FROM t1 WHERE id = 2)
INSERT INTO t1 (id, name, value) SELECT id + 50, name, 999 FROM cte;

SELECT * FROM t1 WHERE id > 50 ORDER BY id;

-- Test 6: Verify WITH ... DELETE still works
WITH cte AS (SELECT id FROM t1 WHERE id > 40)
DELETE FROM t1 WHERE id IN (SELECT id FROM cte);

SELECT * FROM t1 ORDER BY id;

-- Test 7: Verify WITH ... UPDATE still works  
WITH cte AS (SELECT id FROM t1 WHERE id < 15)
UPDATE t1 SET value = value + 100 WHERE id IN (SELECT id FROM cte);

SELECT * FROM t1 ORDER BY id;

-- Cleanup
DROP DATABASE test_with_insert;
