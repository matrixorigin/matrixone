create database if not exists cte01;
use cte01;
DROP TABLE IF EXISTS temp_txn_nested_2372;
CREATE TABLE temp_txn_nested_2372 (
  id BIGINT PRIMARY KEY,
  company_id BIGINT,
  status VARCHAR(20)
);
INSERT INTO temp_txn_nested_2372
VALUES
  (1, 100, 'OK'),
  (2, 100, 'OK'),
  (3, 101, 'FAIL'),
  (4, 102, NULL),
  (5, 103, 'OK');

-- 用例 1：CTE + FOR UPDATE（历史上会 panic，应能正常执行并加锁）
START TRANSACTION;
WITH ordered AS (
  SELECT id
  FROM temp_txn_nested_2372
  WHERE status IS NOT NULL
  ORDER BY id
  LIMIT 3
)
SELECT id
FROM temp_txn_nested_2372
WHERE id IN (SELECT id FROM ordered) FOR UPDATE;
ROLLBACK;

-- 用例 2：普通 FOR UPDATE（基线对照，应一直成功）
START TRANSACTION;
SELECT id
FROM temp_txn_nested_2372
WHERE status = 'OK'
FOR UPDATE;
ROLLBACK;

-- 用例 3：仅 CTE 不加锁（验证 CTE 自身无问题）
WITH filtered AS (
  SELECT id, company_id FROM temp_txn_nested_2372 WHERE status IS NOT NULL
)
SELECT * FROM filtered ORDER BY id;
drop database cte01;