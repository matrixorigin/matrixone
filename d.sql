create database t;
use t;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (
    id          BIGINT PRIMARY KEY,
    reel_id     BIGINT,
    bill_no     VARCHAR(64),
    stock_num   INT,
    cur_position VARCHAR(128),
    in_time     DATETIME,
    is_deleted  TINYINT DEFAULT 0,
    tenant_id   VARCHAR(16) DEFAULT '000000'
);

CREATE TABLE t2 (
    id          BIGINT PRIMARY KEY,
    reel_id     BIGINT,
    task_id     BIGINT,
    is_deleted  TINYINT DEFAULT 0,
    tenant_id   VARCHAR(16) DEFAULT '000000'
);

-- 插入测试数据
INSERT INTO t1 (id, reel_id, bill_no, stock_num, cur_position, in_time) VALUES
(1, 100, 'BILL-001', 10, 'A-01-01', '2025-02-01 08:00:00'),
(2, 101, 'BILL-002', 20, 'A-01-02', '2025-02-02 09:00:00'),
(3, 102, 'BILL-003', 30, 'B-02-01', '2025-02-03 10:00:00'),
(4, 100, 'BILL-004',  5, 'C-03-01', '2025-02-04 11:00:00');

INSERT INTO t2 (id, reel_id, task_id) VALUES
(1, 100, 1739144656174305282),
(2, 101, 1739144656174305282),
(3, 102, 999999999999999999);

-- 验证查询
SELECT DISTINCT t1.id, bill_no, stock_num, cur_position, in_time
FROM t1
INNER JOIN t2 ON t1.reel_id = t2.reel_id
WHERE t2.task_id IN (1739144656174305282)
  AND t2.is_deleted = 0
  AND t2.tenant_id = '000000'
  AND t1.is_deleted = 0
  AND t1.tenant_id = '000000'
FOR UPDATE;
