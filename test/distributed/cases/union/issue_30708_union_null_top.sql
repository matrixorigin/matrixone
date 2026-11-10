-- @case
-- @desc: 保持 NULL UNION ALL 批次下 Top 排序键的列布局稳定
-- @label:bvt

DROP DATABASE IF EXISTS issue_30708_bvt;
CREATE DATABASE issue_30708_bvt;
USE issue_30708_bvt;

CREATE TABLE issue_30708_top (x INT);
INSERT INTO issue_30708_top VALUES (1), (2);

-- 普通列和 NULL 的两种输入分支顺序，检查完整排序结果及后续查询。
-- @sortkey:0
SELECT NULL AS x
UNION ALL
SELECT x FROM issue_30708_top WHERE x = 2
ORDER BY x DESC
LIMIT 2;
SELECT COUNT(*) AS source_rows FROM issue_30708_top;

-- @sortkey:0
SELECT x FROM issue_30708_top WHERE x = 2
UNION ALL
SELECT NULL AS x
ORDER BY x DESC
LIMIT 2;

-- 最近的对照：全排序、UNION DISTINCT 和普通表上的 Top。
-- @sortkey:0
SELECT NULL AS x
UNION ALL
SELECT x FROM issue_30708_top WHERE x = 2
ORDER BY x DESC;
-- @sortkey:0
SELECT NULL AS x
UNION
SELECT x FROM issue_30708_top WHERE x = 2
ORDER BY x DESC;
-- @sortkey:0
SELECT x FROM issue_30708_top ORDER BY x DESC LIMIT 2;

-- LIMIT 边界和跨越 NULL 行的 OFFSET。
SELECT NULL AS x
UNION ALL
SELECT x FROM issue_30708_top WHERE x = 2
ORDER BY x DESC LIMIT 0;
SELECT NULL AS x
UNION ALL
SELECT x FROM issue_30708_top WHERE x = 2
ORDER BY x DESC LIMIT 1;
-- @sortkey:0
SELECT NULL AS x
UNION ALL
SELECT x FROM issue_30708_top WHERE x = 2
ORDER BY x DESC LIMIT 3;
SELECT NULL AS x
UNION ALL
SELECT x FROM issue_30708_top WHERE x = 2
ORDER BY x DESC LIMIT 1 OFFSET 1;

-- 默认 NULL 顺序，以及与直接列共存的计算排序键。
-- @sortkey:0
SELECT NULL AS x
UNION ALL
SELECT x FROM issue_30708_top WHERE x = 2
ORDER BY x ASC LIMIT 2;
-- @sortkey:0
SELECT NULL AS x
UNION ALL
SELECT x FROM issue_30708_top WHERE x = 2
ORDER BY x IS NULL DESC, x DESC LIMIT 2;
SELECT NULL AS x UNION ALL SELECT 1 AS x;

-- Repeated keys keep the first direction in resident and spill Top.
SELECT NULL AS x UNION ALL SELECT x FROM issue_30708_top
ORDER BY x DESC, x ASC LIMIT 3;
SELECT NULL AS x UNION ALL SELECT x FROM issue_30708_top
ORDER BY x ASC, x DESC LIMIT 3;
SELECT NULL AS x UNION ALL SELECT x FROM issue_30708_top
ORDER BY x DESC, x ASC LIMIT 1 OFFSET 1;
SELECT NULL AS x UNION ALL SELECT x FROM issue_30708_top
ORDER BY x DESC, x ASC LIMIT 16385;
SELECT NULL AS x UNION ALL SELECT x FROM issue_30708_top
ORDER BY x DESC, x ASC;

-- 同一预处理语句的不同参数及空表分支。
PREPARE issue_30708_stmt FROM
    'SELECT NULL AS x UNION ALL
     SELECT x FROM issue_30708_top WHERE x = ?
     ORDER BY x DESC LIMIT ?';
SET @issue_30708_key = 2;
SET @issue_30708_limit = 2;
-- @sortkey:0
EXECUTE issue_30708_stmt USING @issue_30708_key, @issue_30708_limit;
SET @issue_30708_key = 1;
SET @issue_30708_limit = 1;
EXECUTE issue_30708_stmt USING @issue_30708_key, @issue_30708_limit;
SET @issue_30708_key = 99;
SET @issue_30708_limit = 2;
EXECUTE issue_30708_stmt USING @issue_30708_key, @issue_30708_limit;
DEALLOCATE PREPARE issue_30708_stmt;

-- 明细与合计行：typed NULL 日期、隐藏排序列、格式化投影与分页组合。
CREATE TABLE top_bill (bill_no VARCHAR(20), amount DECIMAL(10, 2), due DATE);
INSERT INTO top_bill VALUES ('A', 10, '2026-01-02'), ('B', 20, '2026-01-01');
-- @sortkey:0
SELECT bill_no, FORMAT(amount, 2) AS amount, due
FROM (
    SELECT 1 AS sort_order, bill_no, amount, due, due AS sort_date
    FROM top_bill
    UNION ALL
    SELECT 2 AS sort_order, CAST('total' AS CHAR(20)), SUM(amount),
           CAST(NULL AS DATE), CAST(NULL AS DATE)
    FROM top_bill
) AS combined
ORDER BY sort_order, sort_date DESC, bill_no
LIMIT 3 OFFSET 0;
-- @sortkey:0
SELECT bill_no, FORMAT(amount, 2) AS amount, due
FROM (
    SELECT 2 AS sort_order, CAST('total' AS CHAR(20)) AS bill_no, SUM(amount) AS amount,
           CAST(NULL AS DATE) AS due, CAST(NULL AS DATE) AS sort_date
    FROM top_bill
    UNION ALL
    SELECT 1 AS sort_order, bill_no, amount, due, due AS sort_date
    FROM top_bill
) AS combined
ORDER BY sort_order, sort_date DESC, bill_no
LIMIT 2 OFFSET 1;

DROP TABLE top_bill;
DROP TABLE issue_30708_top;
DROP DATABASE issue_30708_bvt;
