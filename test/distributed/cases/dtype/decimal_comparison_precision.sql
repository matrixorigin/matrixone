-- @suite
-- @case
-- @desc: 隐式 DECIMAL 比较对齐保留整数位容量，显式转换仍检查精度
-- @label:bvt

DROP DATABASE IF EXISTS decimal_comparison_precision;
CREATE DATABASE decimal_comparison_precision;
USE decimal_comparison_precision;

CREATE TABLE scale_pairs (id INT PRIMARY KEY, a DECIMAL(6,5), b DECIMAL(5,4));
INSERT INTO scale_pairs VALUES (1, 9.22340, 9.2234), (2, -9.22340, -9.2234), (3, NULL, NULL);

-- 非常量列保证执行期 CAST、JOIN 键与谓词均走真实输入路径。
SELECT l.id, r.id FROM scale_pairs l JOIN scale_pairs r ON l.a = r.b ORDER BY l.id, r.id;
SELECT id FROM scale_pairs WHERE a = b ORDER BY id;
SELECT id FROM scale_pairs WHERE b = a ORDER BY id;
SELECT id FROM scale_pairs WHERE a <=> b ORDER BY id;
SELECT id FROM scale_pairs WHERE a IN (SELECT b FROM scale_pairs) ORDER BY id;
SELECT id, a IN (b), CASE a WHEN b THEN 1 ELSE 0 END FROM scale_pairs ORDER BY id;
SELECT id FROM scale_pairs WHERE (a, id) = (b, id) ORDER BY id;
SELECT id, a <> b, a < b, a <= b, a > b, a >= b FROM scale_pairs ORDER BY id;
SELECT id, CASE WHEN id = 1 THEN a ELSE b END AS chosen FROM scale_pairs ORDER BY id;

-- 三个输入均须对齐；上下界小于、等于和大于待测值的控制。
SELECT IN_RANGE(10.50, 0.0, 15.0, 0);
SELECT IN_RANGE(10.5, 0.00, 15.0, 0);
SELECT IN_RANGE(10.5, 0.0, 15.00, 0);
SELECT IN_RANGE(10.50, 0.0, 10.5, 0), IN_RANGE(10.50, 0.0, 10.4, 0);
SELECT id, IN_RANGE(a, -9.2234, 9.2234, 0) FROM scale_pairs ORDER BY id;

-- 最大整数容量的 scale 增长要求提升物理位宽，而不是拒绝合法操作数。
CREATE TABLE wide_pairs (a DECIMAL(18,0), b DECIMAL(18,1), c DECIMAL(38,0), d DECIMAL(38,1));
INSERT INTO wide_pairs VALUES (999999999999999999, 99999999999999999.9,
99999999999999999999999999999999999999, 9999999999999999999999999999999999999.9);
SELECT a = b, a <=> b, c = d, c <=> d FROM wide_pairs;
SELECT IN_RANGE(a, 99999999999999999.9, 999999999999999999, 0),
IN_RANGE(c, 9999999999999999999999999999999999999.9,
99999999999999999999999999999999999999, 0) FROM wide_pairs;

-- 高精度声明超出公共系数容量，但实际可表示的值仍可比较。
SELECT CAST(1 AS DECIMAL(65,0)) = CAST(1 AS DECIMAL(65,30)),
       CAST(1 AS DECIMAL(65,0)) <=> CAST(1 AS DECIMAL(65,30));
SELECT CAST(2 AS DECIMAL(65,0)) = CAST(1 AS DECIMAL(65,30)),
       CAST(-1 AS DECIMAL(65,0)) <=> CAST(1 AS DECIMAL(65,30));
CREATE TABLE high_precision_pairs (a DECIMAL(65,0), b DECIMAL(65,30));
INSERT INTO high_precision_pairs VALUES (1, 1), (2, 1), (-1, 1);
SELECT a = b, a <=> b FROM high_precision_pairs ORDER BY a;

-- 显式 CAST 保持既有的目标精度边界截断行为，不扩大用户指定的域。
SELECT CAST(a AS DECIMAL(5,5)) FROM scale_pairs WHERE id = 1;
SELECT CAST(a AS DECIMAL(5,5)) FROM scale_pairs WHERE id = 2;
DELETE FROM scale_pairs WHERE a = b;
SELECT id FROM scale_pairs ORDER BY id;

DROP DATABASE decimal_comparison_precision;
