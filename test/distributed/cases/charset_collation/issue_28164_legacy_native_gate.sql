-- @suite
-- @case
-- @desc: 公开显式native域与legacy兼容域分离，失败不能改变来源或发布对象
-- @label:bvt
DROP DATABASE IF EXISTS issue_28164_collation_gate;
CREATE DATABASE issue_28164_collation_gate;
USE issue_28164_collation_gate;

-- 既有别名用于构造legacy控制；不能把新建canonical列称为历史对象。
CREATE TABLE legacy (
    id INT PRIMARY KEY,
    name VARCHAR(20) COLLATE utf8mb4_0900_ai_ci,
    token VARCHAR(20) COLLATE utf32_bin,
    KEY legacy_name (name)
) COLLATE utf8mb4_0900_ai_ci;
INSERT INTO legacy VALUES
    (1, 'abc', 'abc'), (2, 'ABC', 'ABC'),
    (3, 'café', 'café'), (4, 'cafe', 'cafe'),
    (5, 'a', 'a'), (6, 'a ', 'a ');
SELECT * FROM legacy LIMIT 0;
SELECT COUNT(*), COUNT(DISTINCT name), COUNT(DISTINCT token) FROM legacy;
SELECT id FROM legacy WHERE name = 'abc' ORDER BY id;
-- 显式转换不能借用legacy字节索引遗漏大小写、重音或PAD等价值。
SELECT id FROM legacy WHERE name COLLATE utf8mb4_general_ci = 'abc' ORDER BY id;
SELECT id FROM legacy WHERE name COLLATE utf8mb4_general_ci IN ('cafe', 'a') ORDER BY id;
SELECT 1 COLLATE utf8mb4_general_ci AS public_numeric;
SELECT NULL COLLATE utf8mb4_general_ci AS public_null;

-- 新显式列保留原值，比较/hash/order共同使用版本化域；int主键不受限制。
CREATE TABLE native_values (
    id INT PRIMARY KEY,
    ci VARCHAR(20) COLLATE utf8mb4_general_ci,
    bin VARCHAR(20) COLLATE utf8mb4_bin
);
INSERT INTO native_values SELECT id, name, token FROM legacy;
SELECT COUNT(*), COUNT(DISTINCT ci), COUNT(DISTINCT bin), COUNT(DISTINCT HEX(ci)) FROM native_values;
SELECT MIN(id) AS first_id, COUNT(*) AS group_size FROM native_values GROUP BY ci ORDER BY first_id;
SELECT id, DENSE_RANK() OVER (ORDER BY ci) AS ci_rank,
       COUNT(*) OVER (PARTITION BY bin) AS bin_peers FROM native_values ORDER BY id;
SELECT COUNT(*) AS ci_join_count FROM native_values l JOIN native_values r ON l.ci = r.ci;
SELECT COUNT(*) AS bin_join_count FROM native_values l JOIN native_values r ON l.bin = r.bin;
SELECT id, HEX(ci), HEX(bin) FROM native_values ORDER BY id;

-- 内部typed LIKE/CTAS保留域；独立SQL导出仍不承诺携带native来源。
CREATE TABLE typed_copy LIKE native_values;
INSERT INTO typed_copy SELECT * FROM native_values;
SELECT COUNT(*), COUNT(DISTINCT ci), COUNT(DISTINCT bin) FROM typed_copy;
CREATE TABLE expression_copy AS SELECT ci FROM native_values;
SELECT COUNT(*), COUNT(DISTINCT ci) FROM expression_copy;
SHOW CREATE TABLE native_values;
CREATE VIEW folded_native_expression AS SELECT 'A' COLLATE utf8mb4_general_ci = 'a' AS same_value;
SELECT same_value FROM folded_native_expression;
SELECT CAST(JSON_UNQUOTE(JSON_EXTRACT(viewdef, '$.collation_binding_revision')) AS UNSIGNED) AS binding_revision,
       CAST(JSON_UNQUOTE(JSON_EXTRACT(viewdef, '$.required_protocol_version')) AS SIGNED) AS required_protocol
FROM mo_catalog.mo_tables WHERE reldatabase = DATABASE() AND relname = 'folded_native_expression';

-- 同一PREPARE在不同参数及NULL之间重用，显式域不得漂移。
PREPARE native_filter FROM 'SELECT id FROM legacy WHERE name COLLATE utf8mb4_general_ci = ? ORDER BY id';
SET @native_needle = 'ABC';
EXECUTE native_filter USING @native_needle;
SET @native_needle = 'cafe ';
EXECUTE native_filter USING @native_needle;
SET @native_needle = NULL;
EXECUTE native_filter USING @native_needle;
SET @native_needle = 'a';
EXECUTE native_filter USING @native_needle;
DEALLOCATE PREPARE native_filter;
SET @native_needle = NULL;

-- native物理索引、持久计算与在线重标仍关闭，拒绝后目录与源值不变。
CREATE INDEX rejected_native_key ON native_values (ci);
SELECT COUNT(*) AS rejected_index_count FROM information_schema.statistics
WHERE table_schema = DATABASE() AND table_name = 'native_values' AND index_name = 'rejected_native_key';
CREATE TABLE rejected_computation (v INT DEFAULT (LENGTH('a' COLLATE utf8mb4_general_ci)));
SELECT COUNT(*) AS rejected_table_count FROM information_schema.tables
WHERE table_schema = DATABASE() AND table_name = 'rejected_computation';
ALTER TABLE native_values MODIFY ci VARCHAR(20) COLLATE utf8mb4_bin;
SELECT COUNT(*), COUNT(DISTINCT ci), COUNT(DISTINCT bin) FROM native_values;
SELECT id, HEX(ci), HEX(bin) FROM native_values ORDER BY id;

-- 保留既有不支持名称、legacy持久表达式、错误后元数据控制。
CREATE TABLE native_column (name VARCHAR(20) COLLATE utf8mb4_0900_bin);
CREATE TABLE native_default (id INT) COLLATE utf8mb4_0900_bin;
CREATE TABLE folded_legacy_expression (v INT DEFAULT (LENGTH('a' COLLATE utf8mb4_0900_ai_ci)));
INSERT INTO folded_legacy_expression VALUES (DEFAULT);
SELECT v FROM folded_legacy_expression;
ALTER TABLE legacy DEFAULT COLLATE utf8mb4_0900_bin;
ALTER TABLE legacy CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
SELECT COLUMN_NAME, COLLATION_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='legacy' ORDER BY ORDINAL_POSITION;
SELECT COUNT(*), COUNT(DISTINCT name), COUNT(DISTINCT token) FROM legacy;
SHOW TABLES;
DROP DATABASE issue_28164_collation_gate;
