-- =====================================================
-- INSERT ... ON DUPLICATE KEY UPDATE 测试套件
-- 测试目标：验证 INSERT ON DUPLICATE KEY UPDATE 语句的各种场景
-- 
-- 重要限制：MatrixOne 不支持更新主键列和唯一键列
-- - 如果尝试更新主键列或唯一键列，会返回错误
-- - 对于复合键，只要涉及其中任何一列都不支持更新
-- - 如果主键不冲突但唯一键冲突，MO 不支持 ON DUPLICATE KEY UPDATE, 报错
-- =====================================================

-- =====================================================
-- 第一部分：索引相关测试（场景 1-6）
-- 所有涉及索引的测试集中在此部分
-- =====================================================

-- =====================================================
-- 测试场景 1: UNIQUE KEY 约束（无主键）
-- 测试点：唯一索引冲突时更新非唯一键列
-- =====================================================
DROP TABLE IF EXISTS indup_00;
CREATE TABLE indup_00(
    `id` INT UNSIGNED,
    `act_name` VARCHAR(20) NOT NULL,
    `spu_id` VARCHAR(30) NOT NULL,
    `uv`  BIGINT NOT NULL,
    `update_time` DATE DEFAULT '2020-10-10' COMMENT 'latest time',
    UNIQUE KEY idx_act_name_spu_id_0 (act_name, spu_id)
);

-- 准备索引行数检查
SET @idxsql = CONCAT("SELECT * FROM `", (SELECT DISTINCT index_table_name FROM mo_catalog.mo_indexes WHERE name = 'idx_act_name_spu_id_0'), "`");
PREPARE check_idx FROM @idxsql;

-- 初始化测试数据
INSERT INTO indup_00 VALUES 
    (1,'beijing','001',1,'2021-01-03'),
    (2,'shanghai','002',2,'2022-09-23'),
    (3,'guangzhou','003',3,'2022-09-23');
SELECT * FROM indup_00;
EXECUTE check_idx;

-- 测试 1.1: 唯一索引冲突，只更新非唯一键列（id, uv, update_time）
INSERT INTO indup_00 VALUES (100,'beijing','001',999,'2023-01-01') 
ON DUPLICATE KEY UPDATE `id`=VALUES(`id`), `uv`=VALUES(`uv`), `update_time`=VALUES(`update_time`);
SELECT * FROM indup_00;
EXECUTE check_idx;

-- 测试 1.2: 批量插入，部分冲突，更新非唯一键列
INSERT INTO indup_00 VALUES 
    (200,'shanghai','002',888,'2023-02-01'),
    (4,'shenzhen','004',4,'2023-02-01')
ON DUPLICATE KEY UPDATE `uv`=VALUES(`uv`), `update_time`=VALUES(`update_time`);
SELECT * FROM indup_00;
EXECUTE check_idx;

-- 测试 1.3: 使用表达式更新非唯一键列
INSERT INTO indup_00 VALUES (999,'guangzhou','003',100,'2023-03-01') 
ON DUPLICATE KEY UPDATE `uv`=`uv`+100, `id`=`id`+1000;
SELECT * FROM indup_00;
EXECUTE check_idx;

-- 测试 1.4: 更新为 NULL（可为空列）
INSERT INTO indup_00 VALUES (999,'beijing','001',999,'2023-04-01') 
ON DUPLICATE KEY UPDATE `update_time`=NULL;
SELECT * FROM indup_00;
EXECUTE check_idx;

-- 测试 1.5: 无冲突数据，正常插入
INSERT INTO indup_00 VALUES 
    (5,'chengdu','005',5,'2023-05-01'),
    (6,'xian','006',6,'2023-05-02')
ON DUPLICATE KEY UPDATE `id`=999;
SELECT * FROM indup_00;
EXECUTE check_idx;


-- =====================================================
-- 测试场景 2: PRIMARY KEY + UNIQUE KEY 组合约束
-- 测试点：主键和唯一索引同时存在时的冲突处理
-- =====================================================
DROP TABLE IF EXISTS indup_01;
CREATE TABLE indup_01(
    `id` INT UNSIGNED,
    `act_name` VARCHAR(20) NOT NULL,
    `spu_id` VARCHAR(30) NOT NULL,
    `uv`  BIGINT NOT NULL,
    `update_time` DATE DEFAULT '2020-10-10' COMMENT 'latest time',
    PRIMARY KEY (`id`),
    UNIQUE KEY idx_act_name_spu_id_1 (act_name, spu_id)
);

-- 准备索引行数检查
SET @idxsql = CONCAT("SELECT COUNT(*) FROM `", (SELECT DISTINCT index_table_name FROM mo_catalog.mo_indexes WHERE name = 'idx_act_name_spu_id_1'), "`");
PREPARE check_idx FROM @idxsql;

-- 初始化测试数据
INSERT INTO indup_01 VALUES 
    (1,'beijing','001',1,'2021-01-03'),
    (2,'shanghai','002',2,'2022-09-23'),
    (3,'guangzhou','003',3,'2022-09-23');
SELECT * FROM indup_01;
EXECUTE check_idx;

-- 测试 2.1: 主键冲突，更新非键列
INSERT INTO indup_01 VALUES (1,'newcity','999',999,'2023-01-01') 
ON DUPLICATE KEY UPDATE `uv`=VALUES(`uv`), `update_time`=VALUES(`update_time`);
SELECT * FROM indup_01;
EXECUTE check_idx;

-- 测试 2.2: 唯一索引冲突但主键不冲突（预期报错：MO 不支持此场景）
-- @regex("Duplicate entry '\(beijing,001\)'", true)
INSERT INTO indup_01 VALUES (100,'beijing','001',888,'2023-02-01') 
ON DUPLICATE KEY UPDATE `uv`=VALUES(`uv`), `update_time`=VALUES(`update_time`);
EXECUTE check_idx;

-- 测试 2.3: 批量插入，混合冲突和非冲突
INSERT INTO indup_01 VALUES 
    (4,'shenzhen','004',4,'2023-03-01'),
    (2,'anyname','anyid',777,'2023-03-02')
ON DUPLICATE KEY UPDATE `uv`=VALUES(`uv`);
SELECT * FROM indup_01;
EXECUTE check_idx;


-- =====================================================
-- 测试场景 3: 复合 UNIQUE KEY（无主键）
-- 测试点：多列唯一索引的冲突处理
-- =====================================================
DROP TABLE IF EXISTS indup_04;
CREATE TABLE indup_04(
    id INT,
    col1 VARCHAR(20) NOT NULL,
    col2 VARCHAR(20) NOT NULL,
    value INT,
    UNIQUE KEY uk_col1_col2_3 (col1, col2)
);

-- 准备索引行数检查
SET @idxsql = CONCAT("SELECT COUNT(*) FROM `", (SELECT DISTINCT index_table_name FROM mo_catalog.mo_indexes WHERE name = 'uk_col1_col2_3'), "`");
PREPARE check_idx FROM @idxsql;

-- 初始化测试数据
INSERT INTO indup_04 VALUES 
    (1,'a','b',100),
    (2,'c','d',200),
    (3,'e','f',300);
SELECT * FROM indup_04;
EXECUTE check_idx;

-- 测试 3.1: 复合唯一键冲突，更新非唯一键列
INSERT INTO indup_04 VALUES (999,'a','b',999) 
ON DUPLICATE KEY UPDATE id=VALUES(id), value=VALUES(value);
SELECT * FROM indup_04;
EXECUTE check_idx;

-- 测试 3.2: 复合唯一键部分相同（不冲突），正常插入
INSERT INTO indup_04 VALUES (4,'a','x',400);
SELECT * FROM indup_04;
EXECUTE check_idx;

-- 测试 3.3: 批量更新
INSERT INTO indup_04 VALUES 
    (888,'c','d',888),
    (777,'e','f',777)
ON DUPLICATE KEY UPDATE value=value+VALUES(value);
SELECT * FROM indup_04;
EXECUTE check_idx;


-- =====================================================
-- 测试场景 4: 带普通索引（非唯一）的表
-- 测试点：验证普通索引不影响 ON DUPLICATE KEY UPDATE 的判断逻辑
-- =====================================================

-- 测试 4.1: 复合主键 + 单列普通索引
DROP TABLE IF EXISTS indup_idx_01;
CREATE TABLE indup_idx_01(a INT, b INT, c INT, PRIMARY KEY(a, b), KEY idx_c_4 (c));

-- 准备索引行数检查
SET @idxsql = CONCAT("SELECT COUNT(*) FROM `", (SELECT DISTINCT index_table_name FROM mo_catalog.mo_indexes WHERE name = 'idx_c_4'), "`");
PREPARE check_idx FROM @idxsql;

INSERT INTO indup_idx_01 VALUES (1,1,1);
EXECUTE check_idx;

INSERT INTO indup_idx_01 VALUES (1,1,2), (2,2,2) ON DUPLICATE KEY UPDATE c=c+10;
SELECT * FROM indup_idx_01 ORDER BY a, b;
EXECUTE check_idx;

DELETE FROM indup_idx_01;
INSERT INTO indup_idx_01 VALUES (1,1,1);

-- DELETE 可能导致表重建，重新准备索引检查
SET @idxsql = CONCAT("SELECT COUNT(*) FROM `", (SELECT DISTINCT index_table_name FROM mo_catalog.mo_indexes WHERE name = 'idx_c_4'), "`");
PREPARE check_idx FROM @idxsql;
EXECUTE check_idx;

INSERT INTO indup_idx_01 VALUES (1,1,2), (2,2,2) ON DUPLICATE KEY UPDATE c=VALUES(c)+10;
SELECT * FROM indup_idx_01 ORDER BY a, b;
EXECUTE check_idx;

DROP TABLE indup_idx_01;

-- 测试 4.2: 复合主键 + 复合普通索引
CREATE TABLE indup_idx_01(a INT, b INT, c INT, PRIMARY KEY(a, b), KEY idx_bc_4 (b, c));

-- 准备索引行数检查
SET @idxsql = CONCAT("SELECT COUNT(*) FROM `", (SELECT DISTINCT index_table_name FROM mo_catalog.mo_indexes WHERE name = 'idx_bc_4'), "`");
PREPARE check_idx FROM @idxsql;

INSERT INTO indup_idx_01 VALUES (1,1,1);
EXECUTE check_idx;

INSERT INTO indup_idx_01 VALUES (1,1,2), (2,2,2), (2,2,3) ON DUPLICATE KEY UPDATE c=c+10;
SELECT * FROM indup_idx_01 ORDER BY a, b;
EXECUTE check_idx;

DELETE FROM indup_idx_01;
INSERT INTO indup_idx_01 VALUES (1,1,1);

-- DELETE 可能导致表重建，重新准备索引检查
SET @idxsql = CONCAT("SELECT COUNT(*) FROM `", (SELECT DISTINCT index_table_name FROM mo_catalog.mo_indexes WHERE name = 'idx_bc_4'), "`");
PREPARE check_idx FROM @idxsql;
EXECUTE check_idx;

INSERT INTO indup_idx_01 VALUES (1,1,2), (2,2,2), (2,2,3) ON DUPLICATE KEY UPDATE c=VALUES(c)+10;
SELECT * FROM indup_idx_01 ORDER BY a, b;
EXECUTE check_idx;

DROP TABLE indup_idx_01;


-- =====================================================
-- 测试场景 4.3: UNIQUE KEY + 普通索引混合
-- 测试点：验证唯一索引和普通索引同时存在时的行为
-- =====================================================

DROP TABLE IF EXISTS indup_idx_mix_01;
CREATE TABLE indup_idx_mix_01(
    id INT,
    email VARCHAR(50) NOT NULL,
    name VARCHAR(50),
    age INT,
    status VARCHAR(20),
    UNIQUE KEY uk_email_4 (email),
    KEY idx_name_4 (name),
    KEY idx_age_status_4 (age, status)
);

-- 准备唯一索引行数检查
SET @uk_idxsql = CONCAT("SELECT COUNT(*) FROM `", (SELECT DISTINCT index_table_name FROM mo_catalog.mo_indexes WHERE name = 'uk_email_4'), "`");
PREPARE check_uk_idx FROM @uk_idxsql;

-- 准备普通索引行数检查
SET @idx_name_sql = CONCAT("SELECT COUNT(*) FROM `", (SELECT DISTINCT index_table_name FROM mo_catalog.mo_indexes WHERE name = 'idx_name_4'), "`");
PREPARE check_idx_name FROM @idx_name_sql;

SET @idx_age_sql = CONCAT("SELECT COUNT(*) FROM `", (SELECT DISTINCT index_table_name FROM mo_catalog.mo_indexes WHERE name = 'idx_age_status_4'), "`");
PREPARE check_idx_age FROM @idx_age_sql;

-- 初始化数据
INSERT INTO indup_idx_mix_01 VALUES 
    (1, 'alice@example.com', 'Alice', 25, 'active'),
    (2, 'bob@example.com', 'Bob', 30, 'active'),
    (3, 'charlie@example.com', 'Charlie', 28, 'inactive');
SELECT * FROM indup_idx_mix_01;
EXECUTE check_uk_idx;
EXECUTE check_idx_name;
EXECUTE check_idx_age;

-- 测试 4.3.1: 唯一键冲突，更新普通索引列和普通列
INSERT INTO indup_idx_mix_01 VALUES (100, 'alice@example.com', 'Alice Updated', 26, 'inactive')
ON DUPLICATE KEY UPDATE 
    name = VALUES(name),  -- 更新普通索引列
    age = VALUES(age),    -- 更新复合普通索引列
    status = VALUES(status);  -- 更新复合普通索引列
SELECT * FROM indup_idx_mix_01;
EXECUTE check_uk_idx;
EXECUTE check_idx_name;
EXECUTE check_idx_age;

-- 测试 4.3.2: 只更新部分普通索引列
INSERT INTO indup_idx_mix_01 VALUES (200, 'bob@example.com', 'Bob New', 35, 'inactive')
ON DUPLICATE KEY UPDATE 
    name = VALUES(name),  -- 更新 name（有普通索引）
    id = VALUES(id);      -- 更新 id（无索引）
    -- age 和 status 不更新
SELECT * FROM indup_idx_mix_01;
EXECUTE check_uk_idx;
EXECUTE check_idx_name;
EXECUTE check_idx_age;

-- 测试 4.3.3: 只更新普通列，不更新任何索引列
INSERT INTO indup_idx_mix_01 VALUES (300, 'charlie@example.com', 'Charlie New', 40, 'active')
ON DUPLICATE KEY UPDATE 
    id = VALUES(id);  -- 只更新无索引的列
SELECT * FROM indup_idx_mix_01;
EXECUTE check_uk_idx;
EXECUTE check_idx_name;
EXECUTE check_idx_age;

-- 测试 4.3.4: 批量操作，混合更新索引列和普通列
INSERT INTO indup_idx_mix_01 VALUES 
    (400, 'alice@example.com', 'Alice Final', 27, 'active'),
    (4, 'david@example.com', 'David', 32, 'active')
ON DUPLICATE KEY UPDATE 
    name = VALUES(name),
    age = age + 1,
    id = VALUES(id);
SELECT * FROM indup_idx_mix_01 ORDER BY id;
EXECUTE check_uk_idx;
EXECUTE check_idx_name;
EXECUTE check_idx_age;

DROP TABLE indup_idx_mix_01;


-- =====================================================
-- 测试场景 4.4: 主键 + 唯一索引 + 多个普通索引
-- 测试点：验证复杂索引组合下的更新行为
-- =====================================================

DROP TABLE IF EXISTS indup_idx_complex;
CREATE TABLE indup_idx_complex(
    id INT PRIMARY KEY,
    email VARCHAR(50),
    username VARCHAR(50),
    phone VARCHAR(20),
    address VARCHAR(100),
    city VARCHAR(50),
    score INT,
    UNIQUE KEY uk_email_4 (email),
    UNIQUE KEY uk_username_4 (username),
    KEY idx_phone_4 (phone),
    KEY idx_city_score_4 (city, score)
);

-- 准备所有索引检查
SET @uk_email_sql = CONCAT("SELECT COUNT(*) FROM `", (SELECT DISTINCT index_table_name FROM mo_catalog.mo_indexes WHERE name = 'uk_email_4'), "`");
PREPARE check_uk_email FROM @uk_email_sql;

SET @uk_username_sql = CONCAT("SELECT COUNT(*) FROM `", (SELECT DISTINCT index_table_name FROM mo_catalog.mo_indexes WHERE name = 'uk_username_4'), "`");
PREPARE check_uk_username FROM @uk_username_sql;

SET @idx_phone_sql = CONCAT("SELECT COUNT(*) FROM `", (SELECT DISTINCT index_table_name FROM mo_catalog.mo_indexes WHERE name = 'idx_phone_4'), "`");
PREPARE check_idx_phone FROM @idx_phone_sql;

SET @idx_city_sql = CONCAT("SELECT COUNT(*) FROM `", (SELECT DISTINCT index_table_name FROM mo_catalog.mo_indexes WHERE name = 'idx_city_score_4'), "`");
PREPARE check_idx_city FROM @idx_city_sql;

-- 初始化数据
INSERT INTO indup_idx_complex VALUES 
    (1, 'user1@test.com', 'user1', '1234567890', 'Address 1', 'Beijing', 100),
    (2, 'user2@test.com', 'user2', '1234567891', 'Address 2', 'Shanghai', 200);
SELECT * FROM indup_idx_complex;
EXECUTE check_uk_email;
EXECUTE check_uk_username;
EXECUTE check_idx_phone;
EXECUTE check_idx_city;

-- 测试 4.4.1: 主键冲突，更新所有普通索引列
INSERT INTO indup_idx_complex VALUES 
    (1, 'new1@test.com', 'newuser1', '9999999999', 'New Address 1', 'Guangzhou', 150)
ON DUPLICATE KEY UPDATE 
    phone = VALUES(phone),      -- 更新普通索引列
    city = VALUES(city),        -- 更新复合普通索引列
    score = VALUES(score),      -- 更新复合普通索引列
    address = VALUES(address);  -- 更新普通列
SELECT * FROM indup_idx_complex;
EXECUTE check_uk_email;
EXECUTE check_uk_username;
EXECUTE check_idx_phone;
EXECUTE check_idx_city;

-- 测试 4.4.2: 主键冲突，只更新部分普通索引列
INSERT INTO indup_idx_complex VALUES 
    (2, 'new2@test.com', 'newuser2', '8888888888', 'New Address 2', 'Shenzhen', 250)
ON DUPLICATE KEY UPDATE 
    phone = VALUES(phone),   -- 更新普通索引列
    address = VALUES(address);  -- 更新普通列
    -- city 和 score 不更新（复合普通索引）
SELECT * FROM indup_idx_complex;
EXECUTE check_uk_email;
EXECUTE check_uk_username;
EXECUTE check_idx_phone;
EXECUTE check_idx_city;

-- 测试 4.4.3: 主键冲突，使用表达式更新普通索引列
INSERT INTO indup_idx_complex VALUES 
    (1, 'expr@test.com', 'expruser', '0000000000', 'Expr Address', 'Chengdu', 999)
ON DUPLICATE KEY UPDATE 
    phone = CONCAT(phone, '-ext'),  -- 表达式更新普通索引列
    city = UPPER(city),              -- 表达式更新复合普通索引列
    score = score + 50,              -- 表达式更新复合普通索引列
    address = CONCAT(address, ' (Updated)');
SELECT * FROM indup_idx_complex;
EXECUTE check_uk_email;
EXECUTE check_uk_username;
EXECUTE check_idx_phone;
EXECUTE check_idx_city;

-- 测试 4.4.4: 批量插入，混合更新
INSERT INTO indup_idx_complex VALUES 
    (1, 'batch1@test.com', 'batch1', '1111111111', 'Batch 1', 'City1', 300),
    (3, 'batch3@test.com', 'batch3', '3333333333', 'Batch 3', 'City3', 300)
ON DUPLICATE KEY UPDATE 
    phone = VALUES(phone),
    score = score + VALUES(score);
SELECT * FROM indup_idx_complex ORDER BY id;
EXECUTE check_uk_email;
EXECUTE check_uk_username;
EXECUTE check_idx_phone;
EXECUTE check_idx_city;

DROP TABLE indup_idx_complex;


-- =====================================================
-- 测试场景 5: AUTO_INCREMENT + UNIQUE KEY
-- 测试点：包含自增列和唯一索引的表
-- =====================================================
DROP TABLE IF EXISTS indup_09;
CREATE TABLE indup_09(
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    biz_type BIGINT,
    namespace VARCHAR(64),
    sn BIGINT DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_biz_ns_5 (biz_type, namespace)
);

-- 准备索引行数检查
SET @idxsql = CONCAT("SELECT COUNT(*) FROM `", (SELECT DISTINCT index_table_name FROM mo_catalog.mo_indexes WHERE name = 'uk_biz_ns_5'), "`");
PREPARE check_idx FROM @idxsql;

-- 初始化测试数据
INSERT INTO indup_09 (biz_type, namespace, sn) VALUES 
    (1, 'ns1', 1),
    (2, 'ns2', 1),
    (3, 'ns3', 1);
SELECT * FROM indup_09;
EXECUTE check_idx;

-- 测试 5.1: 唯一索引冲突但主键不冲突（预期报错：MO 不支持此场景）
-- @regex("Duplicate entry '\(1,ns1\)'", true)
INSERT INTO indup_09 (biz_type, namespace, sn) VALUES (1, 'ns1', 999) 
ON DUPLICATE KEY UPDATE sn = sn + 1;

-- 测试 5.2: 多次尝试唯一键冲突更新（预期报错）
-- @regex("Duplicate entry '\(1,ns1\)'", true)
INSERT INTO indup_09 (biz_type, namespace, sn) VALUES (1, 'ns1', 999) 
ON DUPLICATE KEY UPDATE sn = sn + 1;

-- 测试 5.3: 使用主键冲突的方式更新（应该成功）
INSERT INTO indup_09 (id, biz_type, namespace, sn) VALUES (1, 999, 'ns999', 999) 
ON DUPLICATE KEY UPDATE sn = sn + 1;
SELECT * FROM indup_09 WHERE id = 1;
EXECUTE check_idx;

-- 测试 5.4: 插入新记录，自增ID自动增长
INSERT INTO indup_09 (biz_type, namespace, sn) VALUES (100, 'ns100', 1) 
ON DUPLICATE KEY UPDATE sn = sn + 1;
SELECT * FROM indup_09 WHERE biz_type = 100;
EXECUTE check_idx;


-- =====================================================
-- 测试场景 6: 外键约束
-- 测试点：在有外键约束的表上进行 ON DUPLICATE KEY UPDATE
-- =====================================================
DROP TABLE IF EXISTS indup_fk2;
DROP TABLE IF EXISTS indup_fk1;

CREATE TABLE indup_fk1(
    col1 INT PRIMARY KEY,
    col2 VARCHAR(25),
    col3 TINYINT
);

CREATE TABLE indup_fk2(
    col1 INT,
    col2 VARCHAR(25),
    col3 TINYINT PRIMARY KEY,
    CONSTRAINT ck FOREIGN KEY(col1) REFERENCES indup_fk1(col1) 
        ON DELETE RESTRICT ON UPDATE RESTRICT
);

-- 初始化测试数据
INSERT INTO indup_fk1 VALUES (2,'yellow',20),(10,'apple',50),(11,'oppo',51);
INSERT INTO indup_fk2 VALUES (2,'score',1),(2,'student',4),(10,'goods',2);
SELECT * FROM indup_fk1;
SELECT * FROM indup_fk2;

-- 测试 6.1: 冲突时更新外键列为已存在的值
INSERT INTO indup_fk2 VALUES (99,'food',1) 
ON DUPLICATE KEY UPDATE col1=11, col2=VALUES(col2);
SELECT * FROM indup_fk2;

-- 测试 6.2: 冲突时更新非外键列
INSERT INTO indup_fk2 VALUES (99,'updated_food',1) 
ON DUPLICATE KEY UPDATE col2=VALUES(col2);
SELECT * FROM indup_fk2;

DROP TABLE indup_fk2;
DROP TABLE indup_fk1;


-- =====================================================
-- 第二部分：主键相关测试（场景 7-9）
-- =====================================================

-- =====================================================
-- 测试场景 7: PRIMARY KEY 单列约束
-- 测试点：单列主键的冲突处理和各种更新方式
-- =====================================================
DROP TABLE IF EXISTS indup_02;
CREATE TABLE indup_02(
    col1 INT,
    col2 VARCHAR(20) NOT NULL,
    col3 VARCHAR(30) NOT NULL,
    col4 BIGINT DEFAULT 30,
    PRIMARY KEY (col1)
);

-- 初始化测试数据
INSERT INTO indup_02 VALUES (1,'apple','left',NULL),(2,'bear','right',1000);
SELECT * FROM indup_02;

-- 测试 7.1: 主键冲突，使用表达式更新非主键列
INSERT INTO indup_02 VALUES (1,'banana','lower',999) 
ON DUPLICATE KEY UPDATE col2=VALUES(col2), col3=VALUES(col3), col4=col4*2;
SELECT * FROM indup_02;

-- 测试 7.2: 批量插入，部分冲突，使用 VALUES() 和表达式混合
INSERT INTO indup_02(col1,col2,col3,col4) VALUES
    (2,'wechat','tower',500),
    (3,'paper','up',100)
ON DUPLICATE KEY UPDATE col3=VALUES(col3), col4=col4+VALUES(col4);
SELECT * FROM indup_02;

-- 测试 7.3: INSERT INTO SELECT 语法
DROP TABLE IF EXISTS indup_tmp;
CREATE TABLE indup_tmp(col1 INT, col2 VARCHAR(20), col3 VARCHAR(20));
INSERT INTO indup_tmp VALUES 
    (1,'new_apple','new_left'),
    (2,'new_bear','new_right'),
    (10,'wine','down');

INSERT INTO indup_02(col1,col2,col3) 
SELECT col1, col2, col3 FROM indup_tmp 
ON DUPLICATE KEY UPDATE col2='updated_from_select', col3=VALUES(col3);
SELECT * FROM indup_02;

-- 测试 7.4: 更新为常量、NULL、空字符串
INSERT INTO indup_02 VALUES(10,'test','test',999) 
ON DUPLICATE KEY UPDATE col2='constant_value', col4=12345;
SELECT * FROM indup_02;

INSERT INTO indup_02 VALUES(10,'test','test',999) 
ON DUPLICATE KEY UPDATE col3='', col4=NULL;
SELECT * FROM indup_02;

-- 测试 7.5: 使用函数更新
INSERT INTO indup_02 VALUES(3,'test','test',999) 
ON DUPLICATE KEY UPDATE col3=CONCAT(col3, '_suffix'), col2=UPPER(col2);
SELECT * FROM indup_02;

-- 测试 7.6: 无冲突，正常插入
DELETE FROM indup_02;
INSERT INTO indup_02(col1,col2,col3) VALUES
    (100,'app','upper'),
    (200,'light','lower')
ON DUPLICATE KEY UPDATE col2='should_not_update';
SELECT * FROM indup_02;


-- =====================================================
-- 测试场景 8: 复合主键（Composite Primary Key）
-- 测试点：多列组合主键的冲突检测和更新
-- =====================================================
DROP TABLE IF EXISTS indup_03;
CREATE TABLE indup_03(
    `id` INT,
    `act_name` VARCHAR(20) NOT NULL,
    `spu_id` VARCHAR(30) NOT NULL,
    `uv` BIGINT NOT NULL,
    `update_time` DATE DEFAULT '2020-10-10' COMMENT 'latest time',
    PRIMARY KEY (`id`, `act_name`)
);

-- 初始化测试数据
INSERT INTO indup_03 VALUES 
    (1,'beijing','001',1,'2021-01-03'),
    (2,'shanghai','002',2,'2022-09-23'),
    (3,'guangzhou','003',3,'2022-09-23');
SELECT * FROM indup_03;

-- 测试 8.1: 复合主键部分相同（不冲突），正常插入
INSERT INTO indup_03 VALUES (1,'shanghai','999',999,'2023-01-01');
SELECT * FROM indup_03;

-- 测试 8.2: 复合主键完全冲突，更新非主键列
INSERT INTO indup_03 VALUES (1,'beijing','newid',888,'2023-02-01') 
ON DUPLICATE KEY UPDATE `spu_id`=VALUES(`spu_id`), `uv`=VALUES(`uv`);
SELECT * FROM indup_03;

-- 测试 8.3: 批量插入，混合冲突
INSERT INTO indup_03 VALUES 
    (2,'shanghai','updated',777,'2023-03-01'),
    (4,'shenzhen','004',4,'2023-03-01')
ON DUPLICATE KEY UPDATE `spu_id`=VALUES(`spu_id`), `uv`=VALUES(`uv`);
SELECT * FROM indup_03;

-- 测试 8.4: 使用表达式更新
INSERT INTO indup_03 VALUES (4,'shenzhen','test',999,'2023-04-01') 
ON DUPLICATE KEY UPDATE `uv`=`uv`*2, `spu_id`=CONCAT(`spu_id`, '_v2');
SELECT * FROM indup_03;


-- =====================================================
-- 测试场景 9: 无主键和唯一索引的表
-- 测试点：没有唯一约束时，ON DUPLICATE KEY UPDATE 不生效
-- =====================================================
DROP TABLE IF EXISTS indup_05;
CREATE TABLE indup_05(
    col1 INT,
    col2 VARCHAR(20) NOT NULL,
    col3 VARCHAR(30) NOT NULL,
    col4 BIGINT DEFAULT 30
);

-- 测试 9.1: 无唯一约束，所有数据都应该被插入（包括重复数据）
INSERT INTO indup_05 VALUES
    (22,'11','33',1),
    (23,'22','55',2),
    (22,'11','33',1)
ON DUPLICATE KEY UPDATE col1=col1+100;
SELECT * FROM indup_05;


-- =====================================================
-- 第三部分：功能特性测试（场景 10-18）
-- =====================================================

-- =====================================================
-- 测试场景 10: 预处理语句（Prepared Statements）
-- 测试点：使用 PREPARE/EXECUTE 执行 ON DUPLICATE KEY UPDATE
-- =====================================================
DROP TABLE IF EXISTS indup_06;
CREATE TABLE indup_06(
    id INT PRIMARY KEY,
    name VARCHAR(20),
    value INT
);

INSERT INTO indup_06 VALUES (1,'first',100),(2,'second',200);
SELECT * FROM indup_06;

-- 测试 10.1: 预处理语句插入冲突数据
PREPARE stmt1 FROM "INSERT INTO indup_06 VALUES(?, ?, ?) ON DUPLICATE KEY UPDATE name=?, value=value*2";
SET @id = 1;
SET @name = 'updated_first';
SET @value = 999;
EXECUTE stmt1 USING @id, @name, @value, @name;
SELECT * FROM indup_06;

-- 测试 10.2: 预处理语句插入新数据
SET @id = 10;
SET @name = 'tenth';
SET @value = 1000;
EXECUTE stmt1 USING @id, @name, @value, @name;
SELECT * FROM indup_06;

DEALLOCATE PREPARE stmt1;


-- =====================================================
-- 测试场景 11: 特殊字符与转义
-- 测试点：包含特殊字符（引号、反斜杠等）的数据
-- =====================================================

-- 测试 11.1: 单列主键，包含转义字符
DROP TABLE IF EXISTS indup_07;
CREATE TABLE indup_07(
    id INT PRIMARY KEY,
    name VARCHAR(100)
);

INSERT INTO indup_07 VALUES (1, 'matrixone product');
INSERT INTO indup_07 VALUES (1, 'duplicate') 
ON DUPLICATE KEY UPDATE name=CONCAT(name, ' - updated');
SELECT * FROM indup_07;

-- 测试 11.2: 复合主键，包含转义字符
DROP TABLE IF EXISTS indup_08;
CREATE TABLE indup_08(
    a VARCHAR(100),
    b VARCHAR(100),
    value INT,
    PRIMARY KEY (a, b)
);

INSERT INTO indup_08 VALUES ('matrixone\'', 'mo-tester\'', 1);
INSERT INTO indup_08 VALUES ('matrixone\'', 'mo-tester\'', 2) 
ON DUPLICATE KEY UPDATE value=value+1;
SELECT * FROM indup_08;


-- =====================================================
-- 测试场景 12: 不同数据类型的列
-- 测试点：测试各种数据类型在 ON DUPLICATE KEY UPDATE 中的表现
-- =====================================================
DROP TABLE IF EXISTS indup_10;
CREATE TABLE indup_10(
    id INT PRIMARY KEY,
    col_tinyint TINYINT,
    col_smallint SMALLINT,
    col_bigint BIGINT,
    col_float FLOAT,
    col_double DOUBLE,
    col_decimal DECIMAL(10,2),
    col_date DATE,
    col_datetime DATETIME,
    col_char CHAR(10),
    col_varchar VARCHAR(50),
    col_text TEXT
);

-- 插入初始数据
INSERT INTO indup_10 VALUES (
    1, 10, 100, 1000, 1.5, 2.5, 100.50,
    '2023-01-01', '2023-01-01 10:00:00',
    'char_val', 'varchar_val', 'text_value'
);
SELECT * FROM indup_10;

-- 测试 12.1: 更新数值类型
INSERT INTO indup_10 VALUES (
    1, 20, 200, 2000, 3.5, 4.5, 200.50,
    '2023-02-01', '2023-02-01 10:00:00',
    'new_char', 'new_varchar', 'new_text'
) ON DUPLICATE KEY UPDATE
    col_tinyint=VALUES(col_tinyint),
    col_bigint=VALUES(col_bigint),
    col_decimal=VALUES(col_decimal);
SELECT * FROM indup_10;

-- 测试 12.2: 更新日期时间类型
INSERT INTO indup_10 VALUES (
    1, 99, 99, 99, 99.9, 99.9, 99.99,
    '2024-12-31', '2024-12-31 23:59:59',
    'x', 'x', 'x'
) ON DUPLICATE KEY UPDATE
    col_date=VALUES(col_date),
    col_datetime=VALUES(col_datetime);
SELECT * FROM indup_10;

-- 测试 12.3: 更新字符串类型
INSERT INTO indup_10 VALUES (
    1, 99, 99, 99, 99.9, 99.9, 99.99,
    '2025-01-01', '2025-01-01 00:00:00',
    'updated', 'updated_vc', 'updated_text'
) ON DUPLICATE KEY UPDATE
    col_char=VALUES(col_char),
    col_varchar=VALUES(col_varchar),
    col_text=VALUES(col_text);
SELECT * FROM indup_10;

-- 测试 12.4: 使用表达式更新数值类型
INSERT INTO indup_10 VALUES (
    1, 99, 99, 99, 99, 99, 99,
    '2025-01-01', '2025-01-01 00:00:00',
    'x', 'x', 'x'
) ON DUPLICATE KEY UPDATE
    col_tinyint=col_tinyint*2,
    col_bigint=col_bigint+1000,
    col_float=col_float+10.5;
SELECT * FROM indup_10;


-- =====================================================
-- 测试场景 13: 分区表
-- 测试点：验证分区表上的 ON DUPLICATE KEY UPDATE 功能
-- =====================================================

-- 测试 13.1: 单列主键分区表（KEY 分区）
DROP TABLE IF EXISTS indup_part_01;
CREATE TABLE indup_part_01(a INT PRIMARY KEY, b INT) PARTITION BY KEY(a) PARTITIONS 2;

INSERT INTO indup_part_01 VALUES (1,1),(2,2);
SELECT * FROM indup_part_01 ORDER BY a;

-- 部分冲突，部分插入
INSERT INTO indup_part_01 VALUES (1,1),(3,3) ON DUPLICATE KEY UPDATE b = 10;
SELECT * FROM indup_part_01 ORDER BY a;

DROP TABLE indup_part_01;

-- 测试 13.2: 复合主键分区表（KEY 分区）
CREATE TABLE indup_part_01(a INT, b INT, c INT, PRIMARY KEY(a,b)) 
    PARTITION BY KEY(a,b) PARTITIONS 2;

INSERT INTO indup_part_01 VALUES (1,1,1),(2,2,2);
SELECT * FROM indup_part_01 ORDER BY a, b;

-- 部分冲突，部分插入
INSERT INTO indup_part_01 VALUES (1,1,1),(3,3,3) ON DUPLICATE KEY UPDATE c = 10;
SELECT * FROM indup_part_01 ORDER BY a, b;

DROP TABLE indup_part_01;

-- 测试 13.3: 分区表上的表达式更新
CREATE TABLE indup_part_01(a INT PRIMARY KEY, b INT, c INT) 
    PARTITION BY KEY(a) PARTITIONS 3;

INSERT INTO indup_part_01 VALUES (1,10,100),(2,20,200),(3,30,300);
SELECT * FROM indup_part_01 ORDER BY a;

-- 批量更新，跨分区
INSERT INTO indup_part_01 VALUES (1,1,1),(2,2,2),(4,40,400) 
    ON DUPLICATE KEY UPDATE b=b+VALUES(b), c=c+VALUES(c);
SELECT * FROM indup_part_01 ORDER BY a;

DROP TABLE indup_part_01;


-- =====================================================
-- 测试场景 14: NULL 值处理
-- 测试点：验证 NULL 值在 ON DUPLICATE KEY UPDATE 中的行为
-- =====================================================

DROP TABLE IF EXISTS indup_null_01;
CREATE TABLE indup_null_01(
    id INT PRIMARY KEY,
    col1 VARCHAR(50),
    col2 INT,
    col3 VARCHAR(50) DEFAULT 'default_value'
);

-- 测试 14.1: 插入包含 NULL 的数据
INSERT INTO indup_null_01 VALUES (1, 'first', 100, 'initial');
SELECT * FROM indup_null_01;

-- 测试 14.2: 冲突时更新为 NULL
INSERT INTO indup_null_01 VALUES (1, 'updated', 200, 'test') 
    ON DUPLICATE KEY UPDATE col1 = NULL, col2 = VALUES(col2);
SELECT * FROM indup_null_01;

-- 测试 14.3: 使用 VALUES() 插入 NULL
INSERT INTO indup_null_01 VALUES (1, NULL, NULL, 'test2') 
    ON DUPLICATE KEY UPDATE col1 = VALUES(col1), col2 = VALUES(col2);
SELECT * FROM indup_null_01;

-- 测试 14.4: NULL 与非 NULL 列的组合
INSERT INTO indup_null_01 VALUES (2, NULL, 50, NULL);
INSERT INTO indup_null_01 VALUES (2, 'not_null', 100, 'not_null') 
    ON DUPLICATE KEY UPDATE 
        col1 = COALESCE(VALUES(col1), col1),
        col2 = VALUES(col2);
SELECT * FROM indup_null_01 ORDER BY id;

DROP TABLE indup_null_01;


-- =====================================================
-- 测试场景 15: DEFAULT 值处理
-- 测试点：验证 DEFAULT 值在冲突更新时的行为
-- =====================================================

DROP TABLE IF EXISTS indup_def_01;
CREATE TABLE indup_def_01(
    id INT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    status VARCHAR(20) DEFAULT 'active',
    counter INT DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 测试 15.1: 插入使用默认值的数据
INSERT INTO indup_def_01 (id, name) VALUES (1, 'user1');
SELECT id, name, status, counter FROM indup_def_01;

-- 测试 15.2: 冲突时更新为显式值
INSERT INTO indup_def_01 (id, name, status, counter) VALUES (1, 'user1_updated', 'inactive', 5) 
    ON DUPLICATE KEY UPDATE 
        status = VALUES(status), 
        counter = counter + VALUES(counter);
SELECT id, name, status, counter FROM indup_def_01;

-- 测试 15.3: 部分列使用默认值
INSERT INTO indup_def_01 (id, name) VALUES (2, 'user2'), (3, 'user3');
INSERT INTO indup_def_01 (id, name, counter) VALUES (2, 'user2', 10) 
    ON DUPLICATE KEY UPDATE counter = counter + VALUES(counter);
SELECT id, name, status, counter FROM indup_def_01 ORDER BY id;

DROP TABLE indup_def_01;


-- =====================================================
-- 测试场景 16: 事务中的行为
-- 测试点：验证 ON DUPLICATE KEY UPDATE 在事务中的正确性
-- =====================================================

DROP TABLE IF EXISTS indup_tx_01;
CREATE TABLE indup_tx_01(
    id INT PRIMARY KEY,
    value INT,
    description VARCHAR(100)
);

-- 测试 16.1: 事务中的冲突更新
START TRANSACTION;
INSERT INTO indup_tx_01 VALUES (1, 100, 'initial');
INSERT INTO indup_tx_01 VALUES (2, 200, 'second');
SELECT * FROM indup_tx_01 ORDER BY id;

-- 在同一事务中更新
INSERT INTO indup_tx_01 VALUES (1, 150, 'updated') 
    ON DUPLICATE KEY UPDATE value = VALUES(value), description = VALUES(description);
SELECT * FROM indup_tx_01 ORDER BY id;
COMMIT;

-- 验证事务提交后的结果
SELECT * FROM indup_tx_01 ORDER BY id;

-- 测试 16.2: 事务回滚
START TRANSACTION;
INSERT INTO indup_tx_01 VALUES (3, 300, 'third');
INSERT INTO indup_tx_01 VALUES (1, 999, 'should_rollback') 
    ON DUPLICATE KEY UPDATE value = VALUES(value);
SELECT * FROM indup_tx_01 ORDER BY id;
ROLLBACK;

-- 验证回滚后的结果
SELECT * FROM indup_tx_01 ORDER BY id;

-- 测试 16.3: 多表操作事务
START TRANSACTION;
INSERT INTO indup_tx_01 VALUES (4, 400, 'fourth');
INSERT INTO indup_tx_01 VALUES (2, 250, 'updated_second') 
    ON DUPLICATE KEY UPDATE value = value + 50;
UPDATE indup_tx_01 SET description = 'modified' WHERE id = 4;
COMMIT;

SELECT * FROM indup_tx_01 ORDER BY id;

DROP TABLE indup_tx_01;


-- =====================================================
-- 测试场景 17: 子查询作为数据源
-- 测试点：使用子查询作为插入数据源
-- =====================================================

DROP TABLE IF EXISTS indup_subq_01;
DROP TABLE IF EXISTS indup_subq_02;

CREATE TABLE indup_subq_01(
    id INT PRIMARY KEY,
    name VARCHAR(50),
    score INT
);

CREATE TABLE indup_subq_02(
    uid INT,
    uname VARCHAR(50),
    points INT
);

-- 准备数据
INSERT INTO indup_subq_01 VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 150);
INSERT INTO indup_subq_02 VALUES (1, 'Alice', 50), (4, 'David', 300), (2, 'Bob', 80);

-- 测试 17.1: 从子查询插入，处理冲突
INSERT INTO indup_subq_01 (id, name, score) 
SELECT uid, uname, points FROM indup_subq_02 
ON DUPLICATE KEY UPDATE score = score + VALUES(score);

SELECT * FROM indup_subq_01 ORDER BY id;

-- 测试 17.2: 带条件的子查询
DELETE FROM indup_subq_01 WHERE id > 3;
INSERT INTO indup_subq_01 (id, name, score) 
SELECT uid, uname, points * 2 FROM indup_subq_02 WHERE points > 50
ON DUPLICATE KEY UPDATE score = VALUES(score);

SELECT * FROM indup_subq_01 ORDER BY id;

DROP TABLE indup_subq_01;
DROP TABLE indup_subq_02;


-- =====================================================
-- 测试场景 18: 复杂表达式和函数
-- 测试点：在 UPDATE 子句中使用复杂表达式
-- =====================================================

DROP TABLE IF EXISTS indup_expr_01;
CREATE TABLE indup_expr_01(
    id INT PRIMARY KEY,
    val1 INT,
    val2 INT,
    val3 VARCHAR(50)
);

INSERT INTO indup_expr_01 VALUES (1, 10, 20, 'initial');
SELECT * FROM indup_expr_01;

-- 测试 18.1: 使用多列表达式
INSERT INTO indup_expr_01 VALUES (1, 5, 8, 'updated') 
    ON DUPLICATE KEY UPDATE 
        val1 = val1 + VALUES(val1),
        val2 = val2 * 2,
        val3 = CONCAT(val3, '-', VALUES(val3));
SELECT * FROM indup_expr_01;

-- 测试 18.2: 使用条件表达式（IF 模拟 GREATEST/LEAST）
INSERT INTO indup_expr_01 VALUES (1, 100, 200, 'test') 
    ON DUPLICATE KEY UPDATE 
        val1 = IF(val1 > VALUES(val1), val1, VALUES(val1)),  -- 模拟 GREATEST
        val2 = IF(val2 < VALUES(val2), val2, VALUES(val2)),  -- 模拟 LEAST
        val3 = IF(val1 > 50, 'high', 'low');
SELECT * FROM indup_expr_01;

DROP TABLE indup_expr_01;


-- =====================================================
-- 第四部分：边界和特殊场景测试（场景 19-24）
-- =====================================================

-- =====================================================
-- 测试场景 19: 字符集和特殊字符
-- 测试点：不同字符集的字符串处理
-- =====================================================

DROP TABLE IF EXISTS indup_char_01;
CREATE TABLE indup_char_01(
    id INT PRIMARY KEY,
    name VARCHAR(100),
    description TEXT
);

-- 测试 19.1: UTF-8 中文字符
INSERT INTO indup_char_01 VALUES (1, '中文测试', 'Chinese characters: 你好世界');
SELECT * FROM indup_char_01;

INSERT INTO indup_char_01 VALUES (1, '更新后的中文', 'Updated: 测试更新') 
    ON DUPLICATE KEY UPDATE name = VALUES(name), description = VALUES(description);
SELECT * FROM indup_char_01;

-- 测试 19.2: 混合字符
INSERT INTO indup_char_01 VALUES (2, 'Mix混合', 'English and 中文 mixed');
INSERT INTO indup_char_01 VALUES (2, 'Updated Mix', 'New mixed content') 
    ON DUPLICATE KEY UPDATE description = CONCAT(description, ' | ', VALUES(description));
SELECT * FROM indup_char_01 ORDER BY id;

-- 测试 19.3: 特殊字符
INSERT INTO indup_char_01 VALUES (3, 'Special !@#$%', 'Symbols: ~`!@#$%^&*()');
INSERT INTO indup_char_01 VALUES (3, 'Updated', 'New symbols') 
    ON DUPLICATE KEY UPDATE description = VALUES(description);
SELECT * FROM indup_char_01 ORDER BY id;

DROP TABLE indup_char_01;


-- =====================================================
-- 测试场景 20: 批量插入边界情况
-- 测试点：混合冲突和非冲突、边界值处理
-- =====================================================

DROP TABLE IF EXISTS indup_batch_01;
CREATE TABLE indup_batch_01(
    id INT PRIMARY KEY,
    val BIGINT,
    txt VARCHAR(255)
);

-- 测试 20.1: 批量插入混合冲突和非冲突
INSERT INTO indup_batch_01 VALUES (1, 100, 'first'), (2, 200, 'second'), (3, 300, 'third');

INSERT INTO indup_batch_01 VALUES 
    (1, 111, 'update1'),
    (4, 400, 'new1'),
    (2, 222, 'update2'),
    (5, 500, 'new2'),
    (3, 333, 'update3'),
    (6, 600, 'new3')
ON DUPLICATE KEY UPDATE val = VALUES(val), txt = VALUES(txt);

SELECT * FROM indup_batch_01 ORDER BY id;

-- 测试 20.2: 大数值处理
DELETE FROM indup_batch_01;
INSERT INTO indup_batch_01 VALUES (1, 9223372036854775807, 'max_bigint');
INSERT INTO indup_batch_01 VALUES (1, 1000, 'test') 
    ON DUPLICATE KEY UPDATE val = val - 1000;
SELECT * FROM indup_batch_01;

-- 测试 20.3: 空字符串和长字符串
INSERT INTO indup_batch_01 VALUES (2, 100, '');
INSERT INTO indup_batch_01 VALUES (2, 200, REPEAT('A', 255)) 
    ON DUPLICATE KEY UPDATE txt = VALUES(txt);
SELECT id, val, LENGTH(txt) AS txt_length FROM indup_batch_01 ORDER BY id;

DROP TABLE indup_batch_01;


-- =====================================================
-- 测试场景 21: 批量插入同一主键多次
-- 测试点：同一批次中多个值冲突同一行的行为
-- =====================================================

DROP TABLE IF EXISTS indup_dup_01;
CREATE TABLE indup_dup_01(a INT PRIMARY KEY, b INT);

-- 测试 21.1: 表中有数据，批量插入多个相同主键
INSERT INTO indup_dup_01 VALUES (1,1);
SELECT * FROM indup_dup_01;

-- 两个值都与表中 (1,1) 冲突
INSERT INTO indup_dup_01 VALUES (1,2), (1,22) ON DUPLICATE KEY UPDATE b=b+10;
SELECT * FROM indup_dup_01;

-- 测试 21.2: 批量插入中，多个新值冲突表中不同行
DELETE FROM indup_dup_01;
INSERT INTO indup_dup_01 VALUES (1,1),(3,3);

-- (1,2) 冲突表中 (1,1)，(3,33) 冲突表中 (3,3)
INSERT INTO indup_dup_01 VALUES (1,2),(2,22),(3,33) ON DUPLICATE KEY UPDATE b=b+100;
SELECT * FROM indup_dup_01 ORDER BY a;

-- 测试 21.3: 全部冲突的批量插入
INSERT INTO indup_dup_01 VALUES (1, 1), (2, 2), (3, 3) 
    ON DUPLICATE KEY UPDATE b = b * 10;
SELECT * FROM indup_dup_01 ORDER BY a;

-- 测试 21.4: 全部不冲突的批量插入
INSERT INTO indup_dup_01 VALUES (10, 10), (20, 20), (30, 30) 
    ON DUPLICATE KEY UPDATE b = 999;
SELECT * FROM indup_dup_01 ORDER BY a;

DROP TABLE indup_dup_01;


-- =====================================================
-- 测试场景 22: 实际业务场景模拟
-- 测试点：模拟真实业务中的使用模式
-- =====================================================

-- 场景 A：用户访问统计（计数器模式）
DROP TABLE IF EXISTS user_stats;
CREATE TABLE user_stats(
    user_id INT PRIMARY KEY,
    username VARCHAR(50),
    visit_count INT DEFAULT 0,
    last_visit DATETIME,
    total_time INT DEFAULT 0
);

-- 测试 22.1: 用户首次访问
INSERT INTO user_stats (user_id, username, visit_count, last_visit, total_time) 
VALUES (1, 'user1', 1, '2024-01-01 10:00:00', 300)
ON DUPLICATE KEY UPDATE 
    visit_count = visit_count + 1,
    last_visit = VALUES(last_visit),
    total_time = total_time + VALUES(total_time);
SELECT * FROM user_stats;

-- 测试 22.2: 用户再次访问（触发更新）
INSERT INTO user_stats (user_id, username, visit_count, last_visit, total_time) 
VALUES (1, 'user1', 1, '2024-01-02 15:30:00', 450)
ON DUPLICATE KEY UPDATE 
    visit_count = visit_count + 1,
    last_visit = VALUES(last_visit),
    total_time = total_time + VALUES(total_time);
SELECT * FROM user_stats;

-- 测试 22.3: 批量更新多个用户
INSERT INTO user_stats (user_id, username, visit_count, last_visit, total_time) 
VALUES 
    (1, 'user1', 1, '2024-01-03 09:00:00', 200),
    (2, 'user2', 1, '2024-01-03 10:00:00', 350),
    (3, 'user3', 1, '2024-01-03 11:00:00', 400)
ON DUPLICATE KEY UPDATE 
    visit_count = visit_count + 1,
    last_visit = VALUES(last_visit),
    total_time = total_time + VALUES(total_time);
SELECT * FROM user_stats ORDER BY user_id;

DROP TABLE user_stats;

-- 场景 B：库存管理（累加模式）
DROP TABLE IF EXISTS inventory;
CREATE TABLE inventory(
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    quantity INT DEFAULT 0,
    reserved INT DEFAULT 0,
    last_updated DATETIME
);

-- 测试 22.4: 初始库存
INSERT INTO inventory (product_id, product_name, quantity, last_updated) 
VALUES (101, 'Product A', 100, '2024-01-01 08:00:00');
SELECT * FROM inventory;

-- 测试 22.5: 入库操作
INSERT INTO inventory (product_id, product_name, quantity, last_updated) 
VALUES (101, 'Product A', 50, '2024-01-02 10:00:00')
ON DUPLICATE KEY UPDATE 
    quantity = quantity + VALUES(quantity),
    last_updated = VALUES(last_updated);
SELECT * FROM inventory;

-- 测试 22.6: 预留库存
INSERT INTO inventory (product_id, product_name, quantity, reserved, last_updated) 
VALUES (101, 'Product A', 0, 20, '2024-01-03 14:00:00')
ON DUPLICATE KEY UPDATE 
    reserved = reserved + VALUES(reserved),
    last_updated = VALUES(last_updated);
SELECT * FROM inventory;

-- 测试 22.7: 批量操作多个产品
INSERT INTO inventory (product_id, product_name, quantity, last_updated) 
VALUES 
    (101, 'Product A', 30, '2024-01-04 09:00:00'),
    (102, 'Product B', 80, '2024-01-04 09:00:00'),
    (103, 'Product C', 120, '2024-01-04 09:00:00')
ON DUPLICATE KEY UPDATE 
    quantity = quantity + VALUES(quantity),
    last_updated = VALUES(last_updated);
SELECT * FROM inventory ORDER BY product_id;

DROP TABLE inventory;

-- 场景 C：AUTO_INCREMENT + 时间戳自动更新
DROP TABLE IF EXISTS users;
CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT, 
    counter INT, 
    create_at DATETIME DEFAULT CURRENT_TIMESTAMP, 
    update_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- 测试 22.8: AUTO_INCREMENT + DEFAULT CURRENT_TIMESTAMP + ON UPDATE
INSERT INTO users (id, counter) VALUES (112, 1);
SELECT id, counter, create_at = update_at AS timestamps_equal FROM users;

-- 等待 1 秒后更新
SELECT SLEEP(1);

-- 主键冲突，更新 counter 和 create_at
INSERT INTO users (id, counter) VALUES (112, 2) 
    ON DUPLICATE KEY UPDATE 
        counter = counter + VALUES(counter), 
        create_at = CURRENT_TIMESTAMP();

-- 验证：create_at 被更新，update_at 也自动更新
SELECT id, counter, create_at = update_at AS timestamps_equal FROM users;

DROP TABLE users;

-- 场景 D：自增列在冲突时不自增
DROP TABLE IF EXISTS auto_test;
CREATE TABLE auto_test (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(50)
);

INSERT INTO auto_test (id, name) VALUES (1, 'first'), (2, 'second');
SELECT * FROM auto_test ORDER BY id;

-- 主键冲突，不会消耗自增 ID
INSERT INTO auto_test (id, name) VALUES (1, 'updated_first') 
    ON DUPLICATE KEY UPDATE name = VALUES(name);
SELECT * FROM auto_test ORDER BY id;

-- 插入新数据，验证自增 ID 连续性
INSERT INTO auto_test (name) VALUES ('third');
SELECT * FROM auto_test ORDER BY id;

DROP TABLE auto_test;


-- =====================================================
-- 测试场景 23: 大数据量性能测试
-- 测试点：使用 generate_series 生成大量数据
-- =====================================================
DROP TABLE IF EXISTS indup_11;
CREATE TABLE indup_11(
    id INT PRIMARY KEY,
    name VARCHAR(100),
    value INT
);

-- 插入 10 万行数据
INSERT INTO indup_11 (id, name, value) 
SELECT result, CONCAT('name_', result), result 
FROM generate_series(1, 100000) g;

-- 测试冲突更新性能
INSERT INTO indup_11 (id, name, value) 
SELECT result, CONCAT('updated_', result), result * 2
FROM generate_series(1, 1000) g
ON DUPLICATE KEY UPDATE value = VALUES(value);

SELECT COUNT(*) FROM indup_11;
SELECT * FROM indup_11 WHERE id <= 10;


-- =====================================================
-- 测试场景 24: TINYINT 数值溢出测试
-- 测试点：验证数值类型溢出时的行为
-- =====================================================
DROP TABLE IF EXISTS indup_overflow;
CREATE TABLE indup_overflow(
    id INT PRIMARY KEY,
    tiny TINYINT,
    small SMALLINT,
    big BIGINT,
    flt FLOAT,
    dbl DOUBLE
);

-- 插入初始数据
INSERT INTO indup_overflow VALUES (1, 100, 30000, 9223372036854775807, 3.14, 2.718281828);
SELECT * FROM indup_overflow;

-- 测试 24.1: TINYINT 溢出（100 + 50 = 150 溢出为 -106）
INSERT INTO indup_overflow VALUES (1, 50, 1000, 500, 2.0, 3.0)
ON DUPLICATE KEY UPDATE 
    tiny = tiny + VALUES(tiny),  -- 100 + 50 = 150，溢出为 -106
    small = small - VALUES(small),
    flt = flt * VALUES(flt),
    dbl = dbl / VALUES(dbl);
SELECT * FROM indup_overflow;

DROP TABLE indup_overflow;


-- =====================================================
-- 第五部分：错误场景测试（场景 25-30）
-- 所有预期报错的测试集中在此部分
-- =====================================================

-- =====================================================
-- 测试场景 25: 错误场景 - 唯一键冲突但主键不冲突
-- 测试点：验证 MO 只支持主键冲突场景，不支持纯唯一键冲突
-- =====================================================
DROP TABLE IF EXISTS indup_err_uk_01;
CREATE TABLE indup_err_uk_01(
    id INT PRIMARY KEY,
    email VARCHAR(50),
    name VARCHAR(20),
    UNIQUE KEY uk_email_25 (email)
);

-- 准备索引行数检查
SET @idxsql = CONCAT("SELECT COUNT(*) FROM `", (SELECT DISTINCT index_table_name FROM mo_catalog.mo_indexes WHERE name = 'uk_email_25'), "`");
PREPARE check_idx FROM @idxsql;

INSERT INTO indup_err_uk_01 VALUES (1, 'user@example.com', 'user1');
EXECUTE check_idx;

-- 测试 25.1: 唯一键冲突但主键不冲突（预期报错）
-- @regex("Duplicate entry 'user@example.com'", true)
INSERT INTO indup_err_uk_01 VALUES (2, 'user@example.com', 'user2') 
ON DUPLICATE KEY UPDATE name = VALUES(name);
EXECUTE check_idx;

-- 测试 25.2: 复合唯一键冲突但主键不冲突
DROP TABLE IF EXISTS indup_err_uk_02;
CREATE TABLE indup_err_uk_02(
    id INT PRIMARY KEY,
    col1 VARCHAR(20),
    col2 VARCHAR(20),
    value INT,
    UNIQUE KEY uk_cols_25 (col1, col2)
);

-- 准备索引行数检查
SET @idxsql = CONCAT("SELECT COUNT(*) FROM `", (SELECT DISTINCT index_table_name FROM mo_catalog.mo_indexes WHERE name = 'uk_cols_25'), "`");
PREPARE check_idx FROM @idxsql;

INSERT INTO indup_err_uk_02 VALUES (1, 'a', 'b', 100);
EXECUTE check_idx;

-- 预期报错
-- @regex("Duplicate entry '\(a,b\)'", true)
INSERT INTO indup_err_uk_02 VALUES (2, 'a', 'b', 200) 
ON DUPLICATE KEY UPDATE value = VALUES(value);
EXECUTE check_idx;


-- =====================================================
-- 测试场景 26: 错误场景 - 尝试更新主键列
-- 测试点：验证不允许更新主键列的限制
-- =====================================================
DROP TABLE IF EXISTS indup_err_01;
CREATE TABLE indup_err_01(
    id INT PRIMARY KEY,
    name VARCHAR(20),
    value INT
);

INSERT INTO indup_err_01 VALUES (1, 'first', 100);

-- 测试 26.1: 尝试更新单列主键（应该报错）
INSERT INTO indup_err_01 VALUES (1, 'updated', 200) 
ON DUPLICATE KEY UPDATE id = id + 10, name = VALUES(name);

-- 测试 26.2: 尝试使用 VALUES() 更新主键（应该报错）
INSERT INTO indup_err_01 VALUES (1, 'updated', 300) 
ON DUPLICATE KEY UPDATE id = VALUES(id);


-- =====================================================
-- 测试场景 27: 错误场景 - 尝试更新唯一键列
-- 测试点：验证不允许更新唯一键列的限制
-- =====================================================
DROP TABLE IF EXISTS indup_err_02;
CREATE TABLE indup_err_02(
    id INT,
    email VARCHAR(50),
    name VARCHAR(20),
    UNIQUE KEY uk_email_27 (email)
);

-- 准备索引行数检查
SET @idxsql = CONCAT("SELECT COUNT(*) FROM `", (SELECT DISTINCT index_table_name FROM mo_catalog.mo_indexes WHERE name = 'uk_email_27'), "`");
PREPARE check_idx FROM @idxsql;

INSERT INTO indup_err_02 VALUES (1, 'user@example.com', 'user1');
EXECUTE check_idx;

-- 测试 27.1: 尝试更新唯一键列（应该报错）
INSERT INTO indup_err_02 VALUES (2, 'user@example.com', 'user2') 
ON DUPLICATE KEY UPDATE email = 'newemail@example.com', name = VALUES(name);
EXECUTE check_idx;

-- 测试 27.2: 尝试使用 VALUES() 更新唯一键列（应该报错）
INSERT INTO indup_err_02 VALUES (3, 'user@example.com', 'user3') 
ON DUPLICATE KEY UPDATE email = VALUES(email);
EXECUTE check_idx;


-- =====================================================
-- 测试场景 28: 错误场景 - 尝试更新复合主键的任一列
-- 测试点：验证复合主键的任何列都不能更新
-- =====================================================
DROP TABLE IF EXISTS indup_err_03;
CREATE TABLE indup_err_03(
    col1 INT,
    col2 INT,
    col3 VARCHAR(20),
    PRIMARY KEY (col1, col2)
);

INSERT INTO indup_err_03 VALUES (1, 2, 'test');

-- 测试 28.1: 尝试更新复合主键的第一列（应该报错）
INSERT INTO indup_err_03 VALUES (1, 2, 'updated') 
ON DUPLICATE KEY UPDATE col1 = col1 + 1, col3 = VALUES(col3);

-- 测试 28.2: 尝试更新复合主键的第二列（应该报错）
INSERT INTO indup_err_03 VALUES (1, 2, 'updated') 
ON DUPLICATE KEY UPDATE col2 = col2 + 1, col3 = VALUES(col3);

-- 测试 28.3: 尝试同时更新复合主键的两列（应该报错）
INSERT INTO indup_err_03 VALUES (1, 2, 'updated') 
ON DUPLICATE KEY UPDATE col1 = VALUES(col1), col2 = VALUES(col2);


-- =====================================================
-- 测试场景 29: 错误场景 - 尝试更新复合唯一键的任一列
-- 测试点：验证复合唯一键的任何列都不能更新
-- =====================================================
DROP TABLE IF EXISTS indup_err_04;
CREATE TABLE indup_err_04(
    id INT,
    col1 VARCHAR(20),
    col2 VARCHAR(20),
    value INT,
    UNIQUE KEY uk_cols_29 (col1, col2)
);

-- 准备索引行数检查
SET @idxsql = CONCAT("SELECT COUNT(*) FROM `", (SELECT DISTINCT index_table_name FROM mo_catalog.mo_indexes WHERE name = 'uk_cols_29'), "`");
PREPARE check_idx FROM @idxsql;

INSERT INTO indup_err_04 VALUES (1, 'a', 'b', 100);
EXECUTE check_idx;

-- 测试 29.1: 尝试更新复合唯一键的第一列（应该报错）
INSERT INTO indup_err_04 VALUES (2, 'a', 'b', 200) 
ON DUPLICATE KEY UPDATE col1 = 'updated', value = VALUES(value);
EXECUTE check_idx;

-- 测试 29.2: 尝试更新复合唯一键的第二列（应该报错）
INSERT INTO indup_err_04 VALUES (3, 'a', 'b', 300) 
ON DUPLICATE KEY UPDATE col2 = 'updated', value = VALUES(value);
EXECUTE check_idx;

-- 测试 29.3: 尝试同时更新复合唯一键的两列（应该报错）
INSERT INTO indup_err_04 VALUES (4, 'a', 'b', 400) 
ON DUPLICATE KEY UPDATE col1 = VALUES(col1), col2 = VALUES(col2);
EXECUTE check_idx;


-- =====================================================
-- 测试场景 30: 错误场景 - 主键+唯一键，尝试更新任一键列
-- 测试点：验证有多个约束时，所有键列都不能更新
-- =====================================================
DROP TABLE IF EXISTS indup_err_05;
CREATE TABLE indup_err_05(
    id INT PRIMARY KEY,
    email VARCHAR(50),
    name VARCHAR(20),
    value INT,
    UNIQUE KEY uk_email_30 (email)
);

-- 准备索引行数检查
SET @idxsql = CONCAT("SELECT COUNT(*) FROM `", (SELECT DISTINCT index_table_name FROM mo_catalog.mo_indexes WHERE name = 'uk_email_30'), "`");
PREPARE check_idx FROM @idxsql;

INSERT INTO indup_err_05 VALUES (1, 'user@example.com', 'user1', 100);
EXECUTE check_idx;

-- 测试 30.1: 主键冲突时尝试更新主键（应该报错）
INSERT INTO indup_err_05 VALUES (1, 'other@example.com', 'user2', 200) 
ON DUPLICATE KEY UPDATE id = id + 10;
EXECUTE check_idx;

-- 测试 30.2: 唯一键冲突但主键不冲突时尝试更新唯一键（应该报错）
INSERT INTO indup_err_05 VALUES (2, 'user@example.com', 'user3', 300) 
ON DUPLICATE KEY UPDATE email = 'new@example.com';
EXECUTE check_idx;

-- 测试 30.3: 冲突时尝试同时更新主键和唯一键（应该报错）
INSERT INTO indup_err_05 VALUES (1, 'test@example.com', 'user4', 400) 
ON DUPLICATE KEY UPDATE id = id + 10, email = VALUES(email);
EXECUTE check_idx;


-- =====================================================
-- 清理测试数据
-- =====================================================
DROP TABLE IF EXISTS indup_00;
DROP TABLE IF EXISTS indup_01;
DROP TABLE IF EXISTS indup_02;
DROP TABLE IF EXISTS indup_tmp;
DROP TABLE IF EXISTS indup_03;
DROP TABLE IF EXISTS indup_04;
DROP TABLE IF EXISTS indup_05;
DROP TABLE IF EXISTS indup_06;
DROP TABLE IF EXISTS indup_07;
DROP TABLE IF EXISTS indup_08;
DROP TABLE IF EXISTS indup_09;
DROP TABLE IF EXISTS indup_10;
DROP TABLE IF EXISTS indup_11;
DROP TABLE IF EXISTS indup_idx_01;
DROP TABLE IF EXISTS indup_part_01;
DROP TABLE IF EXISTS indup_null_01;
DROP TABLE IF EXISTS indup_def_01;
DROP TABLE IF EXISTS indup_tx_01;
DROP TABLE IF EXISTS indup_subq_01;
DROP TABLE IF EXISTS indup_subq_02;
DROP TABLE IF EXISTS indup_expr_01;
DROP TABLE IF EXISTS indup_char_01;
DROP TABLE IF EXISTS indup_batch_01;
DROP TABLE IF EXISTS indup_dup_01;
DROP TABLE IF EXISTS indup_overflow;
DROP TABLE IF EXISTS user_stats;
DROP TABLE IF EXISTS inventory;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS auto_test;
DROP TABLE IF EXISTS indup_err_uk_01;
DROP TABLE IF EXISTS indup_err_uk_02;
DROP TABLE IF EXISTS indup_err_01;
DROP TABLE IF EXISTS indup_err_02;
DROP TABLE IF EXISTS indup_err_03;
DROP TABLE IF EXISTS indup_err_04;
DROP TABLE IF EXISTS indup_err_05;
