-- ============================================================================
-- matrixone 临时表测试用例 - 索引功能
-- 测试文件: test_cases_index.sql
-- ============================================================================

-- ============================================================================
-- 测试分类 1: 普通索引
-- ============================================================================

-- 测试用例 1.1: 创建表时定义索引
-- 预期结果: 创建成功
drop database if exists temp_index;
create database temp_index;
use temp_index;
create temporary table temp_idx1 (
    id int,
    name varchar(50),
    age int,
    index idx_name (name)
);

insert into temp_idx1 values (1, 'alice', 25);
insert into temp_idx1 values (2, 'bob', 30);
insert into temp_idx1 values (3, 'charlie', 25);

-- 测试用例 1.2: 使用索引查询
-- 预期结果: 查询成功
select * from temp_idx1 where name = 'bob';

-- 测试用例 1.3: show index 显示索引信息
-- 预期结果: 显示索引信息
show index from temp_idx1;

drop table temp_idx1;

-- ============================================================================
-- 测试分类 2: create index 语句
-- ============================================================================

-- 测试用例 2.1: 使用 create index 添加索引
-- 预期结果: 索引创建成功
create temporary table temp_idx2 (
    id int,
    name varchar(50),
    age int
);

create index idx_name on temp_idx2(name);
create index idx_age on temp_idx2(age);

insert into temp_idx2 values (1, 'alice', 25);
insert into temp_idx2 values (2, 'bob', 30);
insert into temp_idx2 values (3, 'charlie', 25);

-- 测试用例 2.2: 验证索引可用
select * from temp_idx2 where name = 'alice';
select * from temp_idx2 where age = 25;

show index from temp_idx2;

drop table temp_idx2;

-- ============================================================================
-- 测试分类 3: 唯一索引
-- ============================================================================

-- 测试用例 3.1: 创建唯一索引
-- 预期结果: 创建成功
create temporary table temp_unique (
    id int,
    email varchar(100)
);

create unique index idx_email on temp_unique(email);

-- 测试用例 3.2: 插入唯一值
-- 预期结果: 插入成功
insert into temp_unique values (1, 'alice@example.com');
insert into temp_unique values (2, 'bob@example.com');

-- 测试用例 3.3: 尝试插入重复值
-- 预期结果: 失败，违反唯一约束
-- @regex("Duplicate entry",true)
insert into temp_unique values (3, 'alice@example.com');

select * from temp_unique;
drop table temp_unique;

-- 测试用例 3.4: 表定义时创建唯一索引
-- 预期结果: 创建成功
create temporary table temp_unique2 (
    id int,
    username varchar(50),
    unique index idx_username (username)
);

insert into temp_unique2 values (1, 'user1');
insert into temp_unique2 values (2, 'user2');
-- @regex("Duplicate entry",true)
insert into temp_unique2 values (3, 'user1');  -- 应该失败

select * from temp_unique2;
drop table temp_unique2;

-- ============================================================================
-- 测试分类 4: 复合索引
-- ============================================================================

-- 测试用例 4.1: 创建复合索引
-- 预期结果: 创建成功
create temporary table temp_composite (
    id int,
    first_name varchar(50),
    last_name varchar(50),
    age int
);

create index idx_name on temp_composite(first_name, last_name);
create index idx_name_age on temp_composite(first_name, last_name, age);

insert into temp_composite values (1, 'john', 'doe', 30);
insert into temp_composite values (2, 'jane', 'smith', 25);
insert into temp_composite values (3, 'john', 'smith', 35);

-- 测试用例 4.2: 使用复合索引查询
select * from temp_composite where first_name = 'john';
select * from temp_composite where first_name = 'john' and last_name = 'doe';
select * from temp_composite where first_name = 'john' and last_name = 'smith' and age = 35;

show index from temp_composite;
drop table temp_composite;

-- ============================================================================
-- 测试分类 5: alter table add index
-- ============================================================================

-- 测试用例 5.1: alter table 添加索引
-- 预期结果: 添加成功
create temporary table temp_alter_idx (
    id int,
    name varchar(50),
    email varchar(100),
    age int
);

insert into temp_alter_idx values (1, 'alice', 'alice@example.com', 25);
insert into temp_alter_idx values (2, 'bob', 'bob@example.com', 30);

-- 添加普通索引
alter table temp_alter_idx add index idx_name (name);

-- 添加唯一索引
alter table temp_alter_idx add unique index idx_email (email);

-- 添加复合索引
alter table temp_alter_idx add index idx_name_age (name, age);

show index from temp_alter_idx;

-- 测试用例 5.2: 验证索引可用
select * from temp_alter_idx where name = 'alice';
select * from temp_alter_idx where email = 'bob@example.com';

drop table temp_alter_idx;

-- ============================================================================
-- 测试分类 6: alter table drop index
-- ============================================================================

-- 测试用例 6.1: alter table 删除索引
-- 预期结果: 删除成功
create temporary table temp_drop_idx (
    id int,
    name varchar(50),
    age int,
    index idx_name (name),
    index idx_age (age)
);

insert into temp_drop_idx values (1, 'alice', 25);
insert into temp_drop_idx values (2, 'bob', 30);

-- 显示初始索引
show index from temp_drop_idx;

-- 删除索引
alter table temp_drop_idx drop index idx_name;

-- 验证索引已删除
show index from temp_drop_idx;

-- 删除另一个索引
alter table temp_drop_idx drop index idx_age;

show index from temp_drop_idx;

drop table temp_drop_idx;

-- ============================================================================
-- 测试分类 7: drop index 语句
-- ============================================================================

-- 测试用例 7.1: 使用 drop index 删除索引
-- 预期结果: 删除成功
create temporary table temp_drop_idx2 (
    id int,
    name varchar(50),
    email varchar(100)
);

create index idx_name on temp_drop_idx2(name);
create index idx_email on temp_drop_idx2(email);

show index from temp_drop_idx2;

-- 使用 drop index 删除
drop index idx_name on temp_drop_idx2;

show index from temp_drop_idx2;

drop index idx_email on temp_drop_idx2;

show index from temp_drop_idx2;

drop table temp_drop_idx2;

-- ============================================================================
-- 测试分类 8: 全文索引
-- ============================================================================

-- 测试用例 8.1: 创建全文索引
-- 预期结果: 创建成功
-- @bvt:issue#23701
create temporary table temp_fulltext (
    id int primary key,
    title varchar(100),
    content text
);

create fulltext index idx_ft_title on temp_fulltext(title);
create fulltext index idx_ft_content on temp_fulltext(content);

insert into temp_fulltext values (1, 'introduction to matrixone', 'matrixone is a hyperconverged database');
insert into temp_fulltext values (2, 'temporary tables guide', 'learn how to use temporary tables');
insert into temp_fulltext values (3, 'index management', 'creating and managing indexes in matrixone');

show index from temp_fulltext;

drop table temp_fulltext;
-- @bvt:issue


-- 测试用例 8.2: 多列全文索引
-- 预期结果: 创建成功
-- @bvt:issue#23701
create temporary table temp_fulltext2 (
    id int primary key,
    title varchar(100),
    body text
);

create fulltext index idx_ft_multi on temp_fulltext2(title, body);

insert into temp_fulltext2 values (1, 'database tutorial', 'learn about databases and sql');
insert into temp_fulltext2 values (2, 'matrixone features', 'explore the features of matrixone');

show index from temp_fulltext2;

drop table temp_fulltext2;
-- @bvt:issue


-- ============================================================================
-- 测试分类 9: 向量索引 (ivf)
-- ============================================================================

-- 测试用例 9.1: 创建向量索引
-- 预期结果: 创建成功（需要开启 experimental_ivf_index）
-- @bvt:issue#23701
set experimental_ivf_index = 1;

create temporary table temp_vector (
    id int primary key,
    embedding vecf32(3)
);

create index idx_ivf using ivfflat on temp_vector(embedding) lists=1 op_type "vector_l2_ops";

insert into temp_vector values (1, '[1.0, 2.0, 3.0]');
insert into temp_vector values (2, '[4.0, 5.0, 6.0]');
insert into temp_vector values (3, '[7.0, 8.0, 9.0]');

show index from temp_vector;

drop table temp_vector;

-- 测试用例 9.2: 不同维度的向量索引
-- 预期结果: 创建成功
create temporary table temp_vector2 (
    id int primary key,
    vec128 vecf32(128)
);

create index idx_vec128 using ivfflat on temp_vector2(vec128) lists=2 op_type "vector_l2_ops";

show index from temp_vector2;

drop table temp_vector2;

set experimental_ivf_index = 0;

-- ============================================================================
-- 测试分类 10: 多索引组合
-- ============================================================================

-- 测试用例 10.1: 在同一表上创建多种类型的索引
-- 预期结果: 所有索引创建成功
create temporary table temp_multi_idx (
    id int primary key,
    username varchar(50),
    email varchar(100),
    age int,
    bio text,
    created_at timestamp
);

-- 普通索引
create index idx_username on temp_multi_idx(username);

-- 唯一索引
create unique index idx_email on temp_multi_idx(email);

-- 复合索引
create index idx_age_created on temp_multi_idx(age, created_at);

-- 全文索引
create fulltext index idx_bio on temp_multi_idx(bio);

-- 显示所有索引
show index from temp_multi_idx;

-- 插入测试数据
insert into temp_multi_idx values
    (1, 'alice', 'alice@example.com', 25, 'software engineer', '2024-01-01 10:00:00'),
    (2, 'bob', 'bob@example.com', 30, 'data scientist', '2024-01-02 11:00:00'),
    (3, 'charlie', 'charlie@example.com', 25, 'product manager', '2024-01-03 12:00:00');

-- 测试各种索引查询
select * from temp_multi_idx where username = 'alice';
select * from temp_multi_idx where email = 'bob@example.com';
select * from temp_multi_idx where age = 25;

drop table temp_multi_idx;
-- @bvt:issue

-- ============================================================================
-- 测试分类 11: 索引边界测试
-- ============================================================================

-- 测试用例 11.1: 空表上创建索引
-- 预期结果: 创建成功
create temporary table temp_empty_idx (
    id int,
    name varchar(50)
);

create index idx_name on temp_empty_idx(name);

show index from temp_empty_idx;

-- 后续插入数据
insert into temp_empty_idx values (1, 'test');
select * from temp_empty_idx where name = 'test';

drop table temp_empty_idx;

-- 测试用例 11.2: 在有数据的表上创建索引
-- 预期结果: 创建成功
create temporary table temp_data_idx (
    id int,
    value varchar(50)
);

insert into temp_data_idx values (1, 'value1');
insert into temp_data_idx values (2, 'value2');
insert into temp_data_idx values (3, 'value3');

-- 在已有数据的表上创建索引
create index idx_value on temp_data_idx(value);

show index from temp_data_idx;

select * from temp_data_idx where value = 'value2';

drop table temp_data_idx;

-- 测试用例 11.3: 删除不存在的索引
-- 预期结果: 报错
create temporary table temp_no_idx (
    id int,
    name varchar(50)
);

drop index idx_nonexistent on temp_no_idx;  -- 应该失败

drop table temp_no_idx;

-- ============================================================================
-- 测试分类 1: 全文索引 - 基础场景
-- ============================================================================

-- 测试用例 1.1: 单列全文索引 + 主键
-- 预期结果: 创建成功
-- @bvt:issue#23701
use temp_index;
CREATE TEMPORARY TABLE temp_ft_basic (
    id INT PRIMARY KEY AUTO_INCREMENT,
    title VARCHAR(200),
    content TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE FULLTEXT INDEX idx_ft_title ON temp_ft_basic(title);

INSERT INTO temp_ft_basic (title, content) VALUES
    ('Introduction to MatrixOne Database', 'MatrixOne is a hyperconverged cloud and edge native database'),
    ('Temporary Tables in MatrixOne', 'Learn how to create and manage temporary tables effectively'),
    ('Full-Text Search Tutorial', 'Understanding full-text search capabilities in modern databases'),
    ('Vector Database Features', 'Exploring vector search and similarity matching'),
    ('MatrixOne Performance Optimization', 'Tips and tricks for optimizing query performance');

-- 验证索引
SHOW INDEX FROM temp_ft_basic;

-- 全文搜索查询
SELECT id, title FROM temp_ft_basic
WHERE MATCH(title) AGAINST('MatrixOne' IN NATURAL LANGUAGE MODE);

DROP TABLE temp_ft_basic;

-- 测试用例 1.2: 多列全文索引 + 主键 + 唯一索引
-- 预期结果: 创建成功，支持多列全文搜索
CREATE TEMPORARY TABLE temp_ft_multi_column (
    doc_id INT PRIMARY KEY AUTO_INCREMENT,
    document_code VARCHAR(50) UNIQUE,
    title VARCHAR(200) NOT NULL,
    abstract TEXT,
    body TEXT,
    author VARCHAR(100),
    category VARCHAR(50),
    publish_date DATE,
    INDEX idx_category (category),
    INDEX idx_author (author)
);

-- 创建多列全文索引
CREATE FULLTEXT INDEX idx_ft_title_abstract ON temp_ft_multi_column(title, abstract);
CREATE FULLTEXT INDEX idx_ft_body ON temp_ft_multi_column(body);

INSERT INTO temp_ft_multi_column (document_code, title, abstract, body, author, category, publish_date) VALUES
    ('DOC001', 'Advanced Database Systems',
     'This paper discusses advanced concepts in modern database systems including HTAP capabilities',
     'Database systems have evolved significantly. HTAP (Hybrid Transactional/Analytical Processing) represents a major advancement. MatrixOne implements HTAP architecture efficiently.',
     'John Smith', 'Database', '2024-01-15'),
    ('DOC002', 'Cloud Native Architecture Patterns',
     'Exploring cloud native design patterns for scalable applications',
     'Cloud native applications require specific architectural patterns. Microservices, containers, and orchestration are key components. MatrixOne supports cloud native deployments.',
     'Jane Doe', 'Cloud', '2024-01-20'),
    ('DOC003', 'Machine Learning in Databases',
     'Integration of machine learning capabilities within database systems',
     'Modern databases are incorporating ML features. Vector search enables similarity matching. MatrixOne provides vector index support for ML workloads.',
     'Bob Johnson', 'AI/ML', '2024-02-01');

SHOW INDEX FROM temp_ft_multi_column;

-- 多列全文搜索
SELECT doc_id, title, author FROM temp_ft_multi_column
WHERE MATCH(title, abstract) AGAINST('database systems' IN NATURAL LANGUAGE MODE);

-- 单列全文搜索
SELECT doc_id, title FROM temp_ft_multi_column
WHERE MATCH(body) AGAINST('MatrixOne' IN NATURAL LANGUAGE MODE);

DROP TABLE temp_ft_multi_column;

-- ============================================================================
-- 测试分类 2: 全文索引 - 复杂场景
-- ============================================================================

-- 测试用例 2.1: 全文索引 + 复合主键 + 多个普通索引
-- 预期结果: 创建成功
CREATE TEMPORARY TABLE temp_ft_composite_pk (
    article_id INT,
    language_code CHAR(2),
    title VARCHAR(300),
    subtitle VARCHAR(300),
    content TEXT,
    tags TEXT,
    author_id INT,
    category_id INT,
    view_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (article_id, language_code),
    INDEX idx_author (author_id),
    INDEX idx_category (category_id),
    INDEX idx_views (view_count),
    INDEX idx_created (created_at),
    INDEX idx_author_category (author_id, category_id)
);

-- 创建全文索引
CREATE FULLTEXT INDEX idx_ft_title_subtitle ON temp_ft_composite_pk(title, subtitle);
CREATE FULLTEXT INDEX idx_ft_content ON temp_ft_composite_pk(content);
CREATE FULLTEXT INDEX idx_ft_tags ON temp_ft_composite_pk(tags);

INSERT INTO temp_ft_composite_pk (article_id, language_code, title, subtitle, content, tags, author_id, category_id, view_count) VALUES
    (1, 'en', 'Getting Started with MatrixOne', 'A comprehensive guide for beginners',
     'MatrixOne is a powerful hyperconverged database. This guide covers installation, configuration, and basic operations.',
     'database, tutorial, beginner, matrixone, guide', 101, 1, 1500),
    (1, 'zh', 'MatrixOne 入门指南', '面向初学者的综合指南',
     'MatrixOne 是一个强大的超融合数据库。本指南涵盖安装、配置和基本操作。',
     '数据库, 教程, 初学者, matrixone, 指南', 101, 1, 800),
    (2, 'en', 'Advanced Query Optimization', 'Techniques for improving query performance',
     'Query optimization is crucial for database performance. Learn about indexes, execution plans, and query rewriting.',
     'performance, optimization, query, advanced, indexes', 102, 2, 2300),
    (3, 'en', 'Temporary Tables Deep Dive', 'Understanding temporary table internals',
     'Temporary tables provide session-level data storage. They are automatically cleaned up when the session ends.',
     'temporary, tables, session, storage, cleanup', 103, 1, 950);

SHOW INDEX FROM temp_ft_composite_pk;

-- 复杂全文搜索查询
SELECT article_id, language_code, title, view_count
FROM temp_ft_composite_pk
WHERE MATCH(title, subtitle) AGAINST('MatrixOne guide' IN NATURAL LANGUAGE MODE)
ORDER BY view_count DESC;

-- 全文搜索 + 条件过滤
SELECT article_id, language_code, title, author_id
FROM temp_ft_composite_pk
WHERE MATCH(content) AGAINST('database performance' IN NATURAL LANGUAGE MODE)
  AND author_id IN (101, 102)
  AND view_count > 1000;

-- 标签搜索
SELECT article_id, title, tags
FROM temp_ft_composite_pk
WHERE MATCH(tags) AGAINST('optimization indexes' IN NATURAL LANGUAGE MODE);

DROP TABLE temp_ft_composite_pk;

-- 测试用例 2.2: 全文索引 + 多个唯一索引 + 复合索引
-- 预期结果: 创建成功
CREATE TEMPORARY TABLE temp_ft_complex_indexes (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    sku VARCHAR(50) UNIQUE NOT NULL,
    barcode VARCHAR(100) UNIQUE,
    product_name VARCHAR(200) NOT NULL,
    description TEXT,
    specifications TEXT,
    search_keywords TEXT,
    brand VARCHAR(100),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    price DECIMAL(10,2),
    stock_quantity INT,
    supplier_id INT,
    created_by INT,
    UNIQUE INDEX idx_sku_brand (sku, brand),
    INDEX idx_category_subcategory (category, subcategory),
    INDEX idx_price_stock (price, stock_quantity),
    INDEX idx_supplier (supplier_id),
    INDEX idx_brand_category (brand, category)
);

-- 创建多个全文索引
CREATE FULLTEXT INDEX idx_ft_name_desc ON temp_ft_complex_indexes(product_name, description);
CREATE FULLTEXT INDEX idx_ft_specs ON temp_ft_complex_indexes(specifications);
CREATE FULLTEXT INDEX idx_ft_keywords ON temp_ft_complex_indexes(search_keywords);

INSERT INTO temp_ft_complex_indexes (sku, barcode, product_name, description, specifications, search_keywords, brand, category, subcategory, price, stock_quantity, supplier_id, created_by) VALUES
    ('SKU-LAPTOP-001', '1234567890123', 'Professional Laptop 15.6 inch',
     'High-performance laptop with Intel i7 processor, 16GB RAM, and 512GB SSD. Perfect for developers and content creators.',
     'CPU: Intel Core i7-11800H, RAM: 16GB DDR4, Storage: 512GB NVMe SSD, Display: 15.6" FHD IPS, GPU: NVIDIA GTX 1650',
     'laptop, computer, notebook, intel, i7, gaming, professional, development, programming',
     'TechBrand', 'Electronics', 'Computers', 1299.99, 50, 1001, 1),
    ('SKU-MOUSE-001', '1234567890124', 'Wireless Gaming Mouse RGB',
     'Ergonomic wireless gaming mouse with customizable RGB lighting and programmable buttons.',
     'Sensor: 16000 DPI, Buttons: 8 programmable, Battery: 70 hours, Connectivity: 2.4GHz wireless + Bluetooth',
     'mouse, gaming, wireless, rgb, ergonomic, programmable, peripheral',
     'GameGear', 'Electronics', 'Accessories', 79.99, 200, 1002, 1),
    ('SKU-MONITOR-001', '1234567890125', '27 inch 4K UHD Monitor',
     'Professional 4K monitor with HDR support and wide color gamut for photo and video editing.',
     'Resolution: 3840x2160, Panel: IPS, Refresh Rate: 60Hz, HDR: HDR10, Color: 99% sRGB, Ports: HDMI 2.0, DisplayPort 1.4',
     'monitor, display, 4k, uhd, hdr, ips, professional, editing, design',
     'ViewPro', 'Electronics', 'Displays', 499.99, 30, 1003, 2);

SHOW INDEX FROM temp_ft_complex_indexes;

-- 复杂全文搜索 + 多条件过滤
SELECT product_id, sku, product_name, brand, price
FROM temp_ft_complex_indexes
WHERE MATCH(product_name, description) AGAINST('professional laptop' IN NATURAL LANGUAGE MODE)
  AND price BETWEEN 1000 AND 1500
  AND stock_quantity > 20;

-- 规格搜索
SELECT product_id, product_name, specifications
FROM temp_ft_complex_indexes
WHERE MATCH(specifications) AGAINST('16GB SSD' IN NATURAL LANGUAGE MODE);

-- 关键词搜索 + 分类过滤
SELECT product_id, product_name, category, subcategory, price
FROM temp_ft_complex_indexes
WHERE MATCH(search_keywords) AGAINST('gaming rgb' IN NATURAL LANGUAGE MODE)
  AND category = 'Electronics'
ORDER BY price DESC;

-- 多表达式全文搜索
SELECT product_id, product_name, brand, price
FROM temp_ft_complex_indexes
WHERE (MATCH(product_name, description) AGAINST('monitor 4k' IN NATURAL LANGUAGE MODE)
   OR MATCH(specifications) AGAINST('4K HDR' IN NATURAL LANGUAGE MODE))
  AND brand IN ('ViewPro', 'TechBrand');

DROP TABLE temp_ft_complex_indexes;

-- ============================================================================
-- 测试分类 3: 向量索引 - 基础场景
-- ============================================================================

-- 测试用例 3.1: 向量索引 + 主键（基础维度）
-- 预期结果: 创建成功
SET experimental_ivf_index = 1;

CREATE TEMPORARY TABLE temp_vec_basic (
    id INT PRIMARY KEY AUTO_INCREMENT,
    item_name VARCHAR(100),
    embedding vecf32(3),
    category VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_category (category)
);

CREATE INDEX idx_vec_embedding USING ivfflat
ON temp_vec_basic(embedding) lists=2 op_type "vector_l2_ops";

INSERT INTO temp_vec_basic (item_name, embedding, category) VALUES
    ('Item A', '[1.0, 2.0, 3.0]', 'Category1'),
    ('Item B', '[4.0, 5.0, 6.0]', 'Category1'),
    ('Item C', '[7.0, 8.0, 9.0]', 'Category2'),
    ('Item D', '[1.5, 2.5, 3.5]', 'Category2'),
    ('Item E', '[10.0, 11.0, 12.0]', 'Category3');

SHOW INDEX FROM temp_vec_basic;

-- 向量相似度搜索
SELECT id, item_name, category, embedding
FROM temp_vec_basic
ORDER BY l2_distance(embedding, '[1.0, 2.0, 3.0]')
LIMIT 3;

DROP TABLE temp_vec_basic;

-- 测试用例 3.2: 向量索引 + 复合主键 + 多个向量列
-- 预期结果: 创建成功，支持多个向量索引
CREATE TEMPORARY TABLE temp_vec_multi_vectors (
    user_id INT,
    session_id INT,
    user_profile_vec vecf32(8),
    behavior_vec vecf32(8),
    preference_vec vecf32(8),
    user_name VARCHAR(100),
    user_type VARCHAR(50),
    registration_date DATE,
    last_active TIMESTAMP,
    total_interactions INT DEFAULT 0,
    PRIMARY KEY (user_id, session_id),
    INDEX idx_user_type (user_type),
    INDEX idx_registration (registration_date),
    INDEX idx_interactions (total_interactions)
);

-- 为每个向量列创建索引
CREATE INDEX idx_vec_profile USING ivfflat
ON temp_vec_multi_vectors(user_profile_vec) lists=3 op_type "vector_l2_ops";

CREATE INDEX idx_vec_behavior USING ivfflat
ON temp_vec_multi_vectors(behavior_vec) lists=3 op_type "vector_l2_ops";

CREATE INDEX idx_vec_preference USING ivfflat
ON temp_vec_multi_vectors(preference_vec) lists=3 op_type "vector_l2_ops";

INSERT INTO temp_vec_multi_vectors (user_id, session_id, user_profile_vec, behavior_vec, preference_vec, user_name, user_type, registration_date, total_interactions) VALUES
    (1001, 1, '[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]', '[0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]', '[0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]', 'Alice Johnson', 'Premium', '2023-01-15', 1250),
    (1001, 2, '[0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85]', '[0.75, 0.65, 0.55, 0.45, 0.35, 0.25, 0.15, 0.05]', '[0.6, 0.6, 0.6, 0.6, 0.6, 0.6, 0.6, 0.6]', 'Alice Johnson', 'Premium', '2023-01-15', 1250),
    (1002, 1, '[0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2]', '[0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]', '[0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4]', 'Bob Smith', 'Standard', '2023-03-20', 850),
    (1003, 1, '[0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]', '[1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3]', '[0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7]', 'Charlie Brown', 'Premium', '2023-02-10', 2100),
    (1004, 1, '[0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]', '[0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2]', '[0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3]', 'Diana Prince', 'Standard', '2023-04-05', 650);

SHOW INDEX FROM temp_vec_multi_vectors;

-- 基于用户画像向量的相似度搜索
SELECT user_id, session_id, user_name, user_type
FROM temp_vec_multi_vectors
ORDER BY l2_distance(user_profile_vec, '[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]')
LIMIT 3;

-- 基于行为向量的相似度搜索 + 条件过滤
SELECT user_id, user_name, user_type, total_interactions
FROM temp_vec_multi_vectors
WHERE user_type = 'Premium'
ORDER BY l2_distance(behavior_vec, '[0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]')
LIMIT 2;

-- 基于偏好向量的相似度搜索 + 复杂过滤
SELECT user_id, session_id, user_name, registration_date
FROM temp_vec_multi_vectors
WHERE total_interactions > 800
  AND registration_date >= '2023-02-01'
ORDER BY l2_distance(preference_vec, '[0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]')
LIMIT 3;

DROP TABLE temp_vec_multi_vectors;

-- ============================================================================
-- 测试分类 4: 向量索引 - 高维向量场景
-- ============================================================================

-- 测试用例 4.1: 高维向量索引（128维）+ 主键 + 复合索引
-- 预期结果: 创建成功
CREATE TEMPORARY TABLE temp_vec_high_dim (
    image_id INT PRIMARY KEY AUTO_INCREMENT,
    image_name VARCHAR(200) NOT NULL,
    image_hash VARCHAR(64) UNIQUE,
    feature_vector vecf32(128),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    tags VARCHAR(500),
    upload_user_id INT,
    upload_date DATE,
    file_size_kb INT,
    width INT,
    height INT,
    format VARCHAR(10),
    is_public BOOLEAN DEFAULT TRUE,
    view_count INT DEFAULT 0,
    INDEX idx_category_subcat (category, subcategory),
    INDEX idx_upload_user (upload_user_id),
    INDEX idx_upload_date (upload_date),
    INDEX idx_dimensions (width, height),
    INDEX idx_public_views (is_public, view_count)
);

CREATE INDEX idx_vec_features USING ivfflat
ON temp_vec_high_dim(feature_vector) lists=5 op_type "vector_l2_ops";

-- 插入测试数据（使用简化的128维向量）
INSERT INTO temp_vec_high_dim (image_name, image_hash, feature_vector, category, subcategory, tags, upload_user_id, upload_date, file_size_kb, width, height, format, view_count) VALUES
    ('sunset_beach.jpg', 'a1b2c3d4e5f6',
     '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]',
     'Nature', 'Landscape', 'sunset,beach,ocean,sky,beautiful', 2001, '2024-01-15', 2048, 1920, 1080, 'JPEG', 15000),
    ('mountain_peak.jpg', 'b2c3d4e5f6g7',
     '[0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]',
     'Nature', 'Landscape', 'mountain,peak,snow,hiking,adventure', 2002, '2024-01-18', 3072, 2560, 1440, 'JPEG', 12000),
    ('city_night.jpg', 'c3d4e5f6g7h8',
     '[0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2]',
     'Urban', 'Cityscape', 'city,night,lights,buildings,urban', 2001, '2024-01-20', 2560, 1920, 1080, 'JPEG', 18000);

SHOW INDEX FROM temp_vec_high_dim;

-- 高维向量相似度搜索
SELECT image_id, image_name, category, view_count
FROM temp_vec_high_dim
ORDER BY l2_distance(feature_vector, '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]')
LIMIT 2;

-- 向量搜索 + 分类过滤
SELECT image_id, image_name, subcategory, width, height
FROM temp_vec_high_dim
WHERE category = 'Nature' AND is_public = TRUE
ORDER BY l2_distance(feature_vector, '[0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]')
LIMIT 2;

-- 向量搜索 + 复杂条件
SELECT image_id, image_name, upload_user_id, view_count
FROM temp_vec_high_dim
WHERE view_count > 10000
  AND upload_date >= '2024-01-15'
  AND width >= 1920
ORDER BY l2_distance(feature_vector, '[0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,1.0,0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2]')
LIMIT 3;

DROP TABLE temp_vec_high_dim;

-- ============================================================================
-- 测试分类 5: 全文索引 + 向量索引混合场景
-- ============================================================================

-- 测试用例 5.1: 全文索引 + 向量索引 + 复合主键 + 多种索引类型
-- 预期结果: 创建成功，支持混合搜索
CREATE TEMPORARY TABLE temp_hybrid_search (
    doc_id INT,
    version INT,
    title VARCHAR(300) NOT NULL,
    abstract TEXT,
    full_text TEXT,
    keywords TEXT,
    embedding_title vecf32(64),
    embedding_content vecf32(64),
    author_id INT,
    author_name VARCHAR(100),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    language VARCHAR(10),
    publish_date DATE,
    update_date DATE,
    citation_count INT DEFAULT 0,
    download_count INT DEFAULT 0,
    rating DECIMAL(3,2),
    is_featured BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (doc_id, version),
    UNIQUE INDEX idx_doc_version (doc_id, version),
    INDEX idx_author (author_id, author_name),
    INDEX idx_category_subcat (category, subcategory),
    INDEX idx_publish_date (publish_date),
    INDEX idx_citations_downloads (citation_count, download_count),
    INDEX idx_rating (rating),
    INDEX idx_featured_rating (is_featured, rating)
);

-- 创建全文索引
CREATE FULLTEXT INDEX idx_ft_title ON temp_hybrid_search(title);
CREATE FULLTEXT INDEX idx_ft_abstract ON temp_hybrid_search(abstract);
CREATE FULLTEXT INDEX idx_ft_fulltext ON temp_hybrid_search(full_text);
CREATE FULLTEXT INDEX idx_ft_keywords ON temp_hybrid_search(keywords);

-- 创建向量索引
CREATE INDEX idx_vec_title USING ivfflat
ON temp_hybrid_search(embedding_title) lists=4 op_type "vector_l2_ops";

CREATE INDEX idx_vec_content USING ivfflat
ON temp_hybrid_search(embedding_content) lists=4 op_type "vector_l2_ops";

-- 插入复杂测试数据
INSERT INTO temp_hybrid_search VALUES
    (1001, 1, 'Deep Learning for Natural Language Processing',
     'This paper presents novel approaches to NLP using deep learning techniques including transformers and attention mechanisms.',
     'Natural Language Processing has been revolutionized by deep learning. Transformer architectures, introduced in 2017, have become the foundation for modern NLP systems. BERT, GPT, and other models demonstrate remarkable performance across various tasks.',
     'deep learning, NLP, transformers, BERT, GPT, attention mechanism, neural networks',
     '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4]',
     '[0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4]',
     3001, 'Dr. Sarah Chen', 'AI/ML', 'NLP', 'en', '2024-01-15', '2024-01-15', 150, 2500, 4.75, TRUE),
    (1002, 1, 'Vector Databases for Similarity Search',
     'An overview of vector database systems and their applications in similarity search and recommendation systems.',
     'Vector databases have emerged as essential infrastructure for AI applications. They enable efficient similarity search using techniques like approximate nearest neighbor search. Applications include recommendation systems, image search, and semantic search.',
     'vector database, similarity search, ANN, embeddings, recommendation, semantic search',
     '[0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4]',
     '[0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4]',
     3002, 'Prof. Michael Zhang', 'Database', 'Vector DB', 'en', '2024-01-20', '2024-01-20', 95, 1800, 4.60, TRUE),
    (1003, 1, 'HTAP Database Architecture Patterns',
     'Exploring hybrid transactional and analytical processing database architectures for modern applications.',
     'HTAP databases combine OLTP and OLAP capabilities in a single system. This architecture eliminates the need for separate systems and ETL processes. MatrixOne is an example of a modern HTAP database that provides real-time analytics on transactional data.',
     'HTAP, database architecture, OLTP, OLAP, real-time analytics, MatrixOne',
     '[0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4]',
     '[0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4]',
     3003, 'Dr. Emily Rodriguez', 'Database', 'Architecture', 'en', '2024-02-01', '2024-02-01', 120, 2200, 4.80, TRUE);

SHOW INDEX FROM temp_hybrid_search;

-- 场景1: 纯全文搜索
SELECT doc_id, title, author_name, citation_count
FROM temp_hybrid_search
WHERE MATCH(title, abstract) AGAINST('database vector search' IN NATURAL LANGUAGE MODE)
ORDER BY citation_count DESC;

-- 场景2: 纯向量搜索
SELECT doc_id, title, category, rating
FROM temp_hybrid_search
ORDER BY l2_distance(embedding_title, '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4]')
LIMIT 2;

-- 场景3: 混合搜索 - 全文 + 向量 + 条件过滤
SELECT doc_id, version, title, author_name, category, rating, citation_count
FROM temp_hybrid_search
WHERE MATCH(keywords) AGAINST('database architecture' IN NATURAL LANGUAGE MODE)
  AND category = 'Database'
  AND rating >= 4.5
  AND is_featured = TRUE
ORDER BY l2_distance(embedding_content, '[0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4]')
LIMIT 3;

-- 场景4: 复杂混合查询 - 多个全文字段 + 向量 + 多条件
SELECT doc_id, title, author_name, category, subcategory,
       citation_count, download_count, rating
FROM temp_hybrid_search
WHERE (MATCH(title) AGAINST('deep learning NLP' IN NATURAL LANGUAGE MODE)
   OR MATCH(abstract) AGAINST('transformer attention' IN NATURAL LANGUAGE MODE))
  AND publish_date >= '2024-01-01'
  AND citation_count > 100
ORDER BY l2_distance(embedding_title, '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4]'),
     rating DESC
LIMIT 5;

-- 场景5: 基于内容向量的相似文档推荐 + 分类过滤
SELECT doc_id, title, author_name, rating, download_count
FROM temp_hybrid_search
WHERE category IN ('Database', 'AI/ML')
  AND rating >= 4.0
ORDER BY l2_distance(embedding_content, '[0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,0.1,0.2,0.3,0.4]')
LIMIT 3;

DROP TABLE temp_hybrid_search;
-- @bvt:issue
drop database temp_index;