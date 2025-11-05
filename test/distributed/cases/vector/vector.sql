-- ==================================================
-- matrixone vector test
-- ==================================================
drop database if exists mo_vec_demo;
create database mo_vec_demo;
use mo_vec_demo;
set experimental_hnsw_index=1;

-- basic table: 4-dimensional vector + some scalar columns
drop table if exists items;
create table items (
  id        bigint primary key,
  title     varchar(64),
  category  varchar(16),
  price     float,
  emb       vecf32(4)
);

-- optional: the second table is used for migration/comparison (which will be used in the subsequent "structural changes")
drop table if exists items_v2;
create table items_v2 (
  id        bigint primary key,
  title     varchar(64),
  category  varchar(16),
  price     float,
  emb       vecf32(8)
);

-- crud:insert/select/update/delete/re-insert
insert into items (id,title,category,price,emb) values
(1,'doc-1','a',10.5,'[0.10,0.20,0.30,0.40]'),
(2,'doc-2','b',20.0,'[0.05,0.21,0.28,0.33]'),
(3,'doc-3','a',12.0,'[0.20,0.10,0.00,0.60]');

-- select
select id,title,category,price,emb from items order by id;
-- update
update items set price=18.8 where id=2;
select * from items;
update items set emb='[0.12,0.22,0.32,0.42]' where id=1;
-- delete
delete from items where id=3;
select * from items;
-- re-insert
insert into items (id,title,category,price,emb)
values (3,'doc-3','a',11.1,'[0.20,0.12,0.01,0.55]');
select * from items;

-- vector index:create/delete/re-create
-- delete old index
drop index if exists idx_items_emb on items;
-- create vector index
create index idx_items_emb using hnsw on items (emb) op_type "vector_l2_ops" m 48 ef_construction 64 ef_search 64;
show create table items;
-- delete vector index
drop index idx_items_emb on items;
show create table items;

-- table schema update
alter table items add column tags varchar(64);
show create table items;
alter table items drop column tags;
show create table items;

insert into items_v2 (id,title,category,price,emb)
select id,title,category,price,
       '[0.10,0.20,0.30,0.40,0.01,0.02,0.03,0.04]'
from items;

-- transaction
start transaction;
  insert into items values (10,'tx-doc','b',9.9,'[0.01,0.02,0.03,0.04]');
  update items set price=19.9 where id=2;
  select id, l2_distance(emb,'[0.01,0.02,0.03,0.04]') as dist
  from items order by dist asc limit 1;
rollback;
select * from items where id=10 or id=2;


-- boundary/abnormal test
insert into items values (100,'bad','x',0.1,'[0.1,0.2,0.3]');
insert into items values (11,'null-vec','a',8.8,null);
select id,
       emb is null as is_null
from items
order by id;


-- create a main table containing vector columns. vecf32 represents a vector of type float32
drop table if exists items;
create table items (
    id bigint primary key,
    name varchar(100),
    description text,
    image_vector vecf32(4),
    text_vector vecf32(10),
    category varchar(50),
    created_at timestamp default now()
);
-- insert single data
insert into items (id, name, description, image_vector, text_vector, category)
values (
    1,
    '示例产品',
    '这是一个测试产品的描述',
    '[0.1, 0.2, 0.3, 0.4]',
    '[0.12, -0.23, 0.33, 0.67, 0.22, 0.99, 10.11, 12.00, 0.12, 0.99]',
    'electronics'
);

-- batch insert
insert into items (id, name, image_vector, text_vector, category) values
(2, '产品a', '[0.15, 0.25, 0.35, 0.45]', '[0.15, -0.25, 0.35, -1.00, -8.89, -12.11, 12.11, 9.11, 22.0, 0.75]', 'books'),
(3, '产品b', '[0.05, 0.15, 0.25, 0.35]', '[0.05, -0.15, 0.25, 0.25, -0.35, 0.45, 0.25, -0.35, 0.45, 0.65]', 'electronics'),
(4, '产品c', '[0.25, 0.35, 0.45, 0.55]', '[0.25, -0.35, 0.45, 0.15, -0.25, 0.35, -1.00, -8.89, -12.11, 12.11]', 'clothing');
-- @ignore:6
select * from items;

-- Use the vector distance function to query and calculate the Euclidean distance from the target vector, and sort it
select
    id,
    name,
    text_vector,
    l2_distance(text_vector, '[0.12, -0.23, 0.33, 0.67, 1.11, 2.13, 3.14, -0.99, 10.11, 11.00]') as dist
from items
order by dist asc
limit 3;

-- update the vector data of a specific id
update items
set image_vector = '[0.18, 0.28, 0.38, 0.48]', name = '更新后的产品a'
where id = 2;

-- delete the vector data of a specific id
delete from items where id = 3;

-- re-insert
insert into items (id, name, image_vector, text_vector, category)
values (
    3,
    '重新插入的产品b',
    '[0.05, 0.15, 0.25, 0.35]',
    '[0.05, -0.15, 0.25, 0.25, -0.35, 0.45, 0.25, -0.35, 0.45, 0.65]',
    'electronics'
);

-- create vector index
create index idx_items_text_vector_ivf using hnsw on items(text_vector) op_type "vector_l2_ops" m 48 ef_construction 64 ef_search 64;

-- create vector index
create index idx_items_image_vector_hnsw using hnsw on items(image_vector) op_type "vector_l2_ops" m 48 ef_construction 64 ef_search 64;
show create table items;
desc items;

-- delete vector index
drop index idx_items_text_vector_ivf on items;
drop index idx_items_image_vector_hnsw on items;

-- use the distance function to find the item that is most similar to the target vector
select
    id,
    name,
    cosine_similarity(text_vector, '[0.12, -0.23, 0.33, 0.67, -10.11, 111.11, -90.1, 89.00, -10.11, 0.11]') as similarity
from items
where category = 'electronics'
order by similarity desc
limit 5;

-- transaction, automatic
start transaction;
insert into items (id, name, image_vector, text_vector) values (10, '事务产品a', '[0.1, 0.1, 0.1, 0.1]', '[0.12, -0.23, 0.33, 0.67, -10.11, 111.11, -90.1, 89.00, -10.11, 0.11]');
update items set name = '事务更新' where id = 2;
-- @ignore:6
select * from items;
commit;

-- rollback
-- roll back the transaction to confirm that the insertion and deletion operations have not taken effect
start transaction;
insert into items (id, name, image_vector, text_vector) values (12, '事务产品a', '[0.1, 0.1, 0.1, 0.1]', '[0.12, -0.23, 0.33, 0.67, -10.11, 111.11, -90.1, 89.00, -10.11, 0.11]');
delete from items where id = 4;
-- @ignore:6
select * from items;
rollback;
-- @ignore:6
select * from items;

-- 5.3 null vector
insert into items (id, name, image_vector, text_vector) values (5, '空向量测试', null, null);

-- dimension mismatch (should fail)
insert into items (id, name, image_vector) values (6, '错误维度', '[0.1, 0.2]'); -- 应为4维

-- table Structure Change and Data Migration (Upgrade of Simulation Vector Dimension
create table items_new (
    id int primary key,
    name varchar(100),
    description text,
    image_vector vecf32(5),
    text_vector vecf32(10),
    category varchar(50),
    created_at timestamp default now()
);
insert into items_new (id, name, description, image_vector, text_vector, category, created_at)
select
    id,
    name,
    description,
    image_vector,
    text_vector,
    category,
    created_at
from items;

-- alter table
alter table items drop column image_vector;
-- @ignore:5
select * from items;
desc items;
show create table items;
drop table items;
alter table items_new rename to items;

drop database if exists mo_vec_demo;