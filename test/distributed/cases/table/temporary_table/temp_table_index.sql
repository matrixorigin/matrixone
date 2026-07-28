drop database if exists test_temp_idx;
create database test_temp_idx;
use test_temp_idx;

-- 1. Create temporary table with inline index
create temporary table t1 (
    id int,
    name varchar(20),
    index idx_name (name)
);
show index from t1;
show create table t1;
insert into t1 values (1, 'apple'), (2, 'banana'), (3, 'cherry');
select * from t1 where name = 'banana';
drop table t1;

-- 2. Create temporary table and then add index using CREATE INDEX
create temporary table t2 (
    id int,
    age int
);
insert into t2 values (1, 20), (2, 30), (3, 25);
create index idx_age on t2(age);
show index from t2;
show create table t2;
select * from t2 where age > 20;

-- 3. Test generic index features (Composite, Unique)
create temporary table t3 (
    c1 int,
    c2 int,
    c3 varchar(10),
    primary key(c1)
);
create index idx_c2_c3 on t3(c2, c3);
insert into t3 values (1, 10, 'A'), (2, 20, 'B'), (3, 10, 'C');
show index from t3;
select * from t3 where c2 = 10;
-- Test Unique Index
create unique index idx_unique_c3 on t3(c3);
-- Should succeed
insert into t3 values (4, 30, 'D');
select * from t3;

-- 4. Test DROP INDEX
drop index idx_c2_c3 on t3;
show index from t3;

-- 5. Test ALTER TABLE ADD INDEX
alter table t3 add index idx_new (c2);
show index from t3;

-- 6. Temporary table with FULLTEXT index
create temporary table t4 (
    id int primary key,
    title varchar(50),
    body varchar(100)
);
insert into t4 values
    (1, 'apple news', 'red apple and fruit'),
    (2, 'banana news', 'yellow banana'),
    (3, 'cherry note', 'sweet cherry note');
create fulltext index idx_ft on t4(title, body);
show create table t4;
select id, title from t4 where match(title, body) against ('apple');
drop table t4;

-- 7. Temporary table with IVF index
set experimental_ivf_index = 1;
create temporary table t5 (
    id int primary key,
    v vecf32(3)
);
insert into t5 values
    (1, '[1,2,3]'),
    (2, '[2,2,3]'),
    (3, '[5,5,5]');
create index idx_ivf using ivfflat on t5(v) lists=1 op_type "vector_l2_ops";
show create table t5;
select id, v from t5 order by L2_DISTANCE(v, "[2,2,3]") limit 2;
drop table t5;
set experimental_ivf_index = 0;

drop database test_temp_idx;
