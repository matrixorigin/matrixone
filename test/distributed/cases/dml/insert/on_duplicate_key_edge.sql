-- Test: INSERT ON DUPLICATE KEY UPDATE edge cases
-- Covers: NULL values, multi-unique-key, VALUES() function, rewrite-to-REPLACE paths

-- ============================================================
-- Part 1: NULL handling with unique keys
-- ============================================================
drop table if exists t_odku_null;
create table t_odku_null (
    id int primary key,
    uk_col int unique,
    val varchar(50)
);

-- NULL unique key should not trigger duplicate
insert into t_odku_null values (1, NULL, 'first');
insert into t_odku_null values (2, NULL, 'second');
select * from t_odku_null order by id;

-- Conflict on PK, unique key is NULL
insert into t_odku_null values (1, NULL, 'updated') on duplicate key update val = 'pk_conflict_null_uk';
select * from t_odku_null order by id;

-- Conflict on unique key (non-NULL)
insert into t_odku_null values (3, NULL, 'third');
update t_odku_null set uk_col = 100 where id = 1;
insert into t_odku_null values (99, 100, 'conflict') on duplicate key update val = 'uk_conflict';
select * from t_odku_null order by id;

drop table if exists t_odku_null;

-- ============================================================
-- Part 2: Multiple unique keys
-- ============================================================
drop table if exists t_odku_multi_uk;
create table t_odku_multi_uk (
    id int primary key,
    uk1 int unique,
    uk2 varchar(20) unique,
    val int default 0
);

insert into t_odku_multi_uk values (1, 10, 'a', 100);
insert into t_odku_multi_uk values (2, 20, 'b', 200);

-- Conflict on uk1 only
insert into t_odku_multi_uk values (3, 10, 'c', 300) on duplicate key update val = val + 1;
select * from t_odku_multi_uk order by id;

-- Conflict on uk2 only
insert into t_odku_multi_uk values (4, 40, 'b', 400) on duplicate key update val = val + 2;
select * from t_odku_multi_uk order by id;

-- Conflict on both uk1 and uk2 pointing to SAME row
insert into t_odku_multi_uk values (5, 10, 'a', 500) on duplicate key update val = val + 10;
select * from t_odku_multi_uk order by id;

-- Conflict on uk1 and uk2 pointing to DIFFERENT rows
-- This should be an error (MySQL errno 1062)
-- @ignore:0
insert into t_odku_multi_uk values (5, 10, 'b', 500) on duplicate key update val = val + 100;

drop table if exists t_odku_multi_uk;

-- ============================================================
-- Part 3: Composite primary key + unique key
-- ============================================================
drop table if exists t_odku_composite;
create table t_odku_composite (
    a int,
    b int,
    c varchar(20) unique,
    val int default 0,
    primary key (a, b)
);

insert into t_odku_composite values (1, 1, 'x', 10);
insert into t_odku_composite values (1, 2, 'y', 20);

-- Conflict on composite PK
insert into t_odku_composite values (1, 1, 'z', 30) on duplicate key update val = val + 100;
select * from t_odku_composite order by a, b;

-- Conflict on unique key
insert into t_odku_composite values (2, 1, 'y', 40) on duplicate key update val = val + 200;
select * from t_odku_composite order by a, b;

drop table if exists t_odku_composite;

-- ============================================================
-- Part 4: VALUES() function in update clause
-- ============================================================
drop table if exists t_odku_values;
create table t_odku_values (
    id int primary key,
    name varchar(50),
    score int
);

insert into t_odku_values values (1, 'alice', 80);
insert into t_odku_values values (1, 'alice_new', 95) on duplicate key update name = values(name), score = values(score);
select * from t_odku_values;

-- Batch insert with VALUES()
insert into t_odku_values values (1, 'bob', 70), (2, 'carol', 85), (3, 'dave', 90)
    on duplicate key update name = values(name), score = values(score) + 5;
select * from t_odku_values order by id;

drop table if exists t_odku_values;

-- ============================================================
-- Part 5: ODKU with expressions and functions
-- ============================================================
drop table if exists t_odku_expr;
create table t_odku_expr (
    id int primary key,
    cnt int default 0,
    last_updated timestamp default current_timestamp
);

insert into t_odku_expr (id, cnt) values (1, 1);
insert into t_odku_expr (id, cnt) values (1, 1) on duplicate key update cnt = cnt + values(cnt);
select id, cnt from t_odku_expr;

-- Multiple conflicting rows in single INSERT
insert into t_odku_expr (id, cnt) values (1, 10), (2, 20), (1, 30)
    on duplicate key update cnt = cnt + values(cnt);
-- @sortkey:0
select id, cnt from t_odku_expr;

drop table if exists t_odku_expr;

-- ============================================================
-- Part 6: ODKU with auto_increment
-- ============================================================
drop table if exists t_odku_auto;
create table t_odku_auto (
    id int auto_increment primary key,
    uk_val int unique,
    data varchar(50)
);

insert into t_odku_auto (uk_val, data) values (1, 'first');
insert into t_odku_auto (uk_val, data) values (2, 'second');
insert into t_odku_auto (uk_val, data) values (1, 'conflict') on duplicate key update data = 'updated_first';
select * from t_odku_auto order by id;

-- auto_increment should not create gaps unnecessarily
insert into t_odku_auto (uk_val, data) values (3, 'third');
select * from t_odku_auto order by id;

drop table if exists t_odku_auto;
