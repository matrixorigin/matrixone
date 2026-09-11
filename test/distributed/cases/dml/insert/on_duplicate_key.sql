create table t1(a int primary key, b int);
delete from t1;
insert into t1 values (1,1);
insert into t1 values (1,2), (2,2) on duplicate key update b=b+10;
select * from t1;
delete from t1;
insert into t1 values (1,1);
insert into t1 values (1,2), (2,2) on duplicate key update b=values(b)+10;
select * from t1;
delete from t1;
insert into t1 values (1,1);
-- Flink's MySQL JDBC dialect repeats the primary key in the update list.
-- VALUES(a) is necessarily equal to the existing a when PRIMARY is the only
-- conflict arbiter, so this must update b without attempting to rewrite a.
insert into t1 values (1,20), (2,30) on duplicate key update a=values(a), b=values(b);
select * from t1 order by a;
-- Replaying the same Flink upsert is idempotent.
insert into t1 values (1,20), (2,30) on duplicate key update a=values(a), b=values(b);
select * from t1 order by a;
-- A primary-key-only no-op keeps the existing non-key value.
insert into t1 values (1,99) on duplicate key update a=values(a);
select * from t1 order by a;
-- A different incoming column is not a primary-key no-op.
insert into t1 values (1,99) on duplicate key update a=values(b);
select * from t1 order by a;
delete from t1;
insert into t1 values (1,1);
-- @bvt:issue#4423
insert into t1 values (1,11), (2,22), (3,33) on duplicate key update a=a+1,b=100;
select * from t1;
-- @bvt:issue
delete from t1;
insert into t1 values (1,1);
insert into t1 values (1,2), (1,22) on duplicate key update b=b+10;
select * from t1;
delete from t1;
insert into t1 values (1,1),(3,3);
insert into t1 values (1,2),(2,22) on duplicate key update a=a+1;
delete from t1;
insert into t1 values (1,1),(3,3);
insert into t1 values (1,2),(2,22),(3,33) on duplicate key update a=a+1;
drop table t1;
create table t1(a int, b int, c int, primary key(a, b));
delete from t1;
insert into t1 values (1,1,10);
insert into t1 values (1,1,20), (2,2,30) on duplicate key update a=values(a), b=values(b), c=values(c);
select * from t1 order by a, b;
insert into t1 values (1,1,20), (2,2,30) on duplicate key update a=values(a), b=values(b), c=values(c);
select * from t1 order by a, b;
insert into t1 values (1,1,99) on duplicate key update a=values(a), b=values(b);
select * from t1 order by a, b;
delete from t1;
insert into t1 values (1,1,1);
insert into t1 values (1,1,2), (2,2,2) on duplicate key update c=c+10;
select * from t1;
delete from t1;
insert into t1 values (1,1,1);
insert into t1 values (1,1,2), (2,2,2) on duplicate key update c=values(c)+10;
select * from t1;
drop table t1;
create table t1(a int, b int, c int, primary key(a, b), key(c));
delete from t1;
insert into t1 values (1,1,1);
insert into t1 values (1,1,2), (2,2,2) on duplicate key update c=c+10;
select * from t1;
delete from t1;
insert into t1 values (1,1,1);
insert into t1 values (1,1,2), (2,2,2) on duplicate key update c=values(c)+10;
select * from t1;
drop table t1;
create table t1(a int, b int, c int, primary key(a, b), key(b, c));
delete from t1;
insert into t1 values (1,1,1);
insert into t1 values (1,1,2), (2,2,2), (2,2,3) on duplicate key update c=c+10;
select * from t1;
delete from t1;
insert into t1 values (1,1,1);
insert into t1 values (1,1,2), (2,2,2), (2,2,3) on duplicate key update c=values(c)+10;
select * from t1;
drop table t1;
create table t1(a int unique key, b int);
delete from t1;
insert into t1 values (1,1);
insert into t1 values (1,2), (2,2) on duplicate key update b=b+10;
select * from t1;
delete from t1;
insert into t1 values (1,1);
insert into t1 values (1,2), (2,2) on duplicate key update b=values(b)+10;
select * from t1;
delete from t1;
insert into t1 values (1,1);
insert into t1 values (1,11), (2,22), (3,33) on duplicate key update a=a+1,b=100;
-- @bvt:issue#4423
select * from t1;
-- @bvt:issue
delete from t1;
insert into t1 values (1,1);
insert into t1 values (1,2), (1,22) on duplicate key update b=b+10;
select * from t1;
delete from t1;
insert into t1 values (1,1),(3,3);
insert into t1 values (1,2),(2,22) on duplicate key update a=a+1;
delete from t1;
insert into t1 values (1,1),(3,3);
insert into t1 values (1,2),(2,22),(3,33) on duplicate key update a=a+1;
drop table t1;
create table t1(a int, b int, c int, unique key(a, b));
delete from t1;
insert into t1 values (1,1,1);
insert into t1 values (1,1,2), (2,2,2) on duplicate key update c=c+10;
select * from t1;
delete from t1;
insert into t1 values (1,1,1);
insert into t1 values (1,1,2), (2,2,2) on duplicate key update c=values(c)+10;
select * from t1;
drop table t1;
CREATE TABLE IF NOT EXISTS indup_00(`id` INT UNSIGNED,`act_name` VARCHAR(20) NOT NULL,`spu_id` VARCHAR(30) NOT NULL,`uv`  BIGINT NOT NULL,`update_time` date default '2020-10-10' COMMENT 'lastest time',unique key idx_act_name_spu_id (act_name,spu_id));
insert into indup_00 values (1,'beijing','001',1,'2021-01-03'),(2,'shanghai','002',2,'2022-09-23'),(3,'guangzhou','003',3,'2022-09-23');
select * from indup_00 order by id;
insert into indup_00 values (6,'shanghai','002',21,'1999-09-23'),(7,'guangzhou','003',31,'1999-09-23') on duplicate key update `act_name`=VALUES(`act_name`), `spu_id`=VALUES(`spu_id`), `uv`=VALUES(`uv`);
-- @bvt:issue#4423
select * from indup_00 order by id;
-- @bvt:issue
drop table indup_00;
CREATE TABLE IF NOT EXISTS indup(
col1 INT primary key,
col2 VARCHAR(20) NOT NULL,
col3 VARCHAR(30) NOT NULL,
col4 BIGINT default 30
);
insert into indup values(22,'11','33',1), (23,'22','55',2),(24,'66','77',1),(25,'99','88',1),(22,'11','33',1) on duplicate key update col1=col1+col2;
-- @bvt:issue#4423
select * from indup;
insert into indup values(24,'1','1',100) on duplicate key update col1=2147483649;
select * from indup;
-- @bvt:issue
drop table indup;
create table t1(a int primary key, b int, c int);
insert into t1 values (1,1,1),(2,2,2);
insert into t1 values (1,9,1),(11,8,2) on duplicate key update a=a+10, c=10;
-- @bvt:issue#4423
select * from t1 order by a;
-- @bvt:issue

drop table if exists t1;
create table t1(a int primary key, b int unique key);
insert into t1 values (1,1),(2,2),(3,3);
-- With a secondary UNIQUE arbiter, VALUES(primary_key) is ambiguous: a
-- secondary-key conflict can select a row with a different existing PK.
insert into t1 values (1,20) on duplicate key update a = values(a), b = values(b);
insert into t1 values (20,1) on duplicate key update a = values(a), b = values(b);
select * from t1 order by a;
insert into t1 values (1,20) on duplicate key update b = b + 1;
insert into t1 values (20,1) on duplicate key update a = a + 1;
delete from t1;
insert into t1 values (1,1),(3,2);
insert into t1 values (1,2) on duplicate key update a = 10;
delete from t1;
insert into t1 values (1,1),(3,2);
insert into t1 values (1,2) on duplicate key update a = a+2;

drop table if exists t1;
create table t1(a int primary key, b int) partition by key(a) partitions 2;
insert into t1 values (1,1),(2,2);
insert into t1 values (1,1),(3,3) on duplicate key update b = 10;
select * from t1 order by a;
drop table if exists t1;
create table t1(a int, b int, c int, primary key(a,b)) partition by key(a,b) partitions 2;
insert into t1 values (1,1,1),(2,2,2);
insert into t1 values (1,1,1),(3,3,3) on duplicate key update c = 10;
select * from t1 order by a;
drop table if exists t1;
create table t1(a int primary key, b int);
insert into t1 values (1,1),(2,2);
prepare s1 from insert into t1 values (?,2) on duplicate key update b = 10;
set @a=1;
execute s1 using @a;
execute s1 using @a;
execute s1 using @a;
execute s1 using @a;

drop table if exists users;
create table users (id int primary key auto_increment, counter int, create_at datetime default current_timestamp, update_at datetime default current_timestamp on update current_timestamp);
insert into users (id, counter) values ('112',1);
select id, counter, create_at = update_at from users;
select sleep(1);
insert into users (id, counter) values ('112',2) on duplicate key update counter=counter+values(counter), create_at=current_timestamp();
select id, counter, create_at = update_at from users;

-- A primary-key-only no-op must not trigger implicit ON UPDATE. mo-tester uses
-- CLIENT_FOUND_ROWS, so it contributes one logical affected row; the same
-- statement shape must still insert a non-conflicting key.
drop table if exists t_pk_only_noop;
create table t_pk_only_noop (
    id int primary key,
    val int,
    updated_at timestamp default '2026-01-01 00:00:00' on update current_timestamp
);
insert into t_pk_only_noop (id, val) values (1, 10);
insert into t_pk_only_noop (id, val) values (1, 99) on duplicate key update id = values(id);
select row_count();
select id, val, updated_at = '2026-01-01 00:00:00' as unchanged from t_pk_only_noop order by id;
insert into t_pk_only_noop (id, val) values (1, 88), (2, 20) on duplicate key update id = values(id);
select row_count();
select id, val, updated_at = '2026-01-01 00:00:00' as unchanged from t_pk_only_noop order by id;
drop table t_pk_only_noop;

-- test for on duplicate key update with NULL values in multi-row insert
drop table if exists t_null_dup;

-- #28160: MySQL-compatible INSERT VALUES/SET row and column aliases.
drop table if exists t_odku_row_alias;
create table t_odku_row_alias (id int primary key, a int, b int);
insert into t_odku_row_alias values (1, 10, 100);
insert into t_odku_row_alias (b, id, a) values (30, 1, 20) as incoming(new_b, new_id, new_a) on duplicate key update a = t_odku_row_alias.a + incoming.new_a, b = incoming.new_b;
select * from t_odku_row_alias order by id;

delete from t_odku_row_alias;
insert into t_odku_row_alias values (1, 10, 100);
insert into t_odku_row_alias values (1, 7, 0) as n on duplicate key update a = t_odku_row_alias.a + 1, b = n.a, a = t_odku_row_alias.a + n.a;
select * from t_odku_row_alias order by id;

delete from t_odku_row_alias;
insert into t_odku_row_alias values (1, 10, 0);
insert into t_odku_row_alias values (1, 2, 0), (1, 3, 0), (2, 4, 0) as n on duplicate key update a = t_odku_row_alias.a + n.a, b = n.a;
select * from t_odku_row_alias order by id;

drop table if exists t_odku_scope_source;
create table t_odku_scope_source (x int primary key, y int);
insert into t_odku_scope_source values (1, 70);
delete from t_odku_row_alias;
insert into t_odku_row_alias values (1, 10, 0);
insert into t_odku_row_alias values (1, 5, 0) as n(x, y, z) on duplicate key update b = (select n.y + t_odku_row_alias.a from t_odku_scope_source as n where n.x = t_odku_row_alias.id);
select * from t_odku_row_alias order by id;

-- A target-correlated subquery must not read a stale target snapshot after an
-- earlier ordered assignment or across input rows that can hit the same target.
delete from t_odku_row_alias;
insert into t_odku_row_alias values (1, 10, 0);
insert into t_odku_row_alias values (1, 5, 0) as n(x, y, z) on duplicate key update a = t_odku_row_alias.a + 1, b = (select n.y + t_odku_row_alias.a from t_odku_scope_source as n where n.x = t_odku_row_alias.id);

delete from t_odku_row_alias;
insert into t_odku_row_alias values (1, 10, 0);
insert into t_odku_row_alias values (1, 5, 0), (1, 6, 0) as n(x, y, z) on duplicate key update b = (select n.y + t_odku_row_alias.a from t_odku_scope_source as n where n.x = t_odku_row_alias.id);

delete from t_odku_row_alias;
insert into t_odku_row_alias values (1, 10, 0);
insert into t_odku_row_alias (id, a, b) select 1, 5, 0 on duplicate key update b = (select s.y + t_odku_row_alias.a from t_odku_scope_source as s where s.x = t_odku_row_alias.id);

-- The target row must be available to a correlated subquery below the
-- candidate pipeline, even when the source alias does not shadow the row alias.
delete from t_odku_row_alias;
insert into t_odku_row_alias values (1, 10, 100);
insert into t_odku_row_alias values (1, 5, 0) as n on duplicate key update b = (select s.y from t_odku_scope_source as s where s.x = t_odku_row_alias.id);
select * from t_odku_row_alias order by id;

-- A scalar UPDATE expression must not run for a non-conflicting insert. The
-- target lookup is NULL, so this otherwise multi-row scalar result is unused.
drop table if exists t_odku_scope_multi;
create table t_odku_scope_multi (x int, y int);
insert into t_odku_scope_multi values (0, 10), (0, 20);
delete from t_odku_row_alias;
insert into t_odku_row_alias values (1, 10, 0);
insert into t_odku_row_alias values (2, 5, 0) as n on duplicate key update b = (select s.y from t_odku_scope_multi as s where s.x = coalesce(t_odku_row_alias.id, 0));
select * from t_odku_row_alias order by id;

-- A nested subquery input cannot be guarded by the outer target-match
-- predicate. Reject it before the unused UPDATE branch can execute.
-- @regex("target-correlated subqueries in on duplicate key update cannot be evaluated before duplicate-key action",true)
insert into t_odku_row_alias values (2, 5, 0) as n on duplicate key update b = (select (select q.y from t_odku_scope_multi as q) from t_odku_scope_multi as s where s.x = coalesce(t_odku_row_alias.id, 0));
drop table t_odku_scope_multi;

delete from t_odku_row_alias;
insert into t_odku_row_alias values (1, 10, 0);
insert into t_odku_row_alias values (1, 6, 0) as n on duplicate key update b = case when n.a > 5 then n.a else NULL end;
select * from t_odku_row_alias order by id;

delete from t_odku_row_alias;
insert into t_odku_row_alias values (1, 10, 0);
insert into t_odku_row_alias values (1, 5, 0) as n(x, y, z) on duplicate key update b = (select n.y + s.y from t_odku_scope_source as s where s.x = n.x);
select * from t_odku_row_alias order by id;
drop table t_odku_scope_source;

delete from t_odku_row_alias;
insert into t_odku_row_alias values (1, 10, 0);
insert into t_odku_row_alias set id = 1, a = 8, b = 9 as n on duplicate key update a = n.a, b = n.b;
insert into t_odku_row_alias set id = 2, a = 6, b = 7 as n on duplicate key update a = n.a, b = n.b;
select * from t_odku_row_alias order by id;

delete from t_odku_row_alias;
insert into t_odku_row_alias values (1, 10, 0);
prepare s_row_alias from insert into t_odku_row_alias values (?, ?, ?) as n(k, x, y) on duplicate key update a = t_odku_row_alias.a + n.x, b = n.y;
set @row_id = 1;
set @row_a = 4;
set @row_b = 8;
execute s_row_alias using @row_id, @row_a, @row_b;
set @row_a = 3;
set @row_b = 9;
execute s_row_alias using @row_id, @row_a, @row_b;
set @row_id = 2;
set @row_a = 7;
set @row_b = 6;
execute s_row_alias using @row_id, @row_a, @row_b;
select * from t_odku_row_alias order by id;
deallocate prepare s_row_alias;
drop table t_odku_row_alias;

-- A generated DEFAULT keeps the original row-alias positions while the
-- no-key ODKU route falls back to a plain insert.
drop table if exists t_odku_no_key_generated;
create table t_odku_no_key_generated (
    a int,
    g int generated always as (a + 1) stored
);
insert into t_odku_no_key_generated(a, g) values (1, default) as n(x, y) on duplicate key update a = n.x;
select * from t_odku_no_key_generated;
prepare s_odku_no_key_generated from insert into t_odku_no_key_generated(a, g) values (?, default) as n(x, y) on duplicate key update a = n.x + ?;
set @odku_no_key_a = 2;
set @odku_no_key_delta = 3;
execute s_odku_no_key_generated using @odku_no_key_a, @odku_no_key_delta;
select * from t_odku_no_key_generated order by a;
deallocate prepare s_odku_no_key_generated;
drop table t_odku_no_key_generated;

create table t_null_dup (id int primary key, a int, b int);
insert into t_null_dup values (1, 100, 100), (3, 300, 300);
insert into t_null_dup (id, a, b) values (1, NULL, NULL), (3, NULL, 30) on duplicate key update a = values(a), b = values(b);
select * from t_null_dup order by id;
drop table if exists t_null_dup;
