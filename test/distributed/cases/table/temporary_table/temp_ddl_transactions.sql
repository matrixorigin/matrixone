-- Temporary definitions are session-owned; data remains transactional.
drop database if exists temp_ddl_transactions;
create database temp_ddl_transactions;
use temp_ddl_transactions;
create table base (id int primary key);

-- CREATE survives rollback, without committing permanent or temporary DML.
start transaction;
insert into base values (1);
create temporary table tc (id int primary key);
insert into tc values (1);
rollback;
select count(*) as n from base;
select count(*) as n from tc;
insert into tc values (2);
select id from tc order by id;

-- DROP survives rollback; recreation proves that the old definition is gone.
create temporary table td (id int primary key);
insert into td values (1);
start transaction;
insert into base values (3);
insert into td values (2);
drop temporary table td;
rollback;
select count(*) as n from base;
create temporary table td (id int primary key);
insert into td values (3);
select id from td order by id;

-- Pending temporary writes cannot make an unrelated permanent COMMIT fail.
start transaction;
insert into base values (4);
update td set id = 5;
drop table td;
commit;
select id from base order by id;
create temporary table td (id int primary key);
select count(*) as n from td;

-- Physical generations do not collide or restore a previously dropped table.
start transaction;
create temporary table tg (id int primary key);
insert into tg values (10);
drop temporary table tg;
create temporary table tg (id int primary key);
insert into tg values (20);
rollback;
select count(*) as n from tg;
insert into tg values (30);
start transaction;
insert into tg values (40);
rollback;
select id from tg order by id;

-- CTAS reads the parent workspace; rollback retains only its definition.
start transaction;
insert into base values (5);
create temporary table ta as select id from base;
select id from ta order by id;
rollback;
select id from base order by id;
select count(*) as n from ta;

-- CTAS data failure retires the failed generation and permits immediate retry.
start transaction;
-- @regex("Duplicate entry",true)
create temporary table tf (id int primary key) as select 1 as id union all select 1 as id;
create temporary table tf (id int primary key);
rollback;
select count(*) as n from tf;

-- Prepared CREATE uses the same definition/data boundary.
prepare create_temp from 'create temporary table tp (id int primary key)';
start transaction;
execute create_temp;
insert into tp values (1);
rollback;
select count(*) as n from tp;
deallocate prepare create_temp;

-- Hidden index definitions survive with the empty root table.
start transaction;
create temporary table ti (id int primary key, v int, unique key uk(v));
insert into ti values (1, 10);
rollback;
select count(*) as n from ti;
insert into ti values (2, 20);
select id, v from ti order by id;

-- Existing temporary data keeps the normal DML rollback contract.
start transaction;
update tc set id = 3;
delete from tc;
insert into tc values (4);
rollback;
select id from tc order by id;

-- New temporary schema must not advance an established permanent SI snapshot.
set session transaction isolation level repeatable read;
start transaction;
select id from base order by id;
-- @session:id=2&user=sys:dump&password=111
insert into temp_ddl_transactions.base values (6);
-- @session
create temporary table ts (id int primary key);
insert into ts values (1);
select id from base order by id;
commit;
select id from ts order by id;
select id from base order by id;
set session transaction isolation level read committed;

drop temporary table tc, td, tg, ta, tp, ti, ts, tf;
drop database temp_ddl_transactions;
