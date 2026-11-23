-- Stored ENUM/SET values and their display values must identify the same row
-- for DELETE, including indexes backfilled before later ordinary inserts.
drop database if exists enum_index_delete;
create database enum_index_delete;
use enum_index_delete;

-- The indexes are backfilled at different times, so one row may use a
-- different encoding in each index. Include NULL and indistinguishable SET
-- display values, and verify transaction rollback and DELETE RETURNING.
create table mixed_t (id int primary key, e enum('z','aa'), s set('','a'), tag varchar(10));
insert into mixed_t values (1,'z',1,'old');
alter table mixed_t add key ie(e);
insert into mixed_t values (2,'aa',2,'mid');
alter table mixed_t add key ite(tag,e);
alter table mixed_t add key its(tag,s);
insert into mixed_t values (3,'aa',0,'new'),(4,'z',1,'four'),(5,null,0,'null');
begin;
delete from mixed_t where id=1;
select row_count();
rollback;
select id,e,cast(s as unsigned),tag from mixed_t order by id;
delete from mixed_t where id=1;
select row_count();
delete from mixed_t where id=2 returning id,tag;
delete from mixed_t where id in (3,4) order by concat(e,''),id limit 1 returning id,tag;
delete from mixed_t where id=5;
select row_count();
select id,e,cast(s as unsigned),tag from mixed_t order by id;
delete from mixed_t where id=1;
select row_count();

-- A legacy backfill and a later insert can produce distinct physical unique
-- keys for the same logical value. Deleting one must not delete the other's
-- index record or release its uniqueness constraint.
create table unique_t (id int primary key, e enum('red','green'), n int);
insert into unique_t values (1,'green',7);
alter table unique_t add unique key uk(e,n);
insert into unique_t values (2,'green',7);
delete from unique_t where id=1;
select row_count();
select id,e,n from unique_t order by id;
-- @regex("Duplicate entry", true)
insert into unique_t values (3,'green',7);
delete from unique_t where id=2;
select row_count();
insert into unique_t values (3,'green',7);
select id,e,n from unique_t order by id;

-- Duplicate source matches must still delete a single target row.
create table join_t (id int primary key, e enum('red','green'), key ix(e));
create table join_source (id int, marker int);
insert into join_t values (1,'green'),(2,'red');
insert into join_source values (1,11),(1,12),(2,13);
delete t from join_t t join join_source s on t.id=s.id where t.id=1;
select row_count();
select id,e from join_t order by id;

-- Multi-target DELETE uses the legacy planner. Its index maintenance must
-- resolve each target independently and preserve another row's unique key.
create table legacy_a (id int primary key, e enum('red','green'));
insert into legacy_a values (1,'green');
alter table legacy_a add key ix(e);
insert into legacy_a values (2,'green');
create table legacy_b (id int primary key, e enum('red','green'), n int);
insert into legacy_b values (1,'green',7);
alter table legacy_b add unique key uk(e,n);
insert into legacy_b values (2,'green',7);
delete a,b from legacy_a a join legacy_b b on a.id=b.id where a.id=1;
select row_count();
select id,e from legacy_a order by id;
select id,e,n from legacy_b order by id;
-- @regex("Duplicate entry", true)
insert into legacy_b values (3,'green',7);
delete a,b from legacy_a a join legacy_b b on a.id=b.id where a.id=2;
select row_count();
select count(*) from legacy_a;
select count(*) from legacy_b;

-- Existing UPDATEs can leave both physical encodings for one owner. DELETE
-- must remove both hidden rows without counting the main row twice.
create table dual_t (id int primary key, e enum('z','aa'), n int);
insert into dual_t values (1,'z',1);
alter table dual_t add unique key uk(e,n);
update dual_t set e='aa' where id=1;
update dual_t set e='z' where id=1;
set @idx=(select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='enum_index_delete' and relname='dual_t') and name='uk' limit 1);
set @q=concat('select count(*) from `',@idx,'`');
prepare idx_count from @q;
execute idx_count;
delete from dual_t where id=1;
select row_count();
execute idx_count;
deallocate prepare idx_count;

create table dual_legacy (id int primary key, e enum('z','aa'), n int);
create table dual_source (id int primary key);
insert into dual_legacy values (1,'z',1);
insert into dual_source values (1);
alter table dual_legacy add unique key uk(e,n);
update dual_legacy set e='aa' where id=1;
update dual_legacy set e='z' where id=1;
set @idx=(select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='enum_index_delete' and relname='dual_legacy') and name='uk' limit 1);
set @q=concat('select count(*) from `',@idx,'`');
prepare idx_count from @q;
execute idx_count;
delete a,b from dual_legacy a join dual_source b on a.id=b.id where a.id=1;
select row_count();
execute idx_count;
deallocate prepare idx_count;

drop database enum_index_delete;
