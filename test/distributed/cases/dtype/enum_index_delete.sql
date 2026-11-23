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

-- Spatial indexes on the same table use the base PK as their physical key.
create table spatial_t (id int primary key, e enum('x','y'), g point not null, key ie(e));
create spatial index ig on spatial_t(g);
insert into spatial_t values (1,'x',st_geomfromtext('POINT(1 1)')),(2,'y',st_geomfromtext('POINT(2 2)'));
set @idx=(select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='enum_index_delete' and relname='spatial_t') and name='ig' limit 1);
set @q=concat('select count(*) from `',@idx,'`');
prepare idx_count from @q;
begin;
delete from spatial_t where id=1;
select row_count();
execute idx_count;
rollback;
execute idx_count;
delete from spatial_t where id=1 returning id;
execute idx_count;
select id from spatial_t where st_intersects(g,st_geomfromtext('POINT(2 2)'));
deallocate prepare idx_count;
create table spatial_float (pk double primary key, e enum('x','y'), g point not null, key ie(e));
create spatial index ig on spatial_float(g);
insert into spatial_float values (cast('NaN' as double),'x',st_geomfromtext('POINT(1 1)'));
set @idx=(select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='enum_index_delete' and relname='spatial_float') and name='ig' limit 1);
set @q=concat('select count(*) from `',@idx,'`');
prepare idx_count from @q;
delete from spatial_float where e='x';
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

-- NaN primary keys require physical identity when matching the index owner.
-- This covers both an existing backfill and an ordinary insert.
create table float_pk (pk float primary key, e enum('x','y'), key ix(e));
insert into float_pk values (cast('NaN' as float),'x');
delete from float_pk where e='x';
select row_count();
select count(*) from float_pk;

create table double_pk (pk double primary key, e enum('x','y'), tag varchar(10));
insert into double_pk values (cast('NaN' as double),'x','old');
alter table double_pk add key ix(e);
insert into double_pk values (2,'x','new');
begin;
delete from double_pk where tag='old';
select row_count();
rollback;
delete from double_pk where tag='old';
select row_count();
select pk,e,tag from double_pk order by tag;
delete from double_pk where tag='new';
select row_count();

-- A conflicting canonical unique key belongs to another owner. Removing a
-- NaN owner must remove its own backfilled key while preserving that owner.
create table double_unique (pk double primary key, e enum('x','y'), n int, tag varchar(10));
insert into double_unique values (cast('NaN' as double),'x',1,'old');
alter table double_unique add unique key uk(e,n);
insert into double_unique values (2,'x',1,'new');
set @idx=(select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='enum_index_delete' and relname='double_unique') and name='uk' limit 1);
set @q=concat('select count(*) from `',@idx,'`');
prepare idx_count from @q;
execute idx_count;
delete from double_unique where tag='old';
select row_count();
execute idx_count;
select pk,e,n,tag from double_unique;
-- @regex("Duplicate entry", true)
insert into double_unique values (3,'x',1,'again');
delete from double_unique where tag='new';
select row_count();
execute idx_count;
deallocate prepare idx_count;

-- Two non-unique indexes can contain equal physical keys for different
-- owners. Delete all matching addresses only for the selected owner, without
-- multiplying the base row or changing the RETURNING column order.
create table colliding_t (e enum('x','y'), n int, pk varchar(10) primary key, u int,
  key ie(pk(1),e), key inum(pk(1),n), unique key uk(e,u));
insert into colliding_t values ('x',42,'aa',1),('x',42,'ab',2);
set @ie=(select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='enum_index_delete' and relname='colliding_t') and name='ie' limit 1);
set @in=(select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='enum_index_delete' and relname='colliding_t') and name='inum' limit 1);
set @q=concat('select __mo_index_pri_col from `',@ie,'` order by 1');
prepare enum_owners from @q;
set @q=concat('select __mo_index_pri_col from `',@in,'` order by 1');
prepare num_owners from @q;
begin;
delete from colliding_t where pk='ab';
select row_count();
execute enum_owners;
execute num_owners;
rollback;
delete from colliding_t where pk='ab' returning n,pk,u;
select pk,n,u from colliding_t;
execute enum_owners;
execute num_owners;
delete from colliding_t where pk='aa';
select row_count();
execute enum_owners;
execute num_owners;
deallocate prepare enum_owners;
deallocate prepare num_owners;

-- RETURNING reads the ordered target selection before index maintenance.
create table returning_order (id int primary key, e enum('x','y'), n int, key ie(e), unique key uk(e,n));
insert into returning_order values (1,'x',90),(2,'x',20),(3,'x',70),(4,'x',10),(5,'x',60),(6,'x',30);
begin;
delete from returning_order where id>0 order by n desc limit 4 returning id,n;
rollback;
select count(*) from returning_order;

-- RETURNING also maintains a synchronous fulltext index from the same rows.
create table mixed_fulltext (id int primary key, e enum('x','y'), n int, body text, key ie(e));
create fulltext index ft on mixed_fulltext(body);
insert into mixed_fulltext values (1,'x',10,'alpha apple'),(2,'x',20,'beta berry'),(3,'y',30,'gamma grape'),(4,'y',40,'delta date');
set @ie=(select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='enum_index_delete' and relname='mixed_fulltext') and name='ie' limit 1);
set @ft=(select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='enum_index_delete' and relname='mixed_fulltext') and name='ft' limit 1);
set @q=concat('select count(*) from `',@ie,'`');
prepare enum_count from @q;
set @q=concat('select distinct doc_id from `',@ft,'` order by doc_id');
prepare ft_docs from @q;
execute enum_count;
execute ft_docs;
begin;
delete from mixed_fulltext where id>0 order by n desc limit 2 returning id,n;
select id,n from mixed_fulltext order by id;
execute enum_count;
execute ft_docs;
rollback;
select id,n from mixed_fulltext order by id;
execute enum_count;
execute ft_docs;
deallocate prepare enum_count;
deallocate prepare ft_docs;

-- A released DELETE could leave an index row before reinserting the same
-- base key. Seed that persisted state in both hidden tables so the independent
-- streams must remove every address while counting the base row only once.
create table duplicate_t (pk varchar(10) primary key, e enum('x','y'), n int,
  key ie(pk(1),e), key inum(pk(1),n));
insert into duplicate_t values ('aa','x',42),('ab','x',42);
set @ie=(select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='enum_index_delete' and relname='duplicate_t') and name='ie' limit 1);
set @in=(select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='enum_index_delete' and relname='duplicate_t') and name='inum' limit 1);
set @q=concat('insert into `',@ie,'` (__mo_index_idx_col,__mo_index_pri_col) select __mo_index_idx_col,__mo_index_pri_col from `',@ie,'` where __mo_index_pri_col=''aa''');
prepare seed_owner from @q;
execute seed_owner;
deallocate prepare seed_owner;
set @q=concat('insert into `',@in,'` (__mo_index_idx_col,__mo_index_pri_col) select __mo_index_idx_col,__mo_index_pri_col from `',@in,'` where __mo_index_pri_col=''aa''');
prepare seed_owner from @q;
execute seed_owner;
deallocate prepare seed_owner;
set @q=concat('select __mo_index_pri_col,count(*) from `',@ie,'` group by __mo_index_pri_col order by 1');
prepare enum_counts from @q;
set @q=concat('select __mo_index_pri_col,count(*) from `',@in,'` group by __mo_index_pri_col order by 1');
prepare num_counts from @q;
execute enum_counts;
execute num_counts;
begin;
delete from duplicate_t where pk='aa';
select row_count();
execute enum_counts;
execute num_counts;
rollback;
execute enum_counts;
execute num_counts;
delete from duplicate_t where pk='aa' returning pk,n;
select pk,n from duplicate_t;
execute enum_counts;
execute num_counts;
deallocate prepare enum_counts;
deallocate prepare num_counts;

-- With multiple special columns, the two encoded keys coincide only when
-- every such component is NULL. Empty SET values are non-NULL bitmaps.
create table multiple_special (id int primary key, e enum('red','green'), s set('','a'));
insert into multiple_special values (1,null,null),(2,null,0),(3,'red',null),(4,'red',1);
alter table multiple_special add key ix(e,s);
insert into multiple_special values (5,null,null),(6,null,0),(7,'red',null),(8,'red',1);
set @idx=(select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='enum_index_delete' and relname='multiple_special') and name='ix' limit 1);
set @q=concat('select count(*) from `',@idx,'`');
prepare idx_count from @q;
execute idx_count;
begin;
delete from multiple_special where id between 1 and 8;
select row_count();
execute idx_count;
rollback;
execute idx_count;
delete from multiple_special where id between 1 and 8;
select row_count();
execute idx_count;
deallocate prepare idx_count;

-- An ENUM prefix changes the key expression's type, so both physical keys
-- must be compared even when the original special value is non-NULL.
create table special_prefix (id int primary key, e enum('red','green'), n int);
insert into special_prefix values (1,'red',1);
alter table special_prefix add key ie(e(1));
alter table special_prefix add unique key uk(e(1),n);
insert into special_prefix values (2,'green',2),(3,null,3);
set @idx=(select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='enum_index_delete' and relname='special_prefix') and name='ie' limit 1);
set @q=concat('select count(*) from `',@idx,'`');
prepare idx_count from @q;
execute idx_count;
set @uk=(select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='enum_index_delete' and relname='special_prefix') and name='uk' limit 1);
set @q=concat('select count(*) from `',@uk,'`');
prepare uk_count from @q;
execute uk_count;
delete from special_prefix where id between 1 and 3;
select row_count();
execute idx_count;
execute uk_count;
deallocate prepare idx_count;
deallocate prepare uk_count;

-- Multi-target DELETE maintains the same physical owner through its legacy
-- index step and counts the two deleted base rows once each.
create table double_legacy (pk double primary key, e enum('x','y'), id int);
create table double_anchor (id int primary key);
insert into double_legacy values (cast('NaN' as double),'x',1);
alter table double_legacy add key ix(e);
insert into double_anchor values (1);
set @idx=(select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='enum_index_delete' and relname='double_legacy') and name='ix' limit 1);
set @q=concat('select count(*) from `',@idx,'`');
prepare idx_count from @q;
execute idx_count;
delete a,b from double_legacy a join double_anchor b on a.id=b.id where a.id=1;
select row_count();
execute idx_count;
deallocate prepare idx_count;

drop database enum_index_delete;
