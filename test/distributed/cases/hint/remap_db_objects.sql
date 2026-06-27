-- remapdb effectiveness scope: it remaps TABLE-LEVEL objects (the database part
-- of a qualified reference, and the current database for unqualified references)
-- for DML and table/view/index DDL; it does NOT affect DATABASE-LEVEL statements
-- (USE, CREATE/DROP/ALTER DATABASE).
--
-- Setup: dsrc is the remap source, ddst the destination. Both have a `base`
-- table with DISTINCT data so we can tell which one a reference resolved to.

drop database if exists dsrc;
drop database if exists ddst;
create database dsrc;
create database ddst;
use dsrc;
create table base(id int, v int);
insert into base values (1,1),(2,2),(3,3);
use ddst;
create table base(id int, v int);
insert into base values (10,100),(20,200),(30,300);
use mysql;

set enable_remap_hint = 1;

-- ========================================================================
-- TABLE-LEVEL via QUALIFIED reference (dsrc.x): remapped to ddst.x
-- ========================================================================
set remap_rewrites = '{"remapdb": {"dsrc": "ddst"}}';

-- SELECT: dsrc.base resolves to ddst.base (10/20/30 rows, not 1/2/3)
select * from dsrc.base order by id;
-- INSERT / UPDATE / DELETE land in ddst.base
insert into dsrc.base values (40,400);
update dsrc.base set v = 999 where id = 10;
delete from dsrc.base where id = 20;
set remap_rewrites = '';
-- verify on the real ddst.base; dsrc.base is untouched
select * from ddst.base order by id;
select * from dsrc.base order by id;

-- DDL: CREATE/ALTER/DROP table/view/index on dsrc.* land in ddst.*
set remap_rewrites = '{"remapdb": {"dsrc": "ddst"}}';
create table dsrc.qt(a int);
create view dsrc.qv as select id from dsrc.base;
create index qidx on dsrc.qt(a);
alter table dsrc.qt add column b int;
set remap_rewrites = '';
-- where did they land?
select concat(table_schema,'.',table_name) as obj from information_schema.tables where table_name in ('qt','qv') order by 1;
select table_schema from information_schema.columns where table_name='qt' and column_name='b';
select concat(table_schema,'.',table_name) as idx_on from information_schema.statistics where index_name='qidx';
-- DROP through the remap removes them from ddst
set remap_rewrites = '{"remapdb": {"dsrc": "ddst"}}';
drop index qidx on dsrc.qt;
drop view dsrc.qv;
drop table dsrc.qt;
set remap_rewrites = '';
select count(*) as remaining_qt_qv from information_schema.tables where table_name in ('qt','qv') and table_schema in ('dsrc','ddst');

-- ========================================================================
-- TABLE-LEVEL via the CURRENT DATABASE (unqualified, current db = dsrc)
-- ========================================================================
set remap_rewrites = '{"remapdb": {"dsrc": "ddst"}}';
use dsrc;                       -- USE is NOT remapped: current db is the real dsrc
-- but unqualified names resolve in ddst
select * from base order by id; -- ddst.base
insert into base values (50,500);
update base set v = 888 where id = 30;
delete from base where id = 40;
create table ut(a int);
create view uv as select id from base;
create index uidx on ut(a);
alter table ut add column b int;
set remap_rewrites = '';
-- the unqualified objects landed in ddst, not the real dsrc
select concat(table_schema,'.',table_name) as obj from information_schema.tables where table_name in ('ut','uv') order by 1;
select * from ddst.base order by id;
-- the real dsrc.base is still its original 1,2,3
select * from dsrc.base order by id;

-- ========================================================================
-- DATABASE-LEVEL statements are NOT remapped (current db = dsrc, remap active)
-- ========================================================================
set remap_rewrites = '{"remapdb": {"dsrc": "ddst"}}';
use dsrc;
-- USE not remapped: current database is the real dsrc
select database() as curdb;
-- CREATE DATABASE uses the name as written (a remap of its name would be wrong)
set remap_rewrites = '{"remapdb": {"newdb": "ddst"}}';
create database newdb;
set remap_rewrites = '';
select count(*) as newdb_created from information_schema.schemata where schema_name='newdb';
select count(*) as ddst_unchanged from information_schema.schemata where schema_name='ddst';
-- ALTER DATABASE / DROP DATABASE target the named database, not the remap target
set remap_rewrites = '{"remapdb": {"newdb": "ddst"}}';
alter database newdb set mysql_compatibility_mode = '8.0.0';
drop database newdb;
set remap_rewrites = '';
select count(*) as newdb_dropped_ddst_alive from information_schema.schemata where schema_name in ('newdb');
select count(*) as ddst_still_alive from information_schema.schemata where schema_name='ddst';

set remap_rewrites = '';
set enable_remap_hint = 0;
use mysql;
drop database if exists dsrc;
drop database if exists ddst;
