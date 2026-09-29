-- remapdb: the remap_rewrites session variable (and inline hint) can remap one
-- database name to another. A qualified reference <src>.t resolves to <dst>.t,
-- and when the current database is <src>, UNQUALIFIED names resolve against
-- <dst> too. USE is NOT affected by remapdb (it switches to the named database
-- as written). remapdb is applied before the table rewrites.

drop database if exists rdb_src;
drop database if exists rdb_dst;

set global lower_case_table_names = 0;
-- @session

-- @session:id=2&user=sys:root&password=111
drop database if exists SrcMix27190;
drop database if exists DstMix27190;
create database SrcMix27190;
create database DstMix27190;
create table DstMix27190.t(id int primary key, v int);
set enable_remap_hint = 1;
set remap_rewrites = '{"remapdb": {"SrcMix27190": "DstMix27190"}}';
insert into SrcMix27190.t(SrcMix27190.t.id, SrcMix27190.t.v) values (1, 10);
insert into SrcMix27190.t set SrcMix27190.t.id = 2, SrcMix27190.t.v = 20;
insert into SrcMix27190.t(SrcMix27190.t.id, SrcMix27190.t.v) select 3, 30;
replace into SrcMix27190.t(SrcMix27190.t.id, SrcMix27190.t.v) values (4, 40);
insert into SrcMix27190.t(SrcMix27190.t.id, SrcMix27190.t.v) values (1, 15)
    on duplicate key update v = values(SrcMix27190.t.v);
prepare mixed_case_remap_insert from
    'insert into SrcMix27190.t(SrcMix27190.t.id, SrcMix27190.t.v) values (?, ?)';
set @mixed_id = 5;
set @mixed_v = 50;
execute mixed_case_remap_insert using @mixed_id, @mixed_v;
deallocate prepare mixed_case_remap_insert;
use SrcMix27190;
insert into t(SrcMix27190.t.id, SrcMix27190.t.v) values (6, 60);
select * from DstMix27190.t order by id;
/*+ {"rewrites":{"DstMix27190.t":"select * from DstMix27190.t where id=2"}} */
select * from DstMix27190.t order by id;
use mysql;
set remap_rewrites = '';
set enable_remap_hint = 0;
drop database SrcMix27190;
drop database DstMix27190;
set global lower_case_table_names = 2;
-- @session

-- @session:id=3&user=sys:root&password=111
drop database if exists SrcMix2_27190;
drop database if exists DstMix2_27190;
create database SrcMix2_27190;
create database DstMix2_27190;
create table DstMix2_27190.T(id int primary key, v int);
set enable_remap_hint = 1;
set remap_rewrites = '{"remapdb": {"SrcMix2_27190": "DstMix2_27190"}}';
insert into SrcMix2_27190.T(SrcMix2_27190.T.id, SrcMix2_27190.T.v) values (1, 10);
insert into SrcMix2_27190.T set SrcMix2_27190.T.id = 2, SrcMix2_27190.T.v = 20;
insert into SrcMix2_27190.T(SrcMix2_27190.T.id, SrcMix2_27190.T.v) select 3, 30;
replace into SrcMix2_27190.T(SrcMix2_27190.T.id, SrcMix2_27190.T.v) values (4, 40);
insert into SrcMix2_27190.T(SrcMix2_27190.T.id, SrcMix2_27190.T.v) values (1, 15)
    on duplicate key update v = values(SrcMix2_27190.T.v);
prepare mixed_case_mode_two_insert from
    'insert into SrcMix2_27190.T(SrcMix2_27190.T.id, SrcMix2_27190.T.v) values (?, ?)';
set @mixed_two_id = 5;
set @mixed_two_v = 50;
execute mixed_case_mode_two_insert using @mixed_two_id, @mixed_two_v;
deallocate prepare mixed_case_mode_two_insert;
select * from DstMix2_27190.T order by id;
/*+ {"rewrites":{"DstMix2_27190.T":"select * from DstMix2_27190.T where id=2"}} */
select * from DstMix2_27190.T order by id;
set remap_rewrites = '{"rewrites":{"DstMix2_27190.T":"select * from DstMix2_27190.T where id=3"}}';
select * from dstmix2_27190.t order by id;
set remap_rewrites = '';
set enable_remap_hint = 0;
set global lower_case_table_names = 0;
-- @session

-- @session:id=4&user=sys:root&password=111
drop database SrcMix2_27190;
drop database DstMix2_27190;
set global lower_case_table_names = 1;
-- @session

-- @session:id=5&user=sys:root&password=111
drop database if exists srcform27190;
drop database if exists dstform27190;
drop database if exists srcuse27190;
drop database if exists dstuse27190;
drop database if exists dsta27190;
drop database if exists dstb27190;
drop database if exists srcrewriteinline27190;
drop database if exists dstrewritemixed27190;
create database SrcForm27190;
create database DstForm27190;
create table DstForm27190.t(id int primary key, v int);
create database SrcUse27190;
create database DstUse27190;
create table SrcUse27190.t(id int primary key, v int);
create table DstUse27190.t(id int primary key, v int);
create database SrcRewriteInline27190;
create database DstRewriteMixed27190;
create table DstRewriteMixed27190.t(id int primary key);
insert into DstRewriteMixed27190.t values (1), (2), (3);
set enable_remap_hint = 1;
set remap_rewrites = '{"remapdb":{"SrcForm27190":"DstForm27190","SrcUse27190":"DstUse27190"}}';
insert into SrcForm27190.t(SrcForm27190.t.id, SrcForm27190.t.v) values (1, 10);
use SrcUse27190;
insert into t values (2, 20);
prepare mode_one_unqualified_insert from 'insert into t values (?, ?)';
set @mode_one_id = 3;
set @mode_one_v = 30;
execute mode_one_unqualified_insert using @mode_one_id, @mode_one_v;
deallocate prepare mode_one_unqualified_insert;
create table remapped_ddl(id int primary key);
insert into remapped_ddl values (4);
select * from t order by id;
select * from remapped_ddl;
use mysql;
set remap_rewrites = '';
select count(*) from srcuse27190.t;
select * from dstuse27190.t order by id;
select * from dstform27190.t;
select * from dstuse27190.remapped_ddl;
create database DstA27190;
create database DstB27190;
create table DstA27190.t(id int primary key);
create table DstB27190.t(id int primary key);
set remap_rewrites = '{"remapdb":{"SourceCase27190":"DstA27190"}}';
/*+ {"remapdb":{"sourcecase27190":"DstB27190"}} */ insert into SourceCase27190.t values (5);
set remap_rewrites = '';
select count(*) from dsta27190.t;
select * from dstb27190.t;
/*+ {
  "remapdb":{"SrcRewriteInline27190":"DstRewriteMixed27190"},
  "rewrites":{"DstRewriteMixed27190.t":"select * from DstRewriteMixed27190.t where id=2"}
} */ select * from SrcRewriteInline27190.t order by id;
-- @pattern
set remap_rewrites = '{"remapdb":{"SourceCase27190":"dst_a","sourcecase27190":"dst_b"}}';
-- @pattern
set remap_rewrites = '{"remapdb":{"ChainSrc27190":"MID27190","mid27190":"dst_chain27190"}}';
set enable_remap_hint = 0;
drop database srcform27190;
drop database dstform27190;
drop database srcuse27190;
drop database dstuse27190;
drop database dsta27190;
drop database dstb27190;
drop database srcrewriteinline27190;
drop database dstrewritemixed27190;
-- @session

create database rdb_dst;
create table rdb_dst.t(id int, v int);
insert into rdb_dst.t values (1,10),(2,20),(3,30);
create table rdb_dst.u(id int, w int);
insert into rdb_dst.u values (1,100),(2,200),(3,300);

set enable_remap_hint = 1;
set remap_rewrites = '{"remapdb": {"rdb_src": "rdb_dst"}}';

-- qualified reference is remapped (rdb_src does not exist, only rdb_dst)
select * from rdb_src.t order by id;

-- join across the remapped database
select t.id, u.w from rdb_src.t t join rdb_src.u u on t.id = u.id order by t.id;

-- subquery in FROM
select x.id from (select * from rdb_src.t where id >= 2) x order by x.id;

-- a CTE name is not a database reference: the CTE body is remapped, the CTE
-- reference is left alone
with c as (select * from rdb_src.t where id <= 2) select * from c order by id;

-- a non-remapped database is untouched
select * from rdb_dst.t order by id;

-- remapdb is applied before the table rewrites: the rewrite keys on the target
-- database name
set remap_rewrites = '{"remapdb": {"rdb_src": "rdb_dst"}, "rewrites": {"rdb_dst.t": "select * from rdb_dst.t where id >= 2"}}';
select * from rdb_src.t order by id;

-- an inline hint remapdb overrides the session variable for that query only
set remap_rewrites = '{"remapdb": {"rdb_src": "nosuchdb"}}';
/*+ {"remapdb": {"rdb_src": "rdb_dst"}} */ select * from rdb_src.t order by id;
set remap_rewrites = '';

-- a single inline hint carrying BOTH remapdb and a table rewrite: remapdb is
-- applied first (rdb_src.t -> rdb_dst.t), then the rewrite keyed on rdb_dst.t
/*+ {"remapdb": {"rdb_src": "rdb_dst"}, "rewrites": {"rdb_dst.t": "select * from rdb_dst.t where id = 2"}} */ select * from rdb_src.t order by id;

-- session remapdb combined with an inline rewrite in the same query
set remap_rewrites = '{"remapdb": {"rdb_src": "rdb_dst"}}';
/*+ {"rewrites": {"rdb_dst.t": "select * from rdb_dst.t where id >= 2"}} */ select * from rdb_src.t order by id;
set remap_rewrites = '';

-- remapdb works with INSERT / UPDATE / DELETE (the modified table is remapped
-- like any other reference)
drop database if exists rdb_dml;
create database rdb_dml;
create table rdb_dml.t(id int, v int);
insert into rdb_dml.t values (1,10),(2,20),(3,30);
create table rdb_dml.u(id int, v int);
create table rdb_dml.q(id int primary key, v int);
set remap_rewrites = '{"remapdb": {"rdb_src_dml": "rdb_dml"}}';
insert into rdb_src_dml.u select * from rdb_src_dml.t where id <= 2;
insert into rdb_src_dml.u(rdb_src_dml.u.id, rdb_src_dml.u.v) values (4,40);
insert into rdb_src_dml.u set rdb_src_dml.u.id = 5, rdb_src_dml.u.v = 50;
prepare remap_qualified_insert from
    'insert into rdb_src_dml.u(rdb_src_dml.u.id, rdb_src_dml.u.v) values (?, ?)';
set @remap_id = 6;
set @remap_v = 60;
execute remap_qualified_insert using @remap_id, @remap_v;
deallocate prepare remap_qualified_insert;
select * from rdb_dml.u order by id;
insert into rdb_src_dml.q(rdb_src_dml.q.id, rdb_src_dml.q.v) values (1,10);
insert into rdb_src_dml.q(rdb_src_dml.q.id, rdb_src_dml.q.v) values (1,20)
    on duplicate key update v = values(rdb_src_dml.q.v);
select * from rdb_dml.q;
update rdb_src_dml.t set v = 999 where id = 3;
select * from rdb_dml.t order by id;
delete from rdb_src_dml.t where id = 1;
select * from rdb_dml.t order by id;
set remap_rewrites = '';
drop database if exists rdb_dml;

-- USE is NOT remapped: `use rdb_src` lands in the real rdb_src. But while the
-- current database is rdb_src (a remap source), unqualified names resolve in
-- rdb_dst. Create a real rdb_src with DISTINCT data to show the difference.
create database rdb_src;
create table rdb_src.t(id int, v int);
insert into rdb_src.t values (100,1000),(200,2000);
set remap_rewrites = '{"remapdb": {"rdb_src": "rdb_dst"}}';
use rdb_src;
-- USE not remapped: current database is the real rdb_src
select database() as curdb;
-- unqualified t resolves in rdb_dst (1,2,3), NOT the real rdb_src data (100,200)
select * from t order by id;
-- qualified rdb_src.t is remapped to rdb_dst too
select * from rdb_src.t order by id;
-- turn remapdb off: now the real rdb_src is visible (100,200)
set remap_rewrites = '';
select * from t order by id;
use mysql;
drop database if exists rdb_src;

-- remapdb can remap several databases at once; each reference is resolved
-- independently, even multiple within one query
drop database if exists rdb_dst2;
create database rdb_dst2;
create table rdb_dst2.t(id int, v int);
insert into rdb_dst2.t values (7,70),(8,80);
set remap_rewrites = '{"remapdb": {"rdb_src": "rdb_dst", "rdb_src2": "rdb_dst2"}}';
select * from rdb_src.t order by id;
select * from rdb_src2.t order by id;
select a.id as a_id, b.id as b_id from rdb_src.t a join rdb_src2.t b on a.id + 6 = b.id order by a.id;
set remap_rewrites = '';
drop database if exists rdb_dst2;

-- remapdb names must be valid identifiers (rejected at SET time)
set remap_rewrites = '{"remapdb": {"a.b": "c"}}';

-- source and destination databases must be disjoint: chaining is rejected
-- (y is both a destination of x and a source), at SET time and in an inline hint
set remap_rewrites = '{"remapdb": {"x": "y", "y": "z"}}';
/*+ {"remapdb": {"x": "y", "y": "z"}} */ select 1;
-- a self-map is also rejected
set remap_rewrites = '{"remapdb": {"x": "x"}}';
-- multiple sources mapping to the same destination is allowed
set remap_rewrites = '{"remapdb": {"rdb_src": "rdb_dst", "rdb_src3": "rdb_dst"}}';
select * from rdb_src.t order by id;
set remap_rewrites = '';

-- remapdb reaches qualified references nested inside expression sub-selects:
-- WHERE IN (...), EXISTS (...), a scalar subquery in the projection, and the
-- read source of a DELETE/UPDATE subquery
create table rdb_dst.s(id int);
insert into rdb_dst.s values (2),(3);
set remap_rewrites = '{"remapdb": {"rdb_src": "rdb_dst"}}';
select * from rdb_src.t where id in (select id from rdb_src.s) order by id;
select * from rdb_src.t a where exists (select 1 from rdb_src.s b where b.id = a.id) order by id;
select id, (select count(*) from rdb_src.s) as scnt from rdb_src.t order by id;
delete from rdb_src.t where id in (select id from rdb_src.s);
select * from rdb_dst.t order by id;
set remap_rewrites = '';
insert into rdb_dst.t values (2,20),(3,30);

-- ANALYZE must resolve both qualified and implicit targets after remapping.
-- Row rewrites do not restrict the physical table statistics being published.
create database if not exists rdb_src;
drop table if exists rdb_src.t;
create table rdb_src.t(src_only int);
insert into rdb_src.t values (100),(200);
drop table if exists rdb_dst.t;
create table rdb_dst.t(id int, keep int);
insert into rdb_dst.t values (1,1),(2,1),(3,0),(3,0);
set remap_rewrites = '{"remapdb": {"rdb_src": "rdb_dst"}}';
analyze table rdb_src.t(id);
use rdb_src;
analyze table t(id);
analyze table t;
/*+ {"rewrites": {"rdb_dst.t": "select * from rdb_dst.t where keep = 1"}} */ analyze table t(id);
use mysql;
set remap_rewrites = '';
set enable_remap_hint = 0;
drop database if exists rdb_src;
drop database if exists rdb_dst;
