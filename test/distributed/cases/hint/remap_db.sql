-- remapdb: the remap_rewrites session variable (and inline hint) can remap one
-- database name to another. A reference to <src>.t resolves to <dst>.t, USE
-- <src> switches to <dst>, and remapdb is applied before the table rewrites.

drop database if exists rdb_src;
drop database if exists rdb_dst;
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
set remap_rewrites = '{"remapdb": {"rdb_src_dml": "rdb_dml"}}';
insert into rdb_src_dml.u select * from rdb_src_dml.t where id <= 2;
select * from rdb_dml.u order by id;
update rdb_src_dml.t set v = 999 where id = 3;
select * from rdb_dml.t order by id;
delete from rdb_src_dml.t where id = 1;
select * from rdb_dml.t order by id;
set remap_rewrites = '';
drop database if exists rdb_dml;

-- USE <src> switches the session to the target database; unqualified names then
-- resolve there
use rdb_src;
set remap_rewrites = '{"remapdb": {"rdb_src": "rdb_dst"}}';
use rdb_src;
select database() as curdb;
select * from t order by id;

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
set enable_remap_hint = 0;
drop database if exists rdb_src;
drop database if exists rdb_dst;
