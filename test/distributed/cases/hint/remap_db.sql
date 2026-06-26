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

-- USE <src> switches the session to the target database; unqualified names then
-- resolve there
use rdb_src;
set remap_rewrites = '{"remapdb": {"rdb_src": "rdb_dst"}}';
use rdb_src;
select database() as curdb;
select * from t order by id;

-- remapdb names must be valid identifiers (rejected at SET time)
set remap_rewrites = '{"remapdb": {"a.b": "c"}}';

set remap_rewrites = '';
set enable_remap_hint = 0;
drop database if exists rdb_src;
drop database if exists rdb_dst;
