-- Error handling for remap_rewrites (session variable) and the /*+ ... */ hint:
-- invalid configs are rejected at SET time or parse time, runtime failures
-- surface as normal errors, and a rejected/invalid config never affects later
-- queries.

drop database if exists rerr;
create database rerr;
use rerr;
create table t(id int, v int);
insert into t values (1,10),(2,20),(3,30);
create table u(id int, v int);
insert into u values (1,100),(2,200),(3,300);

set enable_remap_hint = 1;

-- =========================================================================
-- SET-time validation of the remap_rewrites session variable
-- =========================================================================
-- malformed JSON
set remap_rewrites = '{not json}';
-- a JSON array / scalar is not a valid payload
set remap_rewrites = '[1,2,3]';
set remap_rewrites = '123';
-- a non-SELECT rewrite rule is rejected
set remap_rewrites = '{"rerr.t": "delete from t"}';
set remap_rewrites = '{"rerr.t": "update t set v = 0"}';
-- a syntactically invalid rewrite rule is rejected
set remap_rewrites = '{"rerr.t": "select from"}';
-- an empty table key is rejected
set remap_rewrites = '{"   ": "select 1"}';
-- remapdb names must be valid identifiers
set remap_rewrites = '{"remapdb": {"a.b": "c"}}';
set remap_rewrites = '{"remapdb": {"x": "y z"}}';
-- remapdb source/destination sets must be disjoint
set remap_rewrites = '{"remapdb": {"x": "y", "y": "z"}}';
set remap_rewrites = '{"remapdb": {"x": "x"}}';

-- after all the rejected SETs, the variable is still empty and queries work
select @@remap_rewrites as cur;
select * from t order by id;

-- =========================================================================
-- Parse-time validation of an inline hint (no session variable set)
-- =========================================================================
-- malformed JSON inline hint
/*+ {not json} */ select * from t order by id;
-- non-SELECT rewrite rule in an inline hint
/*+ {"rewrites": {"rerr.t": "delete from t"}} */ select * from t order by id;
-- rewrite key must be db-qualified
/*+ {"rewrites": {"t": "select * from t"}} */ select * from t order by id;
-- empty database/table in the rewrite key
/*+ {"rewrites": {"rerr.": "select 1"}} */ select * from t order by id;
-- invalid remapdb identifier in an inline hint
/*+ {"remapdb": {"a.b": "c"}} */ select * from t order by id;
-- remapdb chaining in an inline hint
/*+ {"remapdb": {"x": "y", "y": "z"}} */ select * from t order by id;

-- an invalid inline hint only fails its own query; the next query is fine
select * from t order by id;

-- a malformed inline hint while a session variable is active is reported as an
-- inline-hint problem (not as a remap_rewrites problem)
set remap_rewrites = '{"remapdb": {"q": "r"}}';
/*+ {not json} */ select * from t order by id;
set remap_rewrites = '';

-- =========================================================================
-- Interaction: session remapdb merged with inline remapdb forms a chain
-- (rdb_a -> rdb_b and rdb_b -> rdb_c) and is rejected
-- =========================================================================
set remap_rewrites = '{"remapdb": {"rdb_a": "rdb_b"}}';
/*+ {"remapdb": {"rdb_b": "rdb_c"}} */ select 1;
set remap_rewrites = '';

-- =========================================================================
-- Runtime failures: the config is valid but resolution fails
-- =========================================================================
-- a rewrite rule that reads a non-existent table
set remap_rewrites = '{"rerr.t": "select * from no_such_table"}';
select * from t order by id;
set remap_rewrites = '';
-- remapdb to a non-existent database
set remap_rewrites = '{"remapdb": {"ghost": "no_such_db"}}';
select * from ghost.t order by id;
set remap_rewrites = '';

-- =========================================================================
-- Gating: with enable_remap_hint off the config is ignored, no error
-- =========================================================================
set remap_rewrites = '{"rewrites": {"rerr.t": "select * from t where id <= 1"}}';
set enable_remap_hint = 0;
select * from t order by id;
-- and an inline hint is also ignored (treated as a plain comment)
/*+ {"rewrites": {"rerr.t": "select * from t where id <= 1"}} */ select * from t order by id;
set enable_remap_hint = 1;
select * from t order by id;

set remap_rewrites = '';
set enable_remap_hint = 0;
drop database if exists rerr;
