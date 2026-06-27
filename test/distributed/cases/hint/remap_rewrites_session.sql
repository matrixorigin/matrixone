-- remap_rewrites session variable: rewrite rules set via a session variable
-- apply to all following queries (gated by enable_remap_hint).

drop database if exists remap_sess;
create database remap_sess;
use remap_sess;

create table t1 (id int, name varchar(20), age int);
insert into t1 values (1, 'Alice', 20), (2, 'Bob', 30), (3, 'Charlie', 40);
create table t2 (id int, city varchar(20));
insert into t2 values (1, 'Beijing'), (2, 'Shanghai'), (3, 'Guangzhou');

-- baseline: without the hint enabled, no rewrite happens
select * from t1 order by id;

-- enable the feature and set a session-level rewrite for t1
set enable_remap_hint = 1;
set remap_rewrites = '{"remap_sess.t1": "select * from t1 where age > 25"}';

-- every following select from t1 is now rewritten
select * from t1 order by id;
select id, name from t1 order by id;
select count(*) as c from t1;

-- a query that does not touch t1 is unaffected
select * from t2 order by id;

-- the bare-map and the wrapped {"rewrites": {...}} forms are both accepted
set remap_rewrites = '{"rewrites": {"remap_sess.t1": "select * from t1 where id = 2"}}';
select * from t1 order by id;

-- multiple tables in one session variable
set remap_rewrites = '{"remap_sess.t1": "select * from t1 where id >= 2", "remap_sess.t2": "select * from t2 where city != \'Shanghai\'"}';
select t1.name, t2.city from t1 join t2 on t1.id = t2.id order by t1.name;

-- clearing the variable removes the session rewrites
set remap_rewrites = '';
select * from t1 order by id;

-- an inline /*+ ... */ hint is LAYERED on top of the session variable (a view
-- over the session-rewritten table), and only for the single query that carries
-- it. session keeps age > 25 (Bob 30, Charlie 40); inline then keeps id <= 2 on
-- top of that -> only Bob. The next query is back to the session layer.
set remap_rewrites = '{"remap_sess.t1": "select * from t1 where age > 25"}';
select * from t1 order by id;
/*+ {"rewrites": {"remap_sess.t1": "select * from t1 where id <= 2"}} */ select * from t1 order by id;
select * from t1 order by id;

-- an inline hint for a different table layers on t2 while t1 keeps its session
-- rewrite: t1 age>25 (Bob,Charlie) joined with t2 id=3 (Guangzhou) -> Charlie
/*+ {"rewrites": {"remap_sess.t2": "select * from t2 where id = 3"}} */ select t1.id, t2.city from t1 join t2 on t1.id = t2.id order by t1.id;
set remap_rewrites = '';

-- rules only apply while enable_remap_hint is on
set remap_rewrites = '{"remap_sess.t1": "select * from t1 where age > 25"}';
set enable_remap_hint = 0;
select * from t1 order by id;
set enable_remap_hint = 1;

-- an invalid (non-SELECT) rewrite is rejected at SET time
set remap_rewrites = '{"remap_sess.t1": "delete from t1"}';

-- malformed JSON is rejected at SET time
set remap_rewrites = '{not json}';

-- a rejected SET leaves the previous value in effect (age > 25), and the
-- session is never bricked: subsequent queries still work
select * from t1 order by id;

-- an inline array-form (chain) rewrite still composes on top of a session rule:
-- session layer (age > 25) -> inline layer 1 (age < 40) -> inline layer 2 (id >= 1).
-- The array syntax (which the parser accepts) must not be rejected when merged.
set remap_rewrites = '{"remap_sess.t1": "select * from t1 where age > 25"}';
/*+ {"rewrites": {"remap_sess.t1": ["select * from t1 where age < 40", "select * from t1 where id >= 1"]}} */ select * from t1 order by id;
set remap_rewrites = '';

set remap_rewrites = '';
set enable_remap_hint = 0;
drop database remap_sess;
