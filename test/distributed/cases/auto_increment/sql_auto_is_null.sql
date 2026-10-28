-- GH issue #30717: MySQL sql_auto_is_null compatibility.
-- With sql_auto_is_null=1, the whole-clause `AUTO_INCREMENT pk IS NULL`
-- predicate selects this session's last-inserted row, exactly as in MySQL's
-- own regression suite (mysql-test/suite/engines/funcs/r/ai_sql_auto_is_null.result
-- and mysql-test/r/odbc.result). Composite AND predicates, HAVING, non
-- AUTO_INCREMENT columns and LEFT-JOIN null-extended references keep plain
-- IS NULL semantics. A multi-row INSERT maps to the row holding the first
-- generated value, and an INSERT with an explicit id leaves the session's
-- LAST_INSERT_ID untouched. Statements matching the whole-clause shape never
-- enter the session plan cache, so toggling the variable takes effect
-- immediately. PREPARE freezes the rewrite decision at prepare time; current
-- MySQL resolves the substitution at item resolution, and last_insert_id()
-- still evaluates fresh on every EXECUTE.
drop database if exists sql_auto_is_null_bvt;
create database sql_auto_is_null_bvt;
use sql_auto_is_null_bvt;
set sql_auto_is_null=1;
create table t30717(id bigint auto_increment primary key, v int, w int);
create table s30717(b int);
select @@session.sql_auto_is_null;
insert into t30717(v) values (7);
select id,v from t30717 where id is null;
set sql_auto_is_null=0;
select id,v from t30717 where id is null;
set sql_auto_is_null=1;
select id,v from t30717 where id is null;
select id,v from t30717 where id is null and v=7;
select id,v from t30717 where id=last_insert_id();
insert into t30717(v) values (8),(9),(10);
select id,v from t30717 where id is null;
insert into t30717(id,v) values (100,11);
select id,v from t30717 where id is null;
insert into t30717(v) values (15);
select id,v from t30717 where id is null;
insert into t30717(v,w) values (12,null);
select v,w from t30717 where w is null order by v;
insert into s30717 values (1),(5),(200);
select s.b from s30717 s left join t30717 t on t.id=s.b where t.id is null order by s.b;
select id,v from t30717 having id is null;
-- A statement prepared while the flag is OFF never substitutes, even after ON.
set sql_auto_is_null=0;
prepare st30717off from 'select v from t30717 where id is null';
set sql_auto_is_null=1;
insert into t30717(v) values (13);
execute st30717off;
deallocate prepare st30717off;
-- A statement prepared while ON substitutes with a fresh last_insert_id() on
-- every execution; the decision stays frozen after the flag turns OFF.
prepare st30717on from 'select v from t30717 where id is null';
insert into t30717(v) values (14);
execute st30717on;
set sql_auto_is_null=0;
execute st30717on;
deallocate prepare st30717on;
-- Derived-table projections carry the lineage of their producers: a
-- projected column of the null-extended side keeps plain IS NULL semantics
-- (mo-bot review 5690740934), while the preserved side of the same derived
-- table and a plain derived projection keep the MySQL rewrite.
set sql_auto_is_null=1;
create table u30717(id bigint auto_increment primary key, b int);
insert into u30717(b) values (1),(2);
select d.ti, d.ui from (select t.id as ti, u.id as ui from t30717 t left join u30717 u on u.id=t.id) d where d.ui is null order by d.ti;
select d.ti, d.ui from (select t.id as ti, u.id as ui from t30717 t left join u30717 u on u.id=t.id) d where d.ti is null order by d.ti;
select d.id from (select id from t30717) d where d.id is null;
drop database if exists sql_auto_is_null_bvt;
