-- READ ONLY transaction enforcement (MySQL ERROR 1792 / SQLSTATE 25006):
-- writes are rejected, reads keep working, the transaction stays usable,
-- START TRANSACTION READ WRITE overrides the session default, and
-- TEMPORARY tables stay writable because they are session-local.
drop database if exists txn_readonly;
create database txn_readonly;
use txn_readonly;
create table t (a int primary key);
insert into t values (1);

-- Current transaction READ ONLY: every kind of write is rejected with
-- ERROR 1792, reads pass, and the transaction survives every rejection.
start transaction read only;
select @@session.transaction_read_only, @@session.tx_read_only;
select * from t;
insert into t values (2);
update t set a = a + 1;
delete from t;
replace into t values (3);
truncate table t;
create table ddl_t (a int);
create index idx_a on t (a);
drop table t;
alter table t add column b int;
rename table t to t2;
select * from t;
commit;
select count(*) from t;

-- A plain BEGIN inherits the session default.
set session transaction read only;
begin;
insert into t values (2);
select count(*) from t;
rollback;

-- START TRANSACTION READ WRITE overrides the session default.
start transaction read write;
insert into t values (2);
commit;
select count(*) from t;

-- The session default also gates autocommit statements.
insert into t values (3);
select count(*) from t;
set session transaction read write;
select @@session.transaction_read_only, @@session.tx_read_only;

-- TEMPORARY tables are session-local and remain writable inside a
-- READ ONLY transaction, like MySQL.
start transaction read only;
create temporary table tmp (a int);
insert into tmp values (7);
select * from tmp;
update tmp set a = a + 1;
select * from tmp;
drop table tmp;
commit;
select count(*) from t;

-- Prepared statements are gated at EXECUTE time.
start transaction read only;
prepare s from 'insert into t values (9)';
execute s;
deallocate prepare s;
prepare s2 from 'select count(*) from t';
execute s2;
deallocate prepare s2;
commit;

-- EXPLAIN ANALYZE executes the wrapped statement, so it is gated by what
-- it wraps. (Ordinary EXPLAIN plans only and stays allowed.)
start transaction read only;
explain analyze insert into t values (9);
rollback;
select count(*) from t;

-- Multi-table DELETE write targets resolve through the FROM aliases: a
-- temporary table sharing the target alias does not shadow the persistent
-- victim, while a delete truly aimed at the temporary table stays allowed.
create temporary table tmp (a int);
insert into tmp values (7);
start transaction read only;
delete tmp from t as tmp;
delete y from t as y;
delete tmp from tmp as tmp;
commit;
select count(*) from t;
drop table tmp;

-- A procedure whose body writes runs on its own transaction; the write is
-- rejected at execution with the caller's READ ONLY mode (the procedure's
-- own transaction rolls back), read-only procedures stay callable.
drop procedure if exists txn_read_only_wp;
create procedure txn_read_only_wp() 'begin insert into t values (9); end';
drop procedure if exists txn_read_only_rp;
create procedure txn_read_only_rp() 'begin select count(*) from t; end';
set session transaction read only;
call txn_read_only_wp();
select count(*) from t;
call txn_read_only_rp();
rollback;
select count(*) from t;
set session transaction read write;

-- EXPLAIN PHYPLAN shares the executing branch with EXPLAIN ANALYZE
-- (both run the wrapped plan directly), so it is gated by its wrapped
-- statement exactly the same way.
start transaction read only;
explain phyplan insert into t values (9);
rollback;
select count(*) from t;

-- A schema-qualified multi-table DELETE target still resolves through the
-- FROM alias first: the planner matches aliases by object name even when
-- the target carries a qualifier. A temporary-table spelling of the target
-- must not shadow the persistent victim, and the truly temporary qualified
-- delete stays allowed. Under READ WRITE the same dangerous form deletes
-- from the aliased persistent table, proving admission and execution
-- resolve the same victim.
create table dq (a int primary key);
insert into dq values (1), (2);
create temporary table dqt (a int);
start transaction read only;
delete txn_readonly.dqt from dq as dqt;
select count(*) from dq;
delete txn_readonly.dqt from txn_readonly.dqt;
commit;
select count(*) from dq;
delete txn_readonly.dqt from dq as dqt;
select count(*) from dq;
drop table dq;
drop table dqt;

-- An UPDATE assigning only temporary-table columns stays allowed under
-- READ ONLY even when a persistent table is read through the join (the
-- planner derives written tables from the SET assignment targets, and this
-- gate does the same); assigning a persistent column is rejected.
create table up (a int primary key, b int);
insert into up values (1, 10), (2, 20);
create temporary table ut (a int primary key, b int);
insert into ut values (1, 100), (2, 200);
start transaction read only;
update ut join up on ut.a = up.a set ut.b = up.b;
select b from ut order by a;
update ut join up on ut.a = up.a set up.b = 999;
select b from up order by a;
commit;
drop table ut;
drop table up;

-- Procedure statements are gated at execution with the caller's access mode
-- and the session temporary bindings live at that moment: a body that drops
-- a binding and then writes the persistent table behind it is rejected where
-- it happens, temporary writes inside procedures stay allowed, and the READ
-- WRITE replay of the same body proves the write itself still works.
create table pe (a int primary key);
insert into pe values (1);
create temporary table pt (a int primary key);
drop procedure if exists txn_read_only_tp_proc;
create procedure txn_read_only_tp_proc() 'begin insert into pt values (1); insert into pt values (2); end';
drop procedure if exists txn_read_only_xp;
create procedure txn_read_only_xp() 'begin drop table pt; insert into pe values (9); end';
drop procedure if exists txn_read_only_tr;
create procedure txn_read_only_tr() 'begin truncate table pe; end';
start transaction read only;
call txn_read_only_tp_proc();
select count(*) from pt;
call txn_read_only_xp();
select count(*) from pe;
call txn_read_only_tr();
select count(*) from pe;
commit;
drop procedure if exists txn_read_only_xp2;
create procedure txn_read_only_xp2() 'begin drop table pt2; insert into pe values (9); end';
create temporary table pt2 (a int primary key);
call txn_read_only_xp2();
select count(*) from pe;
call txn_read_only_tr();
select count(*) from pe;
drop procedure txn_read_only_tp_proc;
drop procedure txn_read_only_xp;
drop procedure txn_read_only_tr;
drop procedure txn_read_only_xp2;
drop table if exists pt;
drop table if exists pt2;
drop table pe;

-- Dynamic SQL inside a procedure cannot be classified statically: it would
-- execute on an exempt background session inside the procedure's own
-- committed transaction, so PREPARE/EXECUTE in the body fails closed under
-- READ ONLY — even for an underlying read.
drop procedure if exists txn_read_only_dp;
create procedure txn_read_only_dp() 'begin prepare ds from ''insert into t values (9)''; execute ds; end';
drop procedure if exists txn_read_only_dsel;
create procedure txn_read_only_dsel() 'begin prepare ss from ''select count(*) from t''; execute ss; end';
set session transaction read only;
call txn_read_only_dp();
call txn_read_only_dsel();
select count(*) from t;
set session transaction read write;
call txn_read_only_dp();
select count(*) from t;
call txn_read_only_dsel();
delete from t where a = 9;
drop procedure txn_read_only_dp;
drop procedure txn_read_only_dsel;

-- Direct variable assignment behaves like SET SESSION TRANSACTION.
set session transaction_read_only = 1;
delete from t;
select count(*) from t;
set session tx_read_only = 0;
insert into t values (3);
select count(*) from t;

drop table t;
drop database txn_readonly;
