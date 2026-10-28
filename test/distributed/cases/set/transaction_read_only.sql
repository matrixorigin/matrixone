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

-- A procedure that calls another procedure is gated per statement at
-- execution as well: the nested callee's persistent write is rejected for a
-- READ ONLY caller, the durable state survives the rejection, and the same
-- call really writes under READ WRITE.
create table nw (a int primary key);
insert into nw values (1);
drop procedure if exists ro_inner_ins;
create procedure ro_inner_ins() 'begin insert into nw values (9); end';
drop procedure if exists ro_outer_call;
create procedure ro_outer_call() 'begin call ro_inner_ins(); end';
start transaction read only;
call ro_outer_call();
select count(*) from nw;
rollback;
select count(*) from nw;
call ro_outer_call();
select count(*) from nw;
delete from nw where a = 9;
drop procedure ro_inner_ins;
drop procedure ro_outer_call;
drop table nw;

-- Data branch CREATE/DELETE, MERGE, PICK and materialized DIFF output are
-- persistent catalog/data mutations and observe READ ONLY; diffing without
-- an output table only reads.
create table bsrc (a int primary key);
insert into bsrc values (1);
data branch create table txn_readonly.bchild from txn_readonly.bsrc;
start transaction read only;
data branch create table txn_readonly.bchild2 from txn_readonly.bsrc;
data branch delete table txn_readonly.bchild;
data branch merge txn_readonly.bchild into txn_readonly.bsrc;
data branch diff txn_readonly.bchild against txn_readonly.bsrc output as txn_readonly.bdiff;
data branch diff txn_readonly.bchild against txn_readonly.bsrc output count;
select count(*) from txn_readonly.bchild;
rollback;
data branch delete table txn_readonly.bchild;
drop table bsrc;

-- An unqualified UPDATE assignment resolves to the unique source owning the
-- column, so a temporary-only column writes freely while the persistent
-- owner and the genuinely ambiguous column are rejected.
create table uo_p (id int primary key, pv int);
insert into uo_p values (1, 10);
create temporary table uo_t (id int primary key, tv int);
insert into uo_t values (1, 100);
start transaction read only;
update uo_t join uo_p on uo_t.id = uo_p.id set tv = 1;
select tv from uo_t;
update uo_t join uo_p on uo_t.id = uo_p.id set pv = 9;
update uo_t join uo_p on uo_t.id = uo_p.id set id = 2;
select pv from uo_p;
commit;
select tv from uo_t;
drop table uo_t;
drop table uo_p;

-- SET GLOBAL persists system variables through trusted implementation SQL
-- and is not subject to the client-facing READ ONLY gate; the previous
-- value is saved and restored exactly.
set session transaction_read_only = 1;
set @gcm_save = @@global.group_concat_max_len;
set global group_concat_max_len = 2048;
select @@global.group_concat_max_len;
set global group_concat_max_len = @gcm_save;
select @@global.group_concat_max_len;
set session transaction_read_only = 0;

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

-- In autocommit mode admission runs before a catalog transaction exists, so
-- a column that does not qualify its owner defers its verdict until the
-- catalog transaction opens: only temp-table owners are writable, persistent
-- owners and ambiguous columns are still rejected.
create table uo2_p (id int primary key, pv int);
insert into uo2_p values (1, 10);
create temporary table uo2_t (id int primary key, tv int);
insert into uo2_t values (1, 100);
set session transaction_read_only = 1;
update uo2_t join uo2_p on uo2_t.id = uo2_p.id set tv = 1;
select tv from uo2_t;
update uo2_t join uo2_p on uo2_t.id = uo2_p.id set pv = 9;
update uo2_t join uo2_p on uo2_t.id = uo2_p.id set id = 2;
select pv from uo2_p;
select tv from uo2_t;
set session transaction_read_only = 0;
drop table uo2_t;
drop table uo2_p;

-- Account administration (role/user DDL, GRANT/REVOKE) issues its generated
-- writes through trusted internal sessions and stays usable under READ ONLY,
-- while a user procedure body is still gated.
create procedure dcl_wp() 'begin insert into t values (9); end';
set session transaction_read_only = 1;
create role dcl_r;
create user dcl_u identified by 'Test@1234';
grant select on table txn_readonly.t to dcl_r;
revoke select on table txn_readonly.t from dcl_r;
drop user dcl_u;
drop role dcl_r;
call dcl_wp();
select count(*) from t;
set session transaction_read_only = 0;
call dcl_wp();
select count(*) from t;
delete from t where a = 9;
drop procedure dcl_wp;

-- CREATE/DROP ACCOUNT issue their generated catalog writes (the mo_account
-- insert, account-init schema creation, drop-time catalog deletes) through
-- trusted internal executors and stay usable under READ ONLY, like the rest
-- of account administration; SET DEFAULT ROLE, role rewrite rules and
-- publication administration are the same class.
set session transaction_read_only = 1;
create account ro_acct admin_name = 'ro_admin' identified by 'Test@1234';
drop account ro_acct;
create role ro_r6;
create user ro_u6 identified by 'Test@1234';
grant ro_r6 to ro_u6;
set default role ro_r6 to ro_u6;
alter role ro_r6 add rule 'select a from txn_readonly.t where a > 0' on table txn_readonly.t;
alter role ro_r6 drop rule on table txn_readonly.t;
create publication ro_pub database txn_readonly account all;
drop publication ro_pub;
drop user ro_u6;
drop role ro_r6;
set session transaction_read_only = 0;

-- remapdb redirects unqualified name resolution when the current database is
-- a remap source (TxnCompilerContext.DefaultDatabase). Admission must
-- classify against that same effective database: a temporary binding keyed
-- at the source does not make the remapped persistent target writable, for
-- direct statements and for a prepared binding database alike.
create database r7_src;
create database r7_dst;
create table r7_dst.leak (a int primary key);
set enable_remap_hint = 1;
use r7_src;
create temporary table leak (a int primary key);
set remap_rewrites = '{"remapdb": {"r7_src": "r7_dst"}}';
set session transaction_read_only = 1;
insert into leak values (1);
select count(*) from r7_dst.leak;
prepare r7_pp from 'insert into leak values (1)';
execute r7_pp;
deallocate prepare r7_pp;
set session transaction_read_only = 0;
set remap_rewrites = '';
drop temporary table leak;

-- The converse: a temporary binding keyed at the remap destination is what
-- the unqualified target binds to under remap, so the write stays
-- temporary-only and must be admitted.
create temporary table r7_dst.okt (a int primary key);
set remap_rewrites = '{"remapdb": {"r7_src": "r7_dst"}}';
set session transaction_read_only = 1;
insert into okt values (1);
select count(*) from okt;
set session transaction_read_only = 0;
set remap_rewrites = '';
drop temporary table r7_dst.okt;
set enable_remap_hint = 0;
use txn_readonly;
drop table r7_dst.leak;
drop database r7_src;
drop database r7_dst;

drop table t;
drop database txn_readonly;
