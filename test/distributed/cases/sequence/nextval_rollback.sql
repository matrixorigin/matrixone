-- Defect D (#30896): sequence advancement is not transactional. Values
-- issued inside a transaction that later rolls back are consumed; a later
-- allocation must not re-issue them, in the same session or a fresh one.
drop database if exists seq_rollback_db;
create database seq_rollback_db;
use seq_rollback_db;
create sequence sr as bigint increment by 1 start with 1 no cycle;
create table t (id bigint primary key default nextval('sr'), v int);

-- Default-value allocation inside a rolled-back explicit transaction.
begin;
insert into t(v) values (10);
rollback;
insert into t(v) values (20);
select id, v from t;
select last_seq_num, is_called from sr;

-- The gap stays after a committed change's own rollback drama.
begin;
insert into t(v) values (30);
commit;
select id, v from t order by id;
select last_seq_num, is_called from sr;

-- Direct nextval inside an explicit rolled-back transaction is consumed.
begin;
select nextval('sr');
rollback;
select nextval('sr');
select last_seq_num, is_called from sr;

-- A rolled-back statement drive aborts nothing already emitted.
begin;
insert into t(v) values (40);
rollback;
insert into t(id, v) values (100, 50);
insert into t(v) values (60);
select id, v from t order by id;
select last_seq_num, is_called from sr;

-- Descending sequences obey the same contract.
create sequence sd as bigint increment by -1 start with 10 minvalue 1 maxvalue 10 no cycle;
create table td (id bigint primary key default nextval('sd'), v int);
begin;
insert into td(v) values (1);
rollback;
insert into td(v) values (2);
select id, v from td;
select last_seq_num, is_called from sd;

-- Post-merge rollback (#30359): a merge inside an explicit transaction that
-- later rolls back keeps the destination sequence's consumed advance — a
-- documented gap, never a reuse. The merged rows are removed with the
-- transaction; the next default insert continues past the merged keys.
drop database if exists nvr_merge_src;
drop database if exists nvr_merge_br;
create database nvr_merge_src;
use nvr_merge_src;
create sequence smr as bigint increment by 1 start with 1 no cycle;
create table tmr (id bigint primary key default nextval('smr'), v int);
insert into tmr(v) values (10);
data branch create database nvr_merge_br from nvr_merge_src;
use nvr_merge_br;
insert into tmr(v) values (20);
use nvr_merge_src;
begin;
data branch merge nvr_merge_br.tmr into nvr_merge_src.tmr;
rollback;
select id, v from tmr order by id;
select last_seq_num, is_called from smr;
insert into tmr(v) values (30);
select id, v from tmr order by id;
select last_seq_num, is_called from smr;
drop database nvr_merge_br;
drop database nvr_merge_src;

drop database seq_rollback_db;
