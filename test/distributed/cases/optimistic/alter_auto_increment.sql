-- This directory is executed by the BVT harness with the real optimistic transaction configuration.
drop database if exists ai_alter_txn_optimistic;
create database ai_alter_txn_optimistic;
use ai_alter_txn_optimistic;

create table ai_commit(id bigint primary key auto_increment, v int);
begin;
alter table ai_commit auto_increment = 1000;
insert into ai_commit(v) values (1);
select id, id >= 1000 as used_pending_reset from ai_commit;
commit;
select id, v from ai_commit;

create table ai_rollback(id bigint primary key auto_increment, v int) auto_increment = 10;
insert into ai_rollback(v) values (1);
begin;
alter table ai_rollback auto_increment = 50000;
insert into ai_rollback(v) values (2);
select id >= 50000 as used_pending_reset from ai_rollback where v = 2;
rollback;
insert into ai_rollback(v) values (3);
select count(*) = 2 as rollback_removed_row,
       min(id) = 10 as kept_pre_rollback_state,
       max(id) > 10 and max(id) < 50000 as reset_did_not_leak
from ai_rollback;

-- The ALTER transaction starts before an explicit-id DML commits. Its first
-- AUTO_INCREMENT attempt must fail optimistic validation and be retried from a
-- fresh snapshot, so MAX(id) is reconciled through the SQL ALTER path instead
-- of by injecting an incrservice offset in a unit test. The focused EnginePack
-- barrier test asserts the more specific TN definition-changed retry code.
create table ai_dml_first_retry(id bigint primary key auto_increment, v int);
insert into ai_dml_first_retry values (1, 1);
begin;
select max(id) as max_before_concurrent_dml from ai_dml_first_retry;
-- @session:id=1{
use ai_alter_txn_optimistic;
insert into ai_dml_first_retry values (5000, 2);
-- @session}
alter table ai_dml_first_retry auto_increment = 1000;
commit;

-- Retry the real ALTER statement.  The requested value (1000) is below the
-- committed MAX(id), and the following omitted-column INSERT exercises the
-- real auto-increment allocator.  It must allocate MAX(id) + 1, not 1000.
alter table ai_dml_first_retry auto_increment = 1000;
insert into ai_dml_first_retry(v) values (3);
select id, v from ai_dml_first_retry order by id;

-- #25675 also changes ALTER TABLE's table-definition rewrite.  Keep an
-- expression default intact when AUTO_INCREMENT changes in the same table.
create table ai_expression_default(
    id bigint primary key auto_increment,
    label varchar(20) default (concat('x', 'y'))
);
alter table ai_expression_default auto_increment = 1000;
insert into ai_expression_default values (default, default);
select id, label from ai_expression_default;

drop database ai_alter_txn_optimistic;
