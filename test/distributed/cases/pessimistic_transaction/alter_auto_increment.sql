-- This directory is executed by the BVT harness with the real pessimistic transaction configuration.
drop database if exists ai_alter_txn_pessimistic;
create database ai_alter_txn_pessimistic;
use ai_alter_txn_pessimistic;

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

-- Start the transaction before a concurrent explicit-id DML commits. In
-- pessimistic RC mode the ALTER statement refreshes its snapshot, so its real
-- MAX path must see 5000 and must not lower the allocator to the requested
-- 1000. The deterministic RC prepare-retry interleaving is covered by the
-- EnginePack barrier test, where the ALTER MAX snapshot can be held open.
create table ai_dml_first_retry(id bigint primary key auto_increment, v int);
insert into ai_dml_first_retry values (1, 1);
begin;
select max(id) as max_before_concurrent_dml from ai_dml_first_retry;
-- @session:id=1{
use ai_alter_txn_pessimistic;
insert into ai_dml_first_retry values (5000, 2);
-- @session}
alter table ai_dml_first_retry auto_increment = 1000;
commit;

-- Repeating the real ALTER is idempotent and again reconciles requested
-- AUTO_INCREMENT=1000 with MAX(id)=5000. Omitting id verifies the real
-- allocator observes the reconciled value.
alter table ai_dml_first_retry auto_increment = 1000;
insert into ai_dml_first_retry(v) values (3);
select id, v from ai_dml_first_retry order by id;

-- A non-AUTO_INCREMENT inplace ALTER also advances the transaction-local
-- table version.  If that transaction rolls back, its speculative version
-- must not remain in the CN's committed auto-increment cache and permanently
-- retry every insert planned against the restored version.
create table ai_inplace_rollback(id bigint primary key auto_increment, v int);
insert into ai_inplace_rollback(v) values (1);
begin;
alter table ai_inplace_rollback comment = 'uncommitted';
insert into ai_inplace_rollback(v) values (2);
rollback;
insert into ai_inplace_rollback(v) values (3);
select count(*) = 2 as rollback_removed_comment_row,
       sum(v) = 4 as post_rollback_insert_succeeded
from ai_inplace_rollback;

drop database ai_alter_txn_pessimistic;
