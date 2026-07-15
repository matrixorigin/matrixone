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

drop database ai_alter_txn_optimistic;
