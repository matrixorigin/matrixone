-- r50 P2 (D155-C): a partial multi-account owner-removal reap is atomic.
-- Two tenant accounts hold retained branch-link rows grounded by one
-- SYS-owned cluster owner (cluster PITR first, cluster snapshot second).
-- Dropping the owner runs the link reap account by account inside the
-- drop's pessimistic transaction. A test-only fault armed for the LATER
-- account fails the drop AFTER the earlier account's deletes were
-- already issued; the rollback must restore those deletes byte-
-- identically (every asserted count survives the failed drop), and the
-- retried drop must succeed and reap both accounts.

drop account if exists efa_acc1;
drop account if exists efa_acc2;
create account efa_acc1 admin_name='admin' identified by '111';
create account efa_acc2 admin_name='admin' identified by '111';

drop pitr if exists efa_pitr;
create pitr efa_pitr for cluster range 1 'h';

-- @session:id=1&user=efa_acc1:admin&password=111
drop database if exists branch_efa;
create database branch_efa;
use branch_efa;
create table efa_p (id int primary key, v varchar(32));
insert into efa_p values (2, 'two');
data branch create table efa_b from efa_p;
update efa_b set id = 3 where id = 2;
create table efa_stash (tid bigint);
insert into efa_stash select rel_id from mo_catalog.mo_tables where relname = 'efa_b' and reldatabase = 'branch_efa';
drop table efa_b;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = (select tid from efa_stash);
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = (select tid from efa_stash);
-- @session

-- @session:id=2&user=efa_acc2:admin&password=111
drop database if exists branch_efa;
create database branch_efa;
use branch_efa;
create table efa_p (id int primary key, v varchar(32));
insert into efa_p values (2, 'two');
data branch create table efa_b from efa_p;
update efa_b set id = 3 where id = 2;
create table efa_stash (tid bigint);
insert into efa_stash select rel_id from mo_catalog.mo_tables where relname = 'efa_b' and reldatabase = 'branch_efa';
drop table efa_b;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = (select tid from efa_stash);
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = (select tid from efa_stash);
-- @session

-- While the fault framework is ENABLED the fencing certification
-- (D185(B.1)) refuses every retirement lane at CHECK A: the drop therefore
-- fails at lane admission, atomically, before any reap step runs. This is
-- the SQL-reachable failure-atomicity witness; the injected arm below can
-- never fire while it poisons the envelope, and remove+disable restore the
-- certified lane for the retry that follows.
select enable_fault_injection();
select add_fault_point('fj/cn/data_branch_reap_account_fails', ':::', 'echo', 0, 'efa_acc2');
-- @regex("pk-move link reap of account [0-9]+: internal error: data branch fencing unavailable \(stage-failed\): data branch fencing certification check-a-lane-admission failed \(fault-framework-envelope-violation\): fault framework ENABLED \(any armed point, including a never-fired one\)", true)
drop pitr efa_pitr;
select count(*) from mo_catalog.mo_pitr where pitr_name = 'efa_pitr';

-- @session:id=1&user=efa_acc1:admin&password=111
use branch_efa;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = (select tid from efa_stash);
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = (select tid from efa_stash);
-- @session

-- @session:id=2&user=efa_acc2:admin&password=111
use branch_efa;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = (select tid from efa_stash);
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = (select tid from efa_stash);
-- @session

select remove_fault_point('fj/cn/data_branch_reap_account_fails');
select disable_fault_injection();
drop pitr efa_pitr;
select count(*) from mo_catalog.mo_pitr where pitr_name = 'efa_pitr';

-- @session:id=1&user=efa_acc1:admin&password=111
use branch_efa;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = (select tid from efa_stash);
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = (select tid from efa_stash);
-- @session

-- @session:id=2&user=efa_acc2:admin&password=111
use branch_efa;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = (select tid from efa_stash);
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = (select tid from efa_stash);
-- @session

-- Snapshot twin: same shape with a SYS-owned cluster snapshot.
drop snapshot if exists efa_snap;
create snapshot efa_snap for cluster;

-- @session:id=1&user=efa_acc1:admin&password=111
use branch_efa;
create table efs_p (id int primary key, v varchar(32));
insert into efs_p values (2, 'two');
data branch create table efs_b from efs_p;
update efs_b set id = 3 where id = 2;
delete from efa_stash;
insert into efa_stash select rel_id from mo_catalog.mo_tables where relname = 'efs_b' and reldatabase = 'branch_efa';
drop table efs_b;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = (select tid from efa_stash);
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = (select tid from efa_stash);
-- @session

-- @session:id=2&user=efa_acc2:admin&password=111
use branch_efa;
create table efs_p (id int primary key, v varchar(32));
insert into efs_p values (2, 'two');
data branch create table efs_b from efs_p;
update efs_b set id = 3 where id = 2;
delete from efa_stash;
insert into efa_stash select rel_id from mo_catalog.mo_tables where relname = 'efs_b' and reldatabase = 'branch_efa';
drop table efs_b;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = (select tid from efa_stash);
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = (select tid from efa_stash);
-- @session

select enable_fault_injection();
select add_fault_point('fj/cn/data_branch_reap_account_fails', ':::', 'echo', 0, 'efa_acc2');
-- @regex("pk-move link reap of account [0-9]+: internal error: data branch fencing unavailable \(stage-failed\): data branch fencing certification check-a-lane-admission failed \(fault-framework-envelope-violation\): fault framework ENABLED \(any armed point, including a never-fired one\)", true)
drop snapshot efa_snap;
select count(*) from mo_catalog.mo_snapshots where sname = 'efa_snap';

-- @session:id=1&user=efa_acc1:admin&password=111
use branch_efa;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = (select tid from efa_stash);
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = (select tid from efa_stash);
-- @session

-- @session:id=2&user=efa_acc2:admin&password=111
use branch_efa;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = (select tid from efa_stash);
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = (select tid from efa_stash);
-- @session

select remove_fault_point('fj/cn/data_branch_reap_account_fails');
select disable_fault_injection();
drop snapshot efa_snap;
select count(*) from mo_catalog.mo_snapshots where sname = 'efa_snap';

-- @session:id=1&user=efa_acc1:admin&password=111
use branch_efa;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = (select tid from efa_stash);
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = (select tid from efa_stash);
-- @session

-- @session:id=2&user=efa_acc2:admin&password=111
use branch_efa;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = (select tid from efa_stash);
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = (select tid from efa_stash);
-- @session

-- @session:id=1&user=efa_acc1:admin&password=111
drop database branch_efa;
-- @session

-- @session:id=2&user=efa_acc2:admin&password=111
drop database branch_efa;
-- @session

drop account efa_acc1;
drop account efa_acc2;
