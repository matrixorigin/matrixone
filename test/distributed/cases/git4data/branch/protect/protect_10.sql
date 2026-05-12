-- Branch Protect Snapshot — cross-account via `data branch create ... to account`.
-- Verifies that when the parent and child live in different accounts:
--   * The branch snapshot row is anchored on the PARENT's account
--     (so GC on the parent's account sees it).
--   * Reclaim triggered from the child's account reaches across to
--     delete the snapshot under sys.

drop account if exists acc_protect_child;
drop database if exists protect_db10_src;
drop snapshot if exists sp_protect_db10;
create account acc_protect_child admin_name "root1" identified by "111";
create database protect_db10_src;
use protect_db10_src;
create table t1(a int primary key);
insert into t1 values (1);
create snapshot sp_protect_db10 for table protect_db10_src t1;

-- @session:id=2&user=acc_protect_child:root1&password=111
-- Child account side: create the destination database first.
create database protect_db10_dst;
-- @session

-- sys side: cross-account branch create lands t2 in acc_protect_child.protect_db10_dst.
data branch create table protect_db10_dst.t2 from protect_db10_src.t1{snapshot="sp_protect_db10"} to account acc_protect_child;

-- Capture identifiers for verification.
set @child_acc_id = (select account_id from mo_catalog.mo_account where account_name = 'acc_protect_child');
set @t2_tid = (select rel_id from mo_catalog.mo_tables
               where account_id = @child_acc_id and reldatabase='protect_db10_dst' and relname='t2');
set @t1_tid = (select rel_id from mo_catalog.mo_tables
               where account_id = 0 and reldatabase='protect_db10_src' and relname='t1');
set @t2_sname = concat('__mo_branch_', cast(@t2_tid as char));

-- Branch snapshot row is anchored on the PARENT's account (sys).
-- obj_id points at t1 in sys; account_name='sys'; parent db/table match.
select level, database_name, table_name, account_name,
       obj_id = @t1_tid as obj_id_matches_parent
  from mo_catalog.mo_snapshots
  where sname = @t2_sname and sname like '__mo_branch_%';

-- Drop the child table under the child account.
-- @session:id=2&user=acc_protect_child:root1&password=111
drop table protect_db10_dst.t2;
-- @session

-- Reclaim ran synchronously under sys when ddl.go flipped
-- mo_branch_metadata.table_deleted for t2.
select count(*) as branch_row_after_child_drop
  from mo_catalog.mo_snapshots
  where sname = @t2_sname and sname like '__mo_branch_%';
select table_deleted from mo_catalog.mo_branch_metadata where table_id = @t2_tid;

-- @session:id=2&user=acc_protect_child:root1&password=111
drop database protect_db10_dst;
-- @session

drop snapshot sp_protect_db10;
drop database protect_db10_src;
drop account if exists acc_protect_child;
