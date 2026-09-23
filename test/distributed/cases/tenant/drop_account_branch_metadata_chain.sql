-- DROP ACCOUNT must retain a deleted branch edge while a live child account
-- still depends on that edge, then reclaim the whole chain after the child
-- account is dropped.

drop account if exists issue_27660_chain_c;
drop account if exists issue_27660_chain_a;
drop account if exists issue_27660_chain_b;

create account issue_27660_chain_b admin_name = 'admin' identified by '111';
create account issue_27660_chain_a admin_name = 'admin' identified by '111';
create account issue_27660_chain_c admin_name = 'admin' identified by '111';

-- @session:id=1&user=issue_27660_chain_b:admin&password=111
create database issue_27660_chain_b_db;
create table issue_27660_chain_b_db.base (id int primary key);
insert into issue_27660_chain_b_db.base values (1);
create publication issue_27660_chain_b_pub
  database issue_27660_chain_b_db account issue_27660_chain_a;
-- @session

-- A subscribes to B, then creates a branch from that source into A.
-- The subscription keeps the source account's table identity visible to the
-- Data Branch operation without bypassing the normal privilege path.
-- @session:id=2&user=issue_27660_chain_a:admin&password=111
create database issue_27660_chain_a_sub
  from issue_27660_chain_b publication issue_27660_chain_b_pub;
create database issue_27660_chain_a_branch;
data branch create table issue_27660_chain_a_branch.child
  from issue_27660_chain_a_sub.base;
create publication issue_27660_chain_a_pub
  database issue_27660_chain_a_branch table child account issue_27660_chain_c;
-- @session

-- C repeats the operation from A's published branch table, producing the
-- B -> A -> C lineage that account cleanup must preserve and later reclaim.
-- @session:id=3&user=issue_27660_chain_c:admin&password=111
create database issue_27660_chain_c_sub
  from issue_27660_chain_a publication issue_27660_chain_a_pub;
create database issue_27660_chain_c_branch;
data branch create table issue_27660_chain_c_branch.grandchild
  from issue_27660_chain_c_sub.child;
-- @session

set @issue_27660_chain_b_id = (
  select account_id from mo_catalog.mo_account
  where account_name = 'issue_27660_chain_b'
);
set @issue_27660_chain_a_id = (
  select account_id from mo_catalog.mo_account
  where account_name = 'issue_27660_chain_a'
);
set @issue_27660_chain_c_id = (
  select account_id from mo_catalog.mo_account
  where account_name = 'issue_27660_chain_c'
);
set @issue_27660_chain_b_tid = (
  select rel_id from mo_catalog.mo_tables
  where account_id = @issue_27660_chain_b_id
    and reldatabase = 'issue_27660_chain_b_db'
    and relname = 'base'
);
set @issue_27660_chain_a_tid = (
  select rel_id from mo_catalog.mo_tables
  where account_id = @issue_27660_chain_a_id
    and reldatabase = 'issue_27660_chain_a_branch'
    and relname = 'child'
);
set @issue_27660_chain_c_tid = (
  select rel_id from mo_catalog.mo_tables
  where account_id = @issue_27660_chain_c_id
    and reldatabase = 'issue_27660_chain_c_branch'
    and relname = 'grandchild'
);
set @issue_27660_chain_a_sname = concat('__mo_branch_', cast(@issue_27660_chain_a_tid as char));
set @issue_27660_chain_c_sname = concat('__mo_branch_', cast(@issue_27660_chain_c_tid as char));

select count(*) as chain_edges_before_drop
from mo_catalog.mo_branch_metadata
where (table_id = @issue_27660_chain_a_tid
       and p_table_id = @issue_27660_chain_b_tid
       and creator = @issue_27660_chain_a_id)
   or (table_id = @issue_27660_chain_c_tid
       and p_table_id = @issue_27660_chain_a_tid
       and creator = @issue_27660_chain_c_id);
select count(*) as chain_metadata_before_drop
from mo_catalog.mo_branch_metadata
where table_id in (@issue_27660_chain_a_tid, @issue_27660_chain_c_tid);
select count(*) as chain_snapshots_before_drop
from mo_catalog.mo_snapshots
where kind = 'branch'
  and sname in (@issue_27660_chain_a_sname, @issue_27660_chain_c_sname);
select count(*) as account_a_branch_quota_before_drop
from mo_catalog.mo_feature_limit
where account_id = @issue_27660_chain_a_id
  and feature_code = 'BRANCH';
select count(*) as account_c_branch_quota_before_drop
from mo_catalog.mo_feature_limit
where account_id = @issue_27660_chain_c_id
  and feature_code = 'BRANCH';

-- A's edge is deleted, but it must remain in the DAG while C is live.
drop account issue_27660_chain_a;
select count(*) as retained_deleted_a_edge
from mo_catalog.mo_branch_metadata
where table_id = @issue_27660_chain_a_tid
  and creator = @issue_27660_chain_a_id
  and table_deleted = true;
select count(*) as live_c_edge_after_a_drop
from mo_catalog.mo_branch_metadata
where table_id = @issue_27660_chain_c_tid
  and creator = @issue_27660_chain_c_id
  and table_deleted = false;
select count(*) as retained_chain_snapshots_after_a_drop
from mo_catalog.mo_snapshots
where kind = 'branch'
  and sname in (@issue_27660_chain_a_sname, @issue_27660_chain_c_sname);
select count(*) as account_a_branch_quota_after_drop
from mo_catalog.mo_feature_limit
where account_id = @issue_27660_chain_a_id
  and feature_code = 'BRANCH';

-- Dropping the live child completes the descendant subtree, so both the
-- retained ancestor edge and child edge can be reclaimed together.
drop account issue_27660_chain_c;
select count(*) as chain_metadata_after_child_drop
from mo_catalog.mo_branch_metadata
where table_id in (@issue_27660_chain_a_tid, @issue_27660_chain_c_tid);
select count(*) as chain_snapshots_after_child_drop
from mo_catalog.mo_snapshots
where kind = 'branch'
  and sname in (@issue_27660_chain_a_sname, @issue_27660_chain_c_sname);
select count(*) as chain_creator_rows_after_child_drop
from mo_catalog.mo_branch_metadata
where creator in (@issue_27660_chain_a_id, @issue_27660_chain_c_id);
select count(*) as account_c_branch_quota_after_drop
from mo_catalog.mo_feature_limit
where account_id = @issue_27660_chain_c_id
  and feature_code = 'BRANCH';

drop account if exists issue_27660_chain_b;
drop account if exists issue_27660_chain_c;
drop account if exists issue_27660_chain_a;
