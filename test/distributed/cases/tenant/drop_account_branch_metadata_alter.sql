-- DROP ACCOUNT must compact account-owned ALTER generations after the
-- dropped account's historical sources are removed, while retaining them for
-- an external historical owner until that owner is dropped.

drop snapshot if exists issue_27660_alter_external_owner;
drop account if exists issue_27660_alter_no_source;
drop account if exists issue_27660_alter_external;
drop account if exists issue_27660_alter_source;

create account issue_27660_alter_source admin_name = 'admin' identified by '111';
create account issue_27660_alter_no_source admin_name = 'admin' identified by '111';

-- The source publication makes the target-owned branch edge cross-account,
-- matching the production path that creates an ALTER lineage row with the
-- branch account as creator.
-- @session:id=1&user=issue_27660_alter_source:admin&password=111
create database issue_27660_alter_source_db;
create table issue_27660_alter_source_db.base (id int primary key);
create publication issue_27660_alter_no_source_pub
  database issue_27660_alter_source_db account issue_27660_alter_no_source;
-- @session

-- The target-owned account snapshot is removed by DROP ACCOUNT. It is created
-- before ALTER so it covers the generation during the account's lifetime.
-- @session:id=2&user=issue_27660_alter_no_source:admin&password=111
create database issue_27660_alter_no_source_sub
  from issue_27660_alter_source publication issue_27660_alter_no_source_pub;
create database issue_27660_alter_no_source_branch;
data branch create table issue_27660_alter_no_source_branch.child
  from issue_27660_alter_no_source_sub.base;
create snapshot issue_27660_alter_no_source_owner for account;
alter table issue_27660_alter_no_source_branch.child
  add column altered int default 7;
-- @session

set @issue_27660_alter_no_source_id = (
  select account_id from mo_catalog.mo_account
  where account_name = 'issue_27660_alter_no_source'
);
set @issue_27660_alter_no_source_tid = (
  select table_id from mo_catalog.mo_branch_metadata
  where creator = @issue_27660_alter_no_source_id
    and level = 'alter:table'
);
set @issue_27660_alter_no_source_parent_tid = (
  select p_table_id from mo_catalog.mo_branch_metadata
  where table_id = @issue_27660_alter_no_source_tid
);
set @issue_27660_alter_no_source_parent_sname = concat(
  '__mo_branch_', cast(@issue_27660_alter_no_source_parent_tid as char)
);
set @issue_27660_alter_no_source_sname = concat(
  '__mo_branch_', cast(@issue_27660_alter_no_source_tid as char)
);
select count(*) as no_source_alter_before_drop
from mo_catalog.mo_branch_metadata
where table_id = @issue_27660_alter_no_source_tid
  and creator = @issue_27660_alter_no_source_id
  and level like 'alter%';
select count(*) as no_source_snapshots_before_drop
from mo_catalog.mo_snapshots
where kind = 'branch'
  and sname in (
    @issue_27660_alter_no_source_parent_sname,
    @issue_27660_alter_no_source_sname
  );

drop account issue_27660_alter_no_source;
select count(*) as no_source_alter_after_drop
from mo_catalog.mo_branch_metadata
where table_id = @issue_27660_alter_no_source_tid
  and level like 'alter%';
select count(*) as no_source_snapshot_after_drop
from mo_catalog.mo_snapshots
where kind = 'branch'
  and sname = @issue_27660_alter_no_source_sname;

create account issue_27660_alter_external admin_name = 'admin' identified by '111';

-- Publish the same source table to the external-owner control account.
-- @session:id=1&user=issue_27660_alter_source:admin&password=111
create publication issue_27660_alter_external_pub
  database issue_27660_alter_source_db account issue_27660_alter_external;
-- @session

-- @session:id=3&user=issue_27660_alter_external:admin&password=111
create database issue_27660_alter_external_sub
  from issue_27660_alter_source publication issue_27660_alter_external_pub;
create database issue_27660_alter_external_branch;
data branch create table issue_27660_alter_external_branch.child
  from issue_27660_alter_external_sub.base;
-- @session

-- A system-owned account snapshot is an external historical source. It must
-- be present before ALTER so it covers the generated historical edge.
create snapshot issue_27660_alter_external_owner
  for account issue_27660_alter_external;

-- @session:id=3&user=issue_27660_alter_external:admin&password=111
alter table issue_27660_alter_external_branch.child
  add column altered int default 7;
-- @session

set @issue_27660_alter_external_id = (
  select account_id from mo_catalog.mo_account
  where account_name = 'issue_27660_alter_external'
);
set @issue_27660_alter_external_tid = (
  select table_id from mo_catalog.mo_branch_metadata
  where creator = @issue_27660_alter_external_id
    and level = 'alter:table'
);
set @issue_27660_alter_external_sname = concat(
  '__mo_branch_', cast(@issue_27660_alter_external_tid as char)
);
select count(*) as external_alter_before_drop
from mo_catalog.mo_branch_metadata
where table_id = @issue_27660_alter_external_tid
  and creator = @issue_27660_alter_external_id
  and level like 'alter%';
select count(*) as external_snapshot_before_drop
from mo_catalog.mo_snapshots
where kind = 'branch'
  and sname = @issue_27660_alter_external_sname;

drop account issue_27660_alter_external;
select count(*) as external_alter_after_account_drop
from mo_catalog.mo_branch_metadata
where table_id = @issue_27660_alter_external_tid
  and level like 'alter%';
select count(*) as external_snapshot_after_account_drop
from mo_catalog.mo_snapshots
where kind = 'branch'
  and sname = @issue_27660_alter_external_sname;

drop snapshot issue_27660_alter_external_owner;
select count(*) as external_alter_after_owner_drop
from mo_catalog.mo_branch_metadata
where table_id = @issue_27660_alter_external_tid
  and level like 'alter%';
select count(*) as external_snapshot_after_owner_drop
from mo_catalog.mo_snapshots
where kind = 'branch'
  and sname = @issue_27660_alter_external_sname;

drop account if exists issue_27660_alter_no_source;
drop account if exists issue_27660_alter_external;
drop account if exists issue_27660_alter_source;
