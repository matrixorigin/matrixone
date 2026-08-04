-- Regression for issue #26120.
-- Snapshot branches must bind ancestry to the historical relation generation.
drop snapshot if exists bvt_issue_26120_sp;
drop snapshot if exists bvt_issue_26120_control_sp;
drop snapshot if exists bvt_issue_26120_db_sp;
drop database if exists bvt_issue_26120_db_branch;
drop database if exists bvt_issue_26120_db_src;
drop database if exists bvt_issue_26120;
create database bvt_issue_26120;
use bvt_issue_26120;

-- Ordinary snapshot control.
create table control_parent(id int primary key, val varchar(20));
insert into control_parent values (10, 'control-row');
create snapshot bvt_issue_26120_control_sp
for table bvt_issue_26120 control_parent;
data branch create table control_child
from control_parent{snapshot='bvt_issue_26120_control_sp'};

-- Table-level historical generation.
create table parent_t(id int primary key, val varchar(20));
insert into parent_t values (1, 'snapshot-row');
create snapshot bvt_issue_26120_sp for table bvt_issue_26120 parent_t;
drop table parent_t;
create table parent_t(id int primary key, val varchar(20));
insert into parent_t values (2, 'current-row');
data branch create table child_t
from parent_t{snapshot='bvt_issue_26120_sp'};

-- Database-level historical generation.
create database bvt_issue_26120_db_src;
create table bvt_issue_26120_db_src.parent_t(
    id int primary key,
    val varchar(20)
);
insert into bvt_issue_26120_db_src.parent_t
values (1, 'db-snapshot-row');
create snapshot bvt_issue_26120_db_sp
for database bvt_issue_26120_db_src;
drop table bvt_issue_26120_db_src.parent_t;
create table bvt_issue_26120_db_src.parent_t(
    id int primary key,
    val varchar(20)
);
insert into bvt_issue_26120_db_src.parent_t
values (2, 'db-current-row');
data branch create database bvt_issue_26120_db_branch
from bvt_issue_26120_db_src{snapshot='bvt_issue_26120_db_sp'};

select id, val from control_child;
select id, val from child_t;
select count(*) as historical_parent_matches
from mo_catalog.mo_branch_metadata b
join mo_catalog.mo_tables child on child.rel_id = b.table_id
where child.account_id = 0
  and child.reldatabase = 'bvt_issue_26120'
  and child.relname = 'child_t'
  and b.p_table_id = (
      select old_parent.rel_id
      from mo_catalog.mo_tables{snapshot='bvt_issue_26120_sp'} old_parent
      where old_parent.account_id = 0
        and old_parent.reldatabase = 'bvt_issue_26120'
        and old_parent.relname = 'parent_t'
  )
  and b.p_table_id <> (
      select new_parent.rel_id
      from mo_catalog.mo_tables new_parent
      where new_parent.account_id = 0
        and new_parent.reldatabase = 'bvt_issue_26120'
        and new_parent.relname = 'parent_t'
  );
select count(*) as table_protection_snapshot_matches
from mo_catalog.mo_tables child
join mo_catalog.mo_snapshots protection
  on protection.sname = concat('__mo_branch_', cast(child.rel_id as varchar))
where child.account_id = 0
  and child.reldatabase = 'bvt_issue_26120'
  and child.relname = 'child_t'
  and protection.obj_id = (
      select old_parent.rel_id
      from mo_catalog.mo_tables{snapshot='bvt_issue_26120_sp'} old_parent
      where old_parent.account_id = 0
        and old_parent.reldatabase = 'bvt_issue_26120'
        and old_parent.relname = 'parent_t'
  );
data branch diff child_t against parent_t output summary;

select id, val from bvt_issue_26120_db_branch.parent_t;
select id, val from bvt_issue_26120_db_src.parent_t;
select count(*) as database_historical_parent_matches
from mo_catalog.mo_branch_metadata b
join mo_catalog.mo_tables child on child.rel_id = b.table_id
where child.account_id = 0
  and child.reldatabase = 'bvt_issue_26120_db_branch'
  and child.relname = 'parent_t'
  and b.p_table_id = (
      select old_parent.rel_id
      from mo_catalog.mo_tables{snapshot='bvt_issue_26120_db_sp'} old_parent
      where old_parent.account_id = 0
        and old_parent.reldatabase = 'bvt_issue_26120_db_src'
        and old_parent.relname = 'parent_t'
  )
  and b.p_table_id <> (
      select new_parent.rel_id
      from mo_catalog.mo_tables new_parent
      where new_parent.account_id = 0
        and new_parent.reldatabase = 'bvt_issue_26120_db_src'
        and new_parent.relname = 'parent_t'
  );
select count(*) as database_protection_snapshot_matches
from mo_catalog.mo_tables child
join mo_catalog.mo_snapshots protection
  on protection.sname = concat('__mo_branch_', cast(child.rel_id as varchar))
where child.account_id = 0
  and child.reldatabase = 'bvt_issue_26120_db_branch'
  and child.relname = 'parent_t'
  and protection.obj_id = (
      select old_parent.rel_id
      from mo_catalog.mo_tables{snapshot='bvt_issue_26120_db_sp'} old_parent
      where old_parent.account_id = 0
        and old_parent.reldatabase = 'bvt_issue_26120_db_src'
        and old_parent.relname = 'parent_t'
  );
data branch diff bvt_issue_26120_db_branch.parent_t
against bvt_issue_26120_db_src.parent_t output summary;

drop database bvt_issue_26120_db_branch;
drop database bvt_issue_26120_db_src;
drop database bvt_issue_26120;
drop snapshot bvt_issue_26120_sp;
drop snapshot bvt_issue_26120_control_sp;
drop snapshot bvt_issue_26120_db_sp;
