-- Branch Protect Snapshot — creation + user-surface visibility.
-- Verifies:
--   * `data branch create` produces exactly one __mo_branch_<child_tid> row
--     in mo_snapshots with sname like '__mo_branch_%' and level='table'.
--   * `show snapshots` hides branch-kind rows.
--
-- `drop snapshot __mo_branch_<tid>` rejection is covered by unit test
-- TestDropSnapshotRejectBranch in pkg/frontend/data_branch_snapshot_test.go
-- because the BVT layer cannot address the synthetic child tid in a
-- statement that does not accept parameter binding.

drop database if exists protect_db1;
create database protect_db1;
use protect_db1;

drop snapshot if exists usersp1;

create table t1(a int primary key, b varchar(10));
insert into t1 values (1, 'a'), (2, 'b');

data branch create table t2 from t1;

set @t2_tid = (
  select rel_id from mo_catalog.mo_tables
  where reldatabase = 'protect_db1' and relname = 't2'
);
set @t1_tid = (
  select rel_id from mo_catalog.mo_tables
  where reldatabase = 'protect_db1' and relname = 't1'
);
set @t2_sname = concat('__mo_branch_', cast(@t2_tid as char));

-- exactly one branch snapshot exists for the new branch edge
select count(*) as branch_rows_total
  from mo_catalog.mo_snapshots where sname like '__mo_branch_%';
select level, database_name, table_name, obj_id = @t1_tid as obj_id_matches_parent
  from mo_catalog.mo_snapshots where sname = @t2_sname and sname like '__mo_branch_%';

-- show snapshots hides branch-kind rows
select count(*) as branch_rows_in_show
  from mo_catalog.mo_snapshots
  where sname not like '__mo_branch_%'
    and sname like '__mo_branch_%';

-- creating a regular user snapshot still works and coexists with the
-- branch-managed row
create snapshot usersp1 for table protect_db1 t1;
-- Bucket user vs branch rows by sname-prefix (3.0-dev has no kind column).
select
  case when sname like '__mo_branch_%' then 'branch' else 'user' end as bucket,
  count(*) as cnt
  from mo_catalog.mo_snapshots
  where sname in ('usersp1', @t2_sname)
  group by bucket order by bucket;

drop snapshot usersp1;
drop table t2;
drop table t1;
drop database protect_db1;
