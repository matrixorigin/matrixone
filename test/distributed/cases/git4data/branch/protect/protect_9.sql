-- Branch Protect Snapshot — plain `drop database` cascade reclaim.
-- Verifies that dropping the database holding a branch child triggers
-- the shared reclaim helper for every contained branch table via
-- ddl.go's drop-table loop.

drop database if exists protect_db9_parent;
drop database if exists protect_db9_branch;

create database protect_db9_parent;
use protect_db9_parent;
create table t1(a int primary key);
insert into t1 values (1);

-- Create a branch child database holding two branch tables.
data branch create database protect_db9_branch from protect_db9_parent;

-- Add a third branch edge inside the same child db for variety.
use protect_db9_branch;
data branch create table t_extra from t1;

set @b1_tid = (select rel_id from mo_catalog.mo_tables where reldatabase='protect_db9_branch' and relname='t1');
set @b_extra_tid = (select rel_id from mo_catalog.mo_tables where reldatabase='protect_db9_branch' and relname='t_extra');
set @b1_sname = concat('__mo_branch_', cast(@b1_tid as char));
set @b_extra_sname = concat('__mo_branch_', cast(@b_extra_tid as char));

-- Pre-drop: 2 branch snapshots live.
select count(*) as branch_rows_before
  from mo_catalog.mo_snapshots
  where kind='branch' and sname in (@b1_sname, @b_extra_sname);

-- Plain DDL `drop database` must cascade through both branch children.
use protect_db9_parent;
drop database protect_db9_branch;

-- Post-drop: both reclaimed.
select count(*) as branch_rows_after
  from mo_catalog.mo_snapshots
  where kind='branch' and sname in (@b1_sname, @b_extra_sname);

-- Both metadata rows flipped table_deleted=true.
select count(*) as deleted_branch_meta_rows
  from mo_catalog.mo_branch_metadata
  where table_id in (@b1_tid, @b_extra_tid) and table_deleted = true;

drop database protect_db9_parent;
