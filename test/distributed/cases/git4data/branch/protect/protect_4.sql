-- Branch Protect Snapshot — subtree retention rule.
-- Verifies that dropping an intermediate branch while its subtree is still
-- alive does NOT release protection. Only when the whole subtree rooted at
-- the child is dead can the edge's snapshot be reclaimed.

drop database if exists protect_db4;
create database protect_db4;
use protect_db4;

create table t1(a int primary key);
insert into t1 values (1);

data branch create table t2 from t1;
data branch create table t3 from t2;

set @t2_tid = (select rel_id from mo_catalog.mo_tables where reldatabase='protect_db4' and relname='t2');
set @t3_tid = (select rel_id from mo_catalog.mo_tables where reldatabase='protect_db4' and relname='t3');
set @t2_sname = concat('__mo_branch_', cast(@t2_tid as char));
set @t3_sname = concat('__mo_branch_', cast(@t3_tid as char));

select count(*) as initial_branch_rows
  from mo_catalog.mo_snapshots
  where sname in (@t2_sname, @t3_sname) and sname like '__mo_branch_%';

-- Drop the intermediate t2. t3 is still alive, so __mo_branch_<t2> must
-- survive: t3's LCA probe against t1 would otherwise lose its retention
-- anchor.
drop table t2;

-- t2 metadata is flagged deleted, but the snapshot row survives.
select table_deleted from mo_catalog.mo_branch_metadata where table_id = @t2_tid;
select count(*) as t2_edge_retained
  from mo_catalog.mo_snapshots where sname = @t2_sname and sname like '__mo_branch_%';
select count(*) as t3_edge_retained
  from mo_catalog.mo_snapshots where sname = @t3_sname and sname like '__mo_branch_%';

-- Now drop t3. Both edges become reclaimable (the subtree rooted at t2 is
-- now fully dead).
drop table t3;

select count(*) as remaining_branch_rows
  from mo_catalog.mo_snapshots
  where sname in (@t2_sname, @t3_sname) and sname like '__mo_branch_%';

drop table t1;
drop database protect_db4;
