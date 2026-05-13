-- Branch Protect Snapshot — reclaim on `data branch delete table`.
-- Verifies that dropping a leaf branch reclaims exactly its own
-- __mo_branch_<tid> row, while intermediate branches remain protected as
-- long as any descendant is alive.

drop database if exists protect_db2;
create database protect_db2;
use protect_db2;

create table t1(a int primary key);
insert into t1 values (1), (2);

data branch create table t2 from t1;
data branch create table t3 from t2;

set @t2_tid = (select rel_id from mo_catalog.mo_tables where reldatabase='protect_db2' and relname='t2');
set @t3_tid = (select rel_id from mo_catalog.mo_tables where reldatabase='protect_db2' and relname='t3');
set @t2_sname = concat('__mo_branch_', cast(@t2_tid as char));
set @t3_sname = concat('__mo_branch_', cast(@t3_tid as char));

-- Initial state: both edges protected.
select count(*) as initial_branch_rows
  from mo_catalog.mo_snapshots
  where sname in (@t2_sname, @t3_sname) and sname like '__mo_branch_%';

-- Delete the leaf t3. Only __mo_branch_<t3> is reclaimable.
data branch delete table protect_db2.t3;

select sname like concat('%', cast(@t2_tid as char)) as keeps_t2_edge
  from mo_catalog.mo_snapshots
  where sname like '__mo_branch_%' and sname in (@t2_sname, @t3_sname);

-- Delete the remaining branch t2. Both edges are gone.
data branch delete table protect_db2.t2;

select count(*) as remaining_branch_rows
  from mo_catalog.mo_snapshots
  where sname in (@t2_sname, @t3_sname) and sname like '__mo_branch_%';

drop table t1;
drop database protect_db2;
