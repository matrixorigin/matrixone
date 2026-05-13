-- Branch Protect Snapshot — fan-out independence.
-- Verifies that sibling branches are tracked independently: dropping one
-- sibling does not affect the other siblings' snapshots, even though they
-- all share the same parent.

drop database if exists protect_db5;
create database protect_db5;
use protect_db5;

create table t1(a int primary key);
insert into t1 values (1);

data branch create table t2 from t1;
data branch create table t3 from t1;
data branch create table t4 from t1;

set @t2_tid = (select rel_id from mo_catalog.mo_tables where reldatabase='protect_db5' and relname='t2');
set @t3_tid = (select rel_id from mo_catalog.mo_tables where reldatabase='protect_db5' and relname='t3');
set @t4_tid = (select rel_id from mo_catalog.mo_tables where reldatabase='protect_db5' and relname='t4');
set @t2_sname = concat('__mo_branch_', cast(@t2_tid as char));
set @t3_sname = concat('__mo_branch_', cast(@t3_tid as char));
set @t4_sname = concat('__mo_branch_', cast(@t4_tid as char));

-- Three edges, all anchored on t1.
select count(*) as initial_branch_rows
  from mo_catalog.mo_snapshots
  where sname in (@t2_sname, @t3_sname, @t4_sname) and sname like '__mo_branch_%';

-- Drop t3. Only __mo_branch_<t3> goes away.
drop table t3;

select count(*) as t2_edge
  from mo_catalog.mo_snapshots where sname = @t2_sname and sname like '__mo_branch_%';
select count(*) as t4_edge
  from mo_catalog.mo_snapshots where sname = @t4_sname and sname like '__mo_branch_%';
select count(*) as t3_edge_gone
  from mo_catalog.mo_snapshots where sname = @t3_sname and sname like '__mo_branch_%';

-- Drop t2. __mo_branch_<t2> goes away; __mo_branch_<t4> stays.
drop table t2;

select count(*) as t4_edge_still_there
  from mo_catalog.mo_snapshots where sname = @t4_sname and sname like '__mo_branch_%';
select count(*) as t2_edge_gone
  from mo_catalog.mo_snapshots where sname = @t2_sname and sname like '__mo_branch_%';

drop table t4;
drop table t1;
drop database protect_db5;
