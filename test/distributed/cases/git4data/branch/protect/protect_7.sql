-- Branch Protect Snapshot — account scoping.
-- Verifies that a branch snapshot row created within an account is
-- visible under that account and is reclaimed when the child is dropped.

drop database if exists protect_db7;
create database protect_db7;
use protect_db7;

create table t1(a int primary key);
insert into t1 values (1);

data branch create table t2 from t1;

set @t2_tid = (select rel_id from mo_catalog.mo_tables where reldatabase='protect_db7' and relname='t2');
set @t1_tid = (select rel_id from mo_catalog.mo_tables where reldatabase='protect_db7' and relname='t1');
set @t2_sname = concat('__mo_branch_', cast(@t2_tid as char));

-- Row is anchored on the creator's account with obj_id pointing at t1.
select level,
       database_name,
       table_name,
       obj_id = @t1_tid as obj_id_matches_parent
  from mo_catalog.mo_snapshots
  where sname = @t2_sname and kind = 'branch';

-- Drop the branch child. Snapshot row is reclaimed.
drop table t2;

select count(*) as branch_row_after_drop
  from mo_catalog.mo_snapshots where sname = @t2_sname and kind = 'branch';

drop table t1;
drop database protect_db7;
