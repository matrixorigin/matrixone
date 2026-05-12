-- Branch Protect Snapshot — reclaim on plain `drop table`.
-- Verifies that the ddl.go drop-table path (not `data branch delete table`)
-- also releases the branch snapshot via the shared reclaim helper.

drop database if exists protect_db3;
create database protect_db3;
use protect_db3;

create table t1(a int primary key);
insert into t1 values (1);

data branch create table t2 from t1;

set @t2_tid = (select rel_id from mo_catalog.mo_tables where reldatabase='protect_db3' and relname='t2');
set @t2_sname = concat('__mo_branch_', cast(@t2_tid as char));

select count(*) as branch_row_before_drop
  from mo_catalog.mo_snapshots where sname = @t2_sname and sname like '__mo_branch_%';

-- plain DDL drop — not `data branch delete table`.
drop table t2;

-- ddl.go flips table_deleted=true
select table_deleted from mo_catalog.mo_branch_metadata where table_id = @t2_tid;

-- and the shared reclaim helper wipes the snapshot row
select count(*) as branch_row_after_drop
  from mo_catalog.mo_snapshots where sname = @t2_sname and sname like '__mo_branch_%';

drop table t1;
drop database protect_db3;
