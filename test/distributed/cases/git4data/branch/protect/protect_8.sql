-- Branch Protect Snapshot — `data branch create database` & `data branch delete database`.
-- Verifies that the database-level branch DDL populates one branch-kind
-- snapshot per cloned table and reclaims all of them on delete.

drop database if exists protect_db8_src;
drop database if exists protect_db8_dst;

create database protect_db8_src;
use protect_db8_src;

create table t1(a int primary key);
create table t2(a int primary key);
create table t3(a int primary key);
insert into t1 values (1);
insert into t2 values (2);
insert into t3 values (3);

data branch create database protect_db8_dst from protect_db8_src;

-- One branch snapshot per cloned table.
select count(*) as branch_rows_after_db_create
  from mo_catalog.mo_snapshots
  where sname like '__mo_branch_%' and database_name = 'protect_db8_src';

-- All three branch rows reference parent tables in src db.
set @dst_t1_tid = (select rel_id from mo_catalog.mo_tables where reldatabase='protect_db8_dst' and relname='t1');
set @dst_t2_tid = (select rel_id from mo_catalog.mo_tables where reldatabase='protect_db8_dst' and relname='t2');
set @dst_t3_tid = (select rel_id from mo_catalog.mo_tables where reldatabase='protect_db8_dst' and relname='t3');
set @dst_t1_sname = concat('__mo_branch_', cast(@dst_t1_tid as char));
set @dst_t2_sname = concat('__mo_branch_', cast(@dst_t2_tid as char));
set @dst_t3_sname = concat('__mo_branch_', cast(@dst_t3_tid as char));

select count(*) as matched_edges
  from mo_catalog.mo_snapshots
  where sname like '__mo_branch_%'
    and sname in (@dst_t1_sname, @dst_t2_sname, @dst_t3_sname);

-- `data branch delete database` reclaims all three branch snapshots.
data branch delete database protect_db8_dst;

select count(*) as branch_rows_after_db_delete
  from mo_catalog.mo_snapshots
  where sname like '__mo_branch_%'
    and sname in (@dst_t1_sname, @dst_t2_sname, @dst_t3_sname);

drop database protect_db8_src;
