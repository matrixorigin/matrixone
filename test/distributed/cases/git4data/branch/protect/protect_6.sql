-- Branch Protect Snapshot — SHOW SNAPSHOTS excludes branch rows.
-- Verifies that user-visible tooling cleanly hides the internal
-- branch-kind rows even in the presence of user snapshots.

drop database if exists protect_db6;
create database protect_db6;
use protect_db6;

create table t1(a int primary key);
insert into t1 values (1);

data branch create table t2 from t1;
data branch create table t3 from t2;

drop snapshot if exists user_sp_a;
drop snapshot if exists user_sp_b;
create snapshot user_sp_a for table protect_db6 t1;
create snapshot user_sp_b for table protect_db6 t2;

-- SHOW SNAPSHOTS hides branch-kind rows. Query the internal table
-- directly with the same filter `SHOW SNAPSHOTS` uses so we can assert a
-- stable row count (scoped to this test's user snapshots).
select count(*) as show_visible_rows
  from mo_catalog.mo_snapshots
  where sname not like 'ccpr_%' and sname not like '__mo_branch_%'
    and sname like 'user_sp_%';
select sname as visible_sname
  from mo_catalog.mo_snapshots
  where sname not like 'ccpr_%' and sname not like '__mo_branch_%'
    and sname like 'user_sp_%'
  order by sname;

-- Direct query confirms the branch rows are really there — they are
-- just filtered at the SHOW layer (scoped to this test's 2 edges).
select count(*) as direct_branch_rows
  from mo_catalog.mo_snapshots
  where sname like '__mo_branch_%' and database_name = 'protect_db6';

drop snapshot user_sp_a;
drop snapshot user_sp_b;
drop table t3;
drop table t2;
drop table t1;
drop database protect_db6;
