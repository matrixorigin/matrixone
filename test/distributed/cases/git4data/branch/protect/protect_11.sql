-- Branch Protect Snapshot — reject branch-managed snames in user-facing entry points.
-- Verifies that branch-managed snapshots cannot be addressed by name through
-- DROP SNAPSHOT, {snapshot = '...'} hint, etc.

drop database if exists protect_db11;
create database protect_db11;
use protect_db11;

create table t1(a int primary key);
insert into t1 values (1);

data branch create table t2 from t1;

-- DROP SNAPSHOT on a branch-managed sname must be rejected before any catalog
-- lookup — hence we use a synthetic name the user could plausibly guess.
drop snapshot `__mo_branch_probe`;

-- Same rejection path for DROP SNAPSHOT IF EXISTS — the prefix guard runs
-- before IfExists logic, so even with the IF EXISTS clause the user cannot
-- silently target a branch-managed row.
drop snapshot if exists `__mo_branch_probe`;

-- `{snapshot = '__mo_branch_*'}` hint in a user query must also be rejected.
select * from t1{snapshot = '__mo_branch_probe'};

drop table t2;
drop table t1;
drop database protect_db11;
