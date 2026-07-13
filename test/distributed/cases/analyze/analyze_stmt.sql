-- prepare
drop database if exists db_analyze_stmt;
create database db_analyze_stmt;
use db_analyze_stmt;

drop table if exists t_analyze_01;
create table t_analyze_01(
    a int,
    b varchar(10)
);
insert into t_analyze_01 values
    (1, 'a'),
    (1, 'a'),
    (2, 'b'),
    (2, 'c');

drop table if exists t_analyze_02;
create table t_analyze_02(
    x int,
    y int
);
insert into t_analyze_02 values
    (1, 10),
    (2, 20),
    (3, 30),
    (4, 40);

drop table if exists quoted_cols;
create table quoted_cols(`select` int, `a-b` int, `tick``name` int);
insert into quoted_cols values (1, 2, 3), (1, 3, 4);

-- ANALYZE TABLE single table (existing behavior)
analyze table t_analyze_01(a, b);

-- ANALYZE TABLE without column list (issue #23122 core case)
analyze table t_analyze_01;

-- quoted explicit and catalog-expanded column names
analyze table quoted_cols(`select`, `a-b`, `tick``name`);
select 'AFTER_EXPLICIT_QUOTED';
analyze table quoted_cols;
select 'AFTER_EXPANDED_QUOTED';

-- persistent connection after single-table, multi-table, and mid-list error
analyze table t_analyze_01;
select 'AFTER_SINGLE';
analyze table t_analyze_01, t_analyze_02;
select 'AFTER_MULTI';
analyze table t_analyze_01, t_analyze_nonexistent, t_analyze_02;
select 'AFTER_MID_LIST_ERROR';

-- ANALYZE TABLE without column list: non-existent table
analyze table t_analyze_nonexistent;

-- implicit columns must be resolved from the same historical schema
drop snapshot if exists analyze_schema_snapshot;
drop table if exists snapshot_cols;
create table snapshot_cols(old_col int);
insert into snapshot_cols values (1), (2);
create snapshot analyze_schema_snapshot for account;
alter table snapshot_cols add column current_only int;
analyze table snapshot_cols {snapshot = 'analyze_schema_snapshot'};
select 'AFTER_SNAPSHOT_ANALYZE';
drop snapshot analyze_schema_snapshot;
drop table snapshot_cols;

-- CHECK TABLE: returns not-supported error
check table t_analyze_01;
check table t_analyze_01 extended;
check table t_analyze_01 for upgrade;
check table t_analyze_01, t_analyze_02;
check table t_analyze_01, t_analyze_02 extended;

-- SHOW PROFILE: returns not-supported error
show profile;
show profile for query 2;
show profile limit 10;
show profile for query 2 limit 10;
show profile for query 2 limit 10 offset 5;

-- transaction gate: statements should reach their handlers, not the
-- generic unclassified-transaction error
begin;
analyze table t_analyze_01(a, b);
commit;

begin;
analyze table t_analyze_01;
commit;

begin;
analyze table t_analyze_01, t_analyze_02;
select 'AFTER_TXN_MULTI';
rollback;

begin;
check table t_analyze_01;
rollback;

begin;
show profile;
rollback;

-- cleanup
drop table t_analyze_01;
drop table t_analyze_02;
drop table quoted_cols;
drop database db_analyze_stmt;
