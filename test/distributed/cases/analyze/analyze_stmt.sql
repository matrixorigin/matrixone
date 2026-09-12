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
insert into quoted_cols values (1, 2, 3), (2, 3, 4);

-- AUTO returns one maintenance-summary row and publishes sampled statistics
analyze table t_analyze_01(a, b);
select table_cnt,
json_extract(ndv_map, '$.a') as a_ndv,
json_extract(ndv_map, '$.b') as b_ndv
from table_stats('db_analyze_stmt.t_analyze_01', 'get', 'normal') g;

-- ANALYZE TABLE without column list (issue #23122 core case)
analyze table t_analyze_01;

-- FULLSCAN uses the same command path and reports its selected mode
analyze table t_analyze_01(a, b) fullscan;

-- quoted explicit and catalog-expanded column names
analyze table quoted_cols(`select`, `a-b`, `tick``name`);
select 'AFTER_EXPLICIT_QUOTED';
analyze table quoted_cols;
select 'AFTER_EXPANDED_QUOTED';

-- only owned physical tables may publish optimizer statistics
create view v_analyze as select a, b from t_analyze_01;
analyze table v_analyze(a);
select 'AFTER_VIEW_ANALYZE';

-- quoted database, table, and column identifiers
create database `select-db`;
create table `select-db`.`tick``table`(`a-b` int);
insert into `select-db`.`tick``table` values (1),(1),(2);
analyze table `select-db`.`tick``table`(`a-b`);
analyze table `select-db`.`tick``table`;
select 'AFTER_QUOTED_TABLE';
drop table `select-db`.`tick``table`;
drop database `select-db`;

-- persistent connection after single-table, multi-table, and mid-list error
analyze table t_analyze_01;
select 'AFTER_SINGLE';
analyze table t_analyze_01, t_analyze_02;
select 'AFTER_MULTI';
analyze table t_analyze_01, t_analyze_nonexistent, t_analyze_02;
select 'AFTER_MID_LIST_ERROR';

-- duplicate targets are rejected before collection
analyze table t_analyze_01, t_analyze_01;
select 'AFTER_DUPLICATE_TARGETS';

-- ANALYZE TABLE without column list: non-existent table
analyze table t_analyze_nonexistent;

-- explicit missing columns return a semantic error and keep the connection usable
analyze table t_analyze_01(missing_column);
select 'AFTER_MISSING_COLUMN_ERROR';

-- historical snapshots cannot publish current optimizer statistics
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

-- SQL PREPARE/EXECUTE keeps ANALYZE's maintenance result and bound database
prepare analyze_explicit from analyze table t_analyze_01(a, b);
execute analyze_explicit;
execute analyze_explicit;
deallocate prepare analyze_explicit;
prepare analyze_implicit from analyze table t_analyze_01;
execute analyze_implicit;
deallocate prepare analyze_implicit;
create database analyze_prepare_other;
create table analyze_prepare_other.t_analyze_01(a int, b int);
insert into analyze_prepare_other.t_analyze_01 values (9, 9), (9, 9);
prepare analyze_bound from analyze table t_analyze_01(a);
use analyze_prepare_other;
execute analyze_bound;
use db_analyze_stmt;
deallocate prepare analyze_bound;
drop database analyze_prepare_other;

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

-- ANALYZE rejects an already-active user transaction because publication is
-- outside the transaction workspace
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

-- A transaction-local table is not eligible for published optimizer statistics.
begin;
create table txn_created_analyze(a int);
insert into txn_created_analyze values (1), (2);
analyze table txn_created_analyze(a);
rollback;
drop table if exists txn_created_analyze;

-- Uncommitted workspace rows are never mixed into published statistics.
begin;
insert into t_analyze_01 values (3, 30);
analyze table t_analyze_01(a);
rollback;

begin;
check table t_analyze_01;
rollback;

begin;
show profile;
rollback;

-- cleanup
drop view v_analyze;
drop table t_analyze_01;
drop table t_analyze_02;
drop table quoted_cols;
drop database db_analyze_stmt;
