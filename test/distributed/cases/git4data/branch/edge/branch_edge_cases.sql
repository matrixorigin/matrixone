-- Data branch edge cases that are not covered by basic diff/merge/pick flows.

-- Case 1: secondary/unique indexes remain valid after PICK/MERGE.
drop database if exists br_index;
create database br_index;
use br_index;

create table base(
  id int primary key,
  u int,
  k int,
  v varchar(20),
  unique key uk_u(u),
  key idx_k(k)
);
insert into base values
  (1, 101, 10, 'one'),
  (2, 102, 20, 'two'),
  (3, 103, 30, 'three');

data branch create table dst from base;
data branch create table src from base;

insert into src values (4, 104, 20, 'four');
update src set k = 40, v = 'two-src' where id = 2;
data branch pick src into dst keys(2, 4) when conflict accept;

select id, u, k, v from dst where k = 20 order by id;
select id, u, k, v from dst where k = 40 order by id;

-- Ordinary unique-index enforcement still rejects duplicates after PICK.
-- @regex("Duplicate entry '104' for key '(u|__mo_index_idx_col)'(\[[0-9,]+\])?",true)
insert into dst values (5, 104, 50, 'dup-u');

data branch create table merge_dst from base;
data branch create table merge_src from base;
insert into merge_src values (4, 104, 40, 'four');
data branch merge merge_src into merge_dst;
select id, u, k, v from merge_dst where u = 104;

data branch create table conflict_dst from base;
data branch create table conflict_src from base;
insert into conflict_dst values (4, 104, 40, 'dst-four');
insert into conflict_src values (5, 104, 50, 'src-five');
-- MERGE must not bypass unique-index validation.
-- @regex("Duplicate entry '104' for key '(u|__mo_index_idx_col)'(\[[0-9,]+\])?",true)
data branch merge conflict_src into conflict_dst when conflict accept;
select id, u, k, v from conflict_dst order by id;

drop database br_index;

-- Case 2: schema drift after branching.
drop database if exists br_schema_drift;
create database br_schema_drift;
use br_schema_drift;

create table base(a int primary key, b int);
insert into base values (1, 10), (2, 20), (3, 30);

data branch create table left_add from base;
data branch create table right_add from base;
alter table left_add add column c varchar(20) default 'left-only';
insert into right_add values (4, 40);
-- Target-only column c must not produce false diffs; only a=4 (base-only) appears.
data branch diff left_add against right_add;

data branch create table both_add_left from base;
data branch create table both_add_right from base;
alter table both_add_left add column c varchar(20) default 'same';
alter table both_add_right add column c varchar(20) default 'same';
update both_add_left set c = 'left' where a = 2;
update both_add_right set c = 'right' where a = 3;
-- ALTER preserves branch lineage: changes to inherited rows remain UPDATEs,
-- and only the side that changed each row is emitted.
data branch diff both_add_left against both_add_right columns (a, c);

data branch create table type_left from base;
data branch create table type_right from base;
alter table type_left add column c int default 0;
alter table type_right add column c varchar(20) default 'x';
-- Type mismatch on a common column is rejected.
-- @regex("schema compatibility check: column 'c' exists in both schemas but has different types",true)
data branch diff type_left against type_right;

data branch create table rename_left from base;
data branch create table rename_right from base;
alter table rename_left rename column b to bb;
-- A one-sided rename removes base-visible column b from the target schema.
-- @regex("schema compatibility check: base column 'b' is not present in target schema",true)
data branch diff rename_left against rename_right;

-- Case 3: historical ALTER lineage has explicit owners and is reclaimed
-- synchronously when the last owner disappears.
drop snapshot if exists history_owner_s;
create table history_owner_base(a int primary key, b int);
create snapshot history_owner_s for table br_schema_drift history_owner_base;
alter table history_owner_base add column c int default 0;
set @history_owner_tid = (
  select rel_id from mo_catalog.mo_tables
   where reldatabase = 'br_schema_drift' and relname = 'history_owner_base'
);
select count(*) as lineage_before_source_drop
  from mo_catalog.mo_branch_metadata
 where table_id = @history_owner_tid and level = 'alter';
drop snapshot history_owner_s;
select count(*) as snapshots_after_source_drop
  from mo_catalog.mo_snapshots
 where kind = 'branch' and table_name = 'history_owner_base';
select count(*) as lineage_after_source_drop
  from mo_catalog.mo_branch_metadata
 where table_id = @history_owner_tid and level = 'alter';
alter table history_owner_base add column d int default 0;
set @history_owner_second_tid = (
  select rel_id from mo_catalog.mo_tables
   where reldatabase = 'br_schema_drift' and relname = 'history_owner_base'
);
select count(*) as lineage_after_second_alter
  from mo_catalog.mo_branch_metadata
 where table_id = @history_owner_second_tid and level = 'alter';
drop table history_owner_base;

-- Two covered ALTERs leave a deleted intermediate generation. Dropping the
-- last historical owner must reclaim both metadata/snapshot pairs.
drop snapshot if exists history_multi_s;
create table history_multi(a int primary key, b int);
create snapshot history_multi_s for table br_schema_drift history_multi;
alter table history_multi add column c int default 0;
set @history_multi_t2 = (select rel_id from mo_catalog.mo_tables where reldatabase = 'br_schema_drift' and relname = 'history_multi');
alter table history_multi add column d int default 0;
set @history_multi_t3 = (select rel_id from mo_catalog.mo_tables where reldatabase = 'br_schema_drift' and relname = 'history_multi');
select count(*) as multi_lineage_before_owner_drop from mo_catalog.mo_branch_metadata where table_id in (@history_multi_t2, @history_multi_t3) and level = 'alter';
select count(*) as multi_snapshots_before_owner_drop from mo_catalog.mo_snapshots where sname in (concat('__mo_branch_', cast(@history_multi_t2 as char)), concat('__mo_branch_', cast(@history_multi_t3 as char))) and kind = 'branch';
drop snapshot history_multi_s;
select count(*) as multi_lineage_after_owner_drop from mo_catalog.mo_branch_metadata where table_id in (@history_multi_t2, @history_multi_t3) and level = 'alter';
select count(*) as multi_snapshots_after_owner_drop from mo_catalog.mo_snapshots where sname in (concat('__mo_branch_', cast(@history_multi_t2 as char)), concat('__mo_branch_', cast(@history_multi_t3 as char))) and kind = 'branch';
drop table history_multi;

-- If the current table is dropped first, ordinary DROP reclaims the branch
-- snapshots. The later owner drop must still reclaim the orphaned metadata.
drop snapshot if exists history_drop_current_s;
create table history_drop_current(a int primary key, b int);
create snapshot history_drop_current_s for table br_schema_drift history_drop_current;
alter table history_drop_current add column c int default 0;
set @history_drop_current_t2 = (select rel_id from mo_catalog.mo_tables where reldatabase = 'br_schema_drift' and relname = 'history_drop_current');
alter table history_drop_current add column d int default 0;
set @history_drop_current_t3 = (select rel_id from mo_catalog.mo_tables where reldatabase = 'br_schema_drift' and relname = 'history_drop_current');
drop table history_drop_current;
select count(*) as dropped_current_lineage_before_owner_drop from mo_catalog.mo_branch_metadata where table_id in (@history_drop_current_t2, @history_drop_current_t3) and level = 'alter';
select count(*) as dropped_current_snapshots_before_owner_drop from mo_catalog.mo_snapshots where sname in (concat('__mo_branch_', cast(@history_drop_current_t2 as char)), concat('__mo_branch_', cast(@history_drop_current_t3 as char))) and kind = 'branch';
drop snapshot history_drop_current_s;
select count(*) as dropped_current_lineage_after_owner_drop from mo_catalog.mo_branch_metadata where table_id in (@history_drop_current_t2, @history_drop_current_t3) and level = 'alter';
select count(*) as dropped_current_snapshots_after_owner_drop from mo_catalog.mo_snapshots where sname in (concat('__mo_branch_', cast(@history_drop_current_t2 as char)), concat('__mo_branch_', cast(@history_drop_current_t3 as char))) and kind = 'branch';

-- Account- and database-level owners must retain the same scope evidence
-- after current-table-first DROP, not only table-level owners.
drop snapshot if exists history_account_s;
create table history_account(a int primary key, b int);
create snapshot history_account_s for account sys;
alter table history_account add column c int default 0;
set @history_account_tid = (select rel_id from mo_catalog.mo_tables where reldatabase = 'br_schema_drift' and relname = 'history_account');
drop table history_account;
select count(*) as account_lineage_before_owner_drop from mo_catalog.mo_branch_metadata where table_id = @history_account_tid and level = 'alter';
select count(*) as account_snapshots_before_owner_drop from mo_catalog.mo_snapshots where sname = concat('__mo_branch_', cast(@history_account_tid as char)) and kind = 'branch';
drop snapshot history_account_s;
select count(*) as account_lineage_after_owner_drop from mo_catalog.mo_branch_metadata where table_id = @history_account_tid and level = 'alter';
select count(*) as account_snapshots_after_owner_drop from mo_catalog.mo_snapshots where sname = concat('__mo_branch_', cast(@history_account_tid as char)) and kind = 'branch';

drop snapshot if exists history_database_s;
create table history_database_base(a int primary key, b int);
data branch create table history_database from history_database_base;
create snapshot history_database_s for database br_schema_drift;
alter table history_database add column c int default 0;
set @history_database_tid = (select rel_id from mo_catalog.mo_tables where reldatabase = 'br_schema_drift' and relname = 'history_database');
data branch delete table history_database;
select count(*) as database_lineage_before_owner_drop from mo_catalog.mo_branch_metadata where table_id = @history_database_tid and level like 'alter%';
select count(*) as database_snapshots_before_owner_drop from mo_catalog.mo_snapshots where sname = concat('__mo_branch_', cast(@history_database_tid as char)) and kind = 'branch';
drop snapshot history_database_s;
select count(*) as database_lineage_after_owner_drop from mo_catalog.mo_branch_metadata where table_id = @history_database_tid and level = 'alter';
select count(*) as database_snapshots_after_owner_drop from mo_catalog.mo_snapshots where sname = concat('__mo_branch_', cast(@history_database_tid as char)) and kind = 'branch';
drop table history_database_base;

-- A live logical branch owns the connected ALTER component. Deleting the
-- final branch releases both its protect snapshot and the ALTER-only edge.
create table history_live_base(a int primary key, b int);
data branch create table history_live_child from history_live_base;
alter table history_live_base add column c int default 0;
set @history_live_tid = (
  select rel_id from mo_catalog.mo_tables
   where reldatabase = 'br_schema_drift' and relname = 'history_live_base'
);
select count(*) as lineage_with_live_branch
  from mo_catalog.mo_branch_metadata
 where table_id = @history_live_tid and level = 'alter';
data branch delete table history_live_child;
select count(*) as snapshots_after_branch_drop
  from mo_catalog.mo_snapshots
 where kind = 'branch' and table_name = 'history_live_base';
select count(*) as lineage_after_branch_drop
  from mo_catalog.mo_branch_metadata
 where table_id = @history_live_tid and level = 'alter';
drop table history_live_base;

drop database br_schema_drift;

-- Case 4: transaction behavior.
drop database if exists br_txn_src;
drop database if exists br_txn_dst;
create database br_txn_src;
use br_txn_src;

create table base(a int primary key, b varchar(20));
insert into base values (1, 'one'), (2, 'two');

-- Historical snapshot coverage alone must not turn an ordinary ALTER into the
-- explicit-transaction restriction used for live data-branch lineages.
create table alter_begin(a int primary key, b int);
insert into alter_begin values (1, 10);
create table alter_autocommit(a int primary key, b int);
insert into alter_autocommit values (1, 10);
create snapshot br_alter_begin_s for table br_txn_src alter_begin;
create snapshot br_alter_autocommit_s for table br_txn_src alter_autocommit;
begin;
alter table alter_begin add column c int default 7;
commit;
select a, b, c from alter_begin order by a;
set autocommit=0;
alter table alter_autocommit add column c int default 9;
commit;
set autocommit=1;
select a, b, c from alter_autocommit order by a;
drop snapshot br_alter_begin_s;
drop snapshot br_alter_autocommit_s;

begin;
data branch create table br_commit from base;
commit;
select count(*) as br_commit_rows from br_commit;

begin;
data branch create table br_rollback from base;
rollback;
select count(*) as br_rollback_tables
  from mo_catalog.mo_tables
 where reldatabase = 'br_txn_src' and relname = 'br_rollback';

begin;
data branch delete table br_txn_src.br_commit;
rollback;
select count(*) as br_commit_after_rollback
  from mo_catalog.mo_tables
 where reldatabase = 'br_txn_src' and relname = 'br_commit';

begin;
data branch delete table br_txn_src.br_commit;
commit;
select count(*) as br_commit_after_commit
  from mo_catalog.mo_tables
 where reldatabase = 'br_txn_src' and relname = 'br_commit';

data branch create table pick_dst from base;
data branch create table pick_src from base;
insert into pick_src values (3, 'three');

begin;
-- @regex("(DATA BRANCH MERGE/PICK is not supported in transactions|Write conflicts detected|txn need retry|deadlock detected)",true)
data branch pick pick_src into pick_dst keys(3) when conflict accept;
rollback;
select count(*) as pick_dst_rows from pick_dst;

data branch create table txn_merge_dst from base;
data branch create table txn_merge_src from base;
update txn_merge_src set b = 'two-txn' where a = 2;
insert into txn_merge_src values (3, 'three');
delete from txn_merge_src where a = 1;

-- Regression for #24924: DIFF/MERGE should honor explicit transaction boundaries.
-- @bvt:issue#24924
begin;
data branch diff txn_merge_src against txn_merge_dst output count;
rollback;
begin;
data branch merge txn_merge_src into txn_merge_dst when conflict accept;
rollback;
select a, b from txn_merge_dst order by a;
begin;
data branch merge txn_merge_src into txn_merge_dst when conflict accept;
commit;
select a, b from txn_merge_dst order by a;
-- @bvt:issue

data branch create database br_txn_dst from br_txn_src;
select count(*) as dst_tables
  from mo_catalog.mo_tables
 where reldatabase = 'br_txn_dst'
   and relname in ('base', 'pick_dst', 'pick_src');

begin;
data branch delete database br_txn_dst;
rollback;
select count(*) as dst_tables_after_rollback
  from mo_catalog.mo_tables
 where reldatabase = 'br_txn_dst'
   and relname in ('base', 'pick_dst', 'pick_src');

begin;
data branch delete database br_txn_dst;
commit;
select count(*) as dst_tables_after_commit
  from mo_catalog.mo_tables
 where reldatabase = 'br_txn_dst';

drop database if exists br_txn_dst;
drop database br_txn_src;
