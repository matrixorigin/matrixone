-- =====================================================================
-- vector_clone_idxcron.sql — CREATE TABLE ... CLONE must register the
-- cloned vector index's idxcron task in mo_catalog.mo_index_update.
--
-- GPU REQUIRED (cagra / ivfpq builds).
--
-- Why: the idxcron scheduler discovers scheduled-reindex work ONLY by
-- SELECT-ing mo_catalog.mo_index_update (pkg/vectorindex/idxcron/executor.go).
-- If a cloned table never gets its row, the clone silently loses periodic
-- reindexing even though its CDC (incremental sync) is armed. This case is a
-- regression guard that the clone path (CreateTable -> CreateAllIndexUpdateTasks)
-- registers the row for BOTH cagra and ivfpq.
--
-- mo_index_update is a sys-resident catalog table NOT visible to tenant
-- accounts, so the whole case runs in the sys account and asserts the
-- account_id=0 rows directly. Only the identity columns + action are printed
-- (metadata carries machine-dependent thread defaults, so it is not asserted).
-- =====================================================================

set experimental_cagra_index = 1;
set experimental_ivfpq_index = 1;

drop database if exists clone_idx_db;
create database clone_idx_db;
use clone_idx_db;

-- CAGRA base table + index.
create table t_cagra (a bigint primary key, v vecf32(4));
insert into t_cagra values (1,'[1,1,1,1]'),(2,'[2,2,2,2]'),(3,'[3,3,3,3]'),(4,'[4,4,4,4]');
create index ix using cagra on t_cagra (v) op_type 'vector_l2_ops';

-- IVF-PQ base table + index.
create table t_ivfpq (a bigint primary key, v vecf32(4));
insert into t_ivfpq values (1,'[1,1,1,1]'),(2,'[2,2,2,2]'),(3,'[3,3,3,3]'),(4,'[4,4,4,4]');
create index ix using ivfpq on t_ivfpq (v) op_type 'vector_l2_ops' lists=1;

-- Both base tables registered.
select db_name, table_name, index_name, action from mo_catalog.mo_index_update where db_name = 'clone_idx_db' order by table_name, index_name;

-- Clone both tables. Each clone must seed its own idxcron registration row.
create table t_cagra_copy clone t_cagra;
create table t_ivfpq_copy clone t_ivfpq;

-- Expect 4 rows: base + clone for each algorithm.
select db_name, table_name, index_name, action from mo_catalog.mo_index_update where db_name = 'clone_idx_db' order by table_name, index_name;

-- Dropping a clone must remove only its row (DropAllIndexUpdateTasks).
drop table t_cagra_copy;
select db_name, table_name, index_name, action from mo_catalog.mo_index_update where db_name = 'clone_idx_db' order by table_name, index_name;

-- Cleanup: drop database removes the remaining rows.
drop database if exists clone_idx_db;
select count(*) from mo_catalog.mo_index_update where db_name = 'clone_idx_db';

set experimental_cagra_index = 0;
set experimental_ivfpq_index = 0;
