-- Show that CREATE INDEX captures the build-time session vars into
-- algo_params.session_vars (mo_catalog.mo_indexes). They ride with the index
-- def, so a background rebuild (restore reindex / async create) reads them back
-- and reproduces the create-time config instead of the process defaults.
-- GPU-only (ivfpq).
drop database if exists session_vars_db;
create database session_vars_db;
use session_vars_db;
set experimental_ivfpq_index = 1;
set kmeans_train_percent = 100;
set kmeans_max_iteration = 12;
-- Set the captured var to a NON-DEFAULT value that no other case uses. Two
-- reasons: asserting the default (0) would pass even if capture were broken and
-- the field were hard-coded, and the tester shares one session across case
-- files, so several ivfpq cases leave ivfpq_threads_build = 6 behind and this
-- case would otherwise assert whatever ran before it.
set ivfpq_threads_build = 3;
create table t (a bigint primary key, v vecf32(8));
insert into t values (1, '[1,1,1,1,1,1,1,1]'), (2, '[2,2,2,2,2,2,2,2]'), (3, '[3,3,3,3,3,3,3,3]');
create index ix using ivfpq on t (v) op_type 'vector_l2_ops' lists=2 m=8 bits_per_code=8;
select algo_table_type, algo_params from mo_catalog.mo_indexes where name = 'ix' order by algo_table_type;
drop database session_vars_db;
