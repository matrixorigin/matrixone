-- Smoke test for the new index-plugin dispatch path on the CPU-reachable
-- vector algorithms (HNSW, IVF-FLAT). Proves CREATE INDEX dispatches to the
-- plugin, registers the algorithm + its hidden tables in mo_catalog.mo_indexes,
-- round-trips through SHOW CREATE TABLE, and answers a vector search — i.e. the
-- registration/dispatch/catalog hooks of the plugin framework, end to end.

drop database if exists vector_plugin_smoke;
create database vector_plugin_smoke;
use vector_plugin_smoke;

-- ---------------- HNSW (gated by the plugin's ExperimentalFlag hook) --------
set experimental_hnsw_index = 0;
create table h (a bigint primary key, v vecf32(3));
insert into h values (1, '[1,1,1]'), (2, '[2,2,2]'), (3, '[3,3,3]'), (4, '[8,8,8]');

-- flag OFF: the plugin's experimental-flag gate rejects the create
create index ix using hnsw on h (v) op_type "vector_l2_ops";

-- flag ON: dispatch builds the index
set experimental_hnsw_index = 1;
create index ix using hnsw on h (v) op_type "vector_l2_ops";

-- plugin registration: algo + hidden-table types written by the catalog hook
select algo, algo_table_type from mo_catalog.mo_indexes
  where table_id = (select rel_id from mo_catalog.mo_tables
                    where relname = 'h' and reldatabase = 'vector_plugin_smoke')
  order by algo_table_type;
show create table h;
select a from h order by l2_distance(v, '[1,1,1]') asc limit 2;
set experimental_hnsw_index = 0;

-- ---------------- IVF-FLAT --------------------------------------------------
set experimental_ivf_index = 1;
create table f (a bigint primary key, v vecf32(3));
insert into f values (1, '[1,1,1]'), (2, '[2,2,2]'), (3, '[3,3,3]'), (4, '[8,8,8]');
create index ix using ivfflat on f (v) lists = 2 op_type 'vector_l2_ops';

select algo, algo_table_type from mo_catalog.mo_indexes
  where table_id = (select rel_id from mo_catalog.mo_tables
                    where relname = 'f' and reldatabase = 'vector_plugin_smoke')
  order by algo_table_type;
show create table f;
select a from f order by l2_distance(v, '[1,1,1]') asc limit 2;
set experimental_ivf_index = 0;

drop database vector_plugin_smoke;
