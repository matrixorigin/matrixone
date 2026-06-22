-- ALTER TABLE ... ALTER REINDEX per-index build options.
--
-- The REINDEX rule shares index_option_list with CREATE INDEX, so every option
-- parses. Each algorithm now honors the build options it supports on a rebuild
-- (merging them into the persisted algo params, visible via SHOW CREATE TABLE)
-- and rejects any option it does not support with a "not supported" error.
--
-- CPU-only coverage (ivfflat + hnsw). CAGRA / IVF-PQ require a GPU to build, so
-- their per-index merge/reject is covered by unit tests in
-- pkg/vectorindex/{cagra,ivfpq}/plugin/compile.
SET experimental_ivf_index = 1;
SET experimental_hnsw_index = 1;

drop database if exists test_reindex_options;
create database test_reindex_options;
use test_reindex_options;

-- ----------------------------------------------------------------------------
-- IVF-FLAT: honors lists + kmeans_train_percent + kmeans_max_iteration
-- ----------------------------------------------------------------------------
create table ivf_t(a int primary key, b vecf32(4));
insert into ivf_t values(1,"[1,2,3,4]"),(2,"[5,6,7,8]"),(3,"[9,10,11,12]"),(4,"[2,1,4,3]"),(5,"[8,7,6,5]"),(6,"[12,11,10,9]"),(7,"[1,1,1,1]"),(8,"[9,9,9,9]");
create index idx using ivfflat on ivf_t(b) lists=2 op_type "vector_l2_ops";
show create table ivf_t;
-- reindex with new supported params -> merged into algo params
alter table ivf_t alter reindex idx ivfflat lists=4 kmeans_train_percent=80 kmeans_max_iteration=50;
show create table ivf_t;

-- IVF-FLAT rejects options it does not honor (HNSW / CAGRA params)
alter table ivf_t alter reindex idx ivfflat m=16;
alter table ivf_t alter reindex idx ivfflat ef_construction=200;
alter table ivf_t alter reindex idx ivfflat graph_degree=64;

-- ----------------------------------------------------------------------------
-- HNSW: honors m + ef_construction + ef_search + max_index_capacity
-- ----------------------------------------------------------------------------
create table hnsw_t(a bigint primary key, b vecf32(4));
insert into hnsw_t values(1,"[1,2,3,4]"),(2,"[5,6,7,8]"),(3,"[9,10,11,12]"),(4,"[2,1,4,3]");
create index hidx using hnsw on hnsw_t(b) op_type "vector_l2_ops" m=48 ef_construction=64 ef_search=64;
show create table hnsw_t;
-- reindex with new supported params -> merged into algo params
alter table hnsw_t alter reindex hidx hnsw m=32 ef_construction=128 ef_search=100 max_index_capacity=100000;
show create table hnsw_t;

-- HNSW rejects options it does not honor (IVF params)
alter table hnsw_t alter reindex hidx hnsw lists=4;
alter table hnsw_t alter reindex hidx hnsw kmeans_train_percent=5;

drop database test_reindex_options;
