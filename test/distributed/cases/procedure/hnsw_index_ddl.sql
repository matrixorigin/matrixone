-- SQL procedure index DDL must preserve OP_TYPE as a string across execution.
-- Dedicated fixture: two vectors, no approximate search or async build waits.
drop database if exists procedure_hnsw_ddl;
create database procedure_hnsw_ddl;
use procedure_hnsw_ddl;
set @saved_hnsw = @@experimental_hnsw_index;
set @saved_ivf = @@experimental_ivf_index;
set experimental_hnsw_index = 1;
set experimental_ivf_index = 1;
create table vectors(id bigint primary key, v vecf32(4));
insert into vectors values (1, '[1,0,0,0]'), (2, '[0,1,0,0]');

-- Nearest control: the identical double-quoted HNSW DDL works directly.
create index ix using hnsw on vectors(v) op_type "vector_l2_ops" m=4 ef_construction=16 ef_search=16;
select distinct name, algo, algo_params from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'procedure_hnsw_ddl' and relname = 'vectors') and name = 'ix';
drop index ix on vectors;

-- Original CALL path, duplicate error, drop/recreate and same-session reuse.
create procedure p_create() 'begin create index ix using hnsw on vectors(v) op_type "vector_l2_ops" m=4 ef_construction=16 ef_search=16; end';
create procedure p_drop() 'begin drop index ix on vectors; end';
set experimental_hnsw_index = 0;
call p_create();
select count(*) as remaining_indexes from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'procedure_hnsw_ddl' and relname = 'vectors') and name = 'ix';
set experimental_hnsw_index = 1;
call p_create();
select distinct name, algo, algo_params from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'procedure_hnsw_ddl' and relname = 'vectors') and name = 'ix';
call p_create();
select count(distinct name) as logical_indexes from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'procedure_hnsw_ddl' and relname = 'vectors') and name = 'ix';
call p_drop();
call p_create();
call p_drop();
select count(*) as remaining_indexes from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'procedure_hnsw_ddl' and relname = 'vectors') and name = 'ix';

-- All other supported operators, single quotes and minimum options.
create procedure p_l2sq() 'begin create index ix using hnsw on vectors(v) op_type ''vector_l2sq_ops'' m=2 ef_construction=1 ef_search=1; end';
call p_l2sq();
select distinct name, algo, algo_params from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'procedure_hnsw_ddl' and relname = 'vectors') and name = 'ix';
call p_drop();
create procedure p_ip() 'begin create index ix using hnsw on vectors(v) op_type ''vector_ip_ops''; end';
call p_ip();
select distinct name, algo, algo_params from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'procedure_hnsw_ddl' and relname = 'vectors') and name = 'ix';
call p_drop();
create procedure p_cosine() 'begin create index ix using hnsw on vectors(v) op_type "vector_cosine_ops"; end';
call p_cosine();
select distinct name, algo, algo_params from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'procedure_hnsw_ddl' and relname = 'vectors') and name = 'ix';
call p_drop();
create procedure p_defaults() 'begin create index ix using hnsw on vectors(v); end';
call p_defaults();
select distinct name, algo, algo_params from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'procedure_hnsw_ddl' and relname = 'vectors') and name = 'ix';
call p_drop();

-- Runtime operator lookup is case-sensitive on both direct and CALL paths.
create index ix using hnsw on vectors(v) op_type 'VECTOR_L2SQ_OPS' m=2 ef_construction=1 ef_search=1;
select count(*) as remaining_indexes from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'procedure_hnsw_ddl' and relname = 'vectors') and name = 'ix';
create procedure p_upper() 'begin create index ix using hnsw on vectors(v) op_type ''VECTOR_L2SQ_OPS'' m=2 ef_construction=1 ef_search=1; end';
call p_upper();
select count(*) as remaining_indexes from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'procedure_hnsw_ddl' and relname = 'vectors') and name = 'ix';

-- The native connectivity lower bound applies equally to direct and CALL DDL.
create index ix using hnsw on vectors(v) op_type 'vector_l2_ops' m=1;
select count(*) as remaining_indexes from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'procedure_hnsw_ddl' and relname = 'vectors') and name = 'ix';
create procedure p_connectivity() 'begin create index ix using hnsw on vectors(v) op_type "vector_l2_ops" m=1; end';
call p_connectivity();
select count(*) as remaining_indexes from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'procedure_hnsw_ddl' and relname = 'vectors') and name = 'ix';

-- Invalid operators stay semantic errors, with no partial index metadata.
create procedure p_invalid() 'begin create index ix using hnsw on vectors(v) op_type "invalid_operator"; end';
call p_invalid();
select count(*) as remaining_indexes from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'procedure_hnsw_ddl' and relname = 'vectors') and name = 'ix';
create procedure p_quote() 'begin create index ix using hnsw on vectors(v) op_type "bad''operator"; end';
call p_quote();
select count(*) as remaining_indexes from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'procedure_hnsw_ddl' and relname = 'vectors') and name = 'ix';

-- Each numeric option must stay strictly positive; parse failures leave no index.
create procedure p_zero_m() 'begin create index ix using hnsw on vectors(v) op_type "vector_l2_ops" m=0; end';
call p_zero_m();
create procedure p_zero_construction() 'begin create index ix using hnsw on vectors(v) op_type "vector_l2_ops" ef_construction=0; end';
call p_zero_construction();
create procedure p_zero_search() 'begin create index ix using hnsw on vectors(v) op_type "vector_l2_ops" ef_search=0; end';
call p_zero_search();
create procedure p_negative() 'begin create index ix using hnsw on vectors(v) op_type "vector_l2_ops" m=-1; end';
call p_negative();
select count(*) as remaining_indexes from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'procedure_hnsw_ddl' and relname = 'vectors') and name = 'ix';
select count(*) as session_usable from vectors;
call p_create();
call p_drop();

-- Common IndexOption consumer: IVF-FLAT must also survive procedure formatting.
create procedure p_ivf() 'begin create index ix using ivfflat on vectors(v) lists=1 op_type ''vector_l2_ops''; end';
call p_ivf();
select distinct name, algo, algo_params from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'procedure_hnsw_ddl' and relname = 'vectors') and name = 'ix';
call p_drop();

set experimental_hnsw_index = @saved_hnsw;
set experimental_ivf_index = @saved_ivf;
drop database procedure_hnsw_ddl;
select count(*) as remaining_databases from mo_catalog.mo_database where datname = 'procedure_hnsw_ddl';
