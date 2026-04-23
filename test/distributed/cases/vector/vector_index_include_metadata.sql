drop database if exists vector_index_include_metadata;
create database vector_index_include_metadata;
use vector_index_include_metadata;

create table t_ivf(
    id int primary key,
    embedding vecf32(3),
    title varchar(20),
    category int
);

create index idx_ivf_include_meta using ivfflat on t_ivf(embedding)
lists=2 op_type "vector_l2_ops" include(title, category);

select algo_table_type, included_columns, case when algo_params like '%include_columns%' then 1 else 0 end as include_in_params
from mo_catalog.mo_indexes
where name = 'idx_ivf_include_meta'
  and algo = 'ivfflat'
  and table_id = (
      select rel_id
      from mo_catalog.mo_tables
      where reldatabase = database()
        and relname = 't_ivf'
  )
order by algo_table_type;

set experimental_hnsw_index = 1;

create table t_hnsw(
    id bigint primary key,
    embedding vecf32(3),
    title varchar(20),
    category int
);

insert into t_hnsw values
    (1, "[0,0,1]", "cold", 1),
    (2, "[0,1,0]", "warm", 2),
    (3, "[2,0,0]", "hot", 2);

create index idx_hnsw_include_meta using hnsw on t_hnsw(embedding)
op_type "vector_l2_ops" include(title, category);

select algo_table_type, included_columns, case when algo_params like '%include_columns%' then 1 else 0 end as include_in_params
from mo_catalog.mo_indexes
where name = 'idx_hnsw_include_meta'
  and algo = 'hnsw'
  and table_id = (
      select rel_id
      from mo_catalog.mo_tables
      where reldatabase = database()
        and relname = 't_hnsw'
  )
order by algo_table_type;

select id, title, category
from t_hnsw
where category = 2
order by l2_distance(embedding, "[0,1,0]")
limit 1;

set experimental_hnsw_index = 0;

drop database vector_index_include_metadata;
