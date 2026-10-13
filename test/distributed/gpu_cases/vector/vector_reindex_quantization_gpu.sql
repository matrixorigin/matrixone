-- ALTER REINDEX that changes a CAGRA / IVF-PQ index's QUANTIZATION (#29009). GPU REQUIRED.
SET experimental_cagra_index = 1;
SET experimental_ivfpq_index = 1;

drop database if exists reindex_quantization_gpu;
create database reindex_quantization_gpu;
use reindex_quantization_gpu;

create table c(id bigint primary key, v vecf32(8) not null);
insert into c select result,
    concat('[', result, ',', result, ',', result, ',', result, ',', result, ',', result, ',', result, ',', result, ']')
from generate_series(1, 200) g;

-- CAGRA float32 -> int8.
create index ix using cagra on c(v) op_type 'vector_l2_ops' intermediate_graph_degree = 8 graph_degree = 4;
alter table c alter reindex ix cagra quantization 'int8' force_sync;
show create table c;
select id from c order by l2_distance(v, '[50,50,50,50,50,50,50,50]') limit 1;
alter table c alter reindex ix cagra merge quantization 'uint8';

-- CAGRA on a vecf16 base: int8 is accepted, float32 is an upcast and rejected.
create table c16(id bigint primary key, v vecf16(8) not null);
insert into c16 select id, cast(v as vecf16(8)) from c;
create index ix using cagra on c16(v) op_type 'vector_l2_ops' intermediate_graph_degree = 8 graph_degree = 4;
alter table c16 alter reindex ix cagra quantization 'int8';
alter table c16 alter reindex ix cagra quantization 'float32';
show create table c16;

-- IVF-PQ float32 -> float16; IVF-PQ on a vecf16 base rejects float32.
create table p(id bigint primary key, v vecf32(8) not null);
insert into p select id, v from c;
create index ix using ivfpq on p(v) op_type 'vector_l2_ops' lists = 2 m = 2 bits_per_code = 8;
alter table p alter reindex ix ivfpq quantization 'float16' force_sync;
show create table p;
select abs(id - 50) <= 5 as near from p order by l2_distance(v, '[50,50,50,50,50,50,50,50]') limit 1;
alter table p alter reindex ix ivfpq merge quantization 'int8';
create table p16(id bigint primary key, v vecf16(8) not null);
insert into p16 select id, v from c16;
create index ix using ivfpq on p16(v) op_type 'vector_l2_ops' lists = 2 m = 2 bits_per_code = 8;
alter table p16 alter reindex ix ivfpq quantization 'int8';
alter table p16 alter reindex ix ivfpq quantization 'float32';

drop database reindex_quantization_gpu;
