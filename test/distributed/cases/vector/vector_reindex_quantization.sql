-- ALTER REINDEX cannot change an IVF-FLAT index's QUANTIZATION (#29009); the index is unchanged.
SET experimental_ivf_index = 1;
SET probe_limit = 4;

drop database if exists reindex_quantization;
create database reindex_quantization;
use reindex_quantization;

create table t(id bigint primary key, v vecf32(3) not null);
insert into t select result, concat('[', result % 31, ',', result % 37, ',', result % 41, ']')
from generate_series(1, 256) g;
create index ix using ivfflat on t(v) lists = 4 op_type 'vector_l2_ops';

alter table t alter reindex ix ivfflat quantization 'float16';
alter table t alter reindex ix ivfflat quantization 'bf16';
alter table t alter reindex ix ivfflat quantization 'int8';
alter table t alter reindex ix ivfflat quantization 'uint8';
alter table t alter reindex ix ivfflat merge;
show create table t;
select id from t order by l2_distance(v, '[7,7,7]') limit 3 by rank with option 'mode=post';

-- The stored quantization is accepted and other options apply.
create table q(id bigint primary key, v vecf32(3) not null);
insert into q select id, v from t;
create index qi using ivfflat on q(v) lists = 4 op_type 'vector_l2_ops' quantization 'int8';
alter table q alter reindex qi ivfflat quantization 'INT8' lists = 2;
show create table q;
select id from q order by l2_distance(v, '[7,7,7]') limit 3 by rank with option 'mode=post';
alter table q alter reindex qi ivfflat quantization 'float16';
show create table q;

drop database reindex_quantization;
