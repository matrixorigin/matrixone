-- IVF search should be correct when the index reader is partitioned across CNs.
drop database if exists vector_ivf_multicn_search;
create database vector_ivf_multicn_search;
use vector_ivf_multicn_search;

set ivf_preload_entries = 0;
set probe_limit = 4;

create table t_int(a bigint primary key, b vecf32(4));
insert into t_int values
(1, '[0,0,0,0]'),(2, '[1,0,0,0]'),(3, '[2,0,0,0]'),(4, '[3,0,0,0]'),
(5, '[4,0,0,0]'),(6, '[5,0,0,0]'),(7, '[6,0,0,0]'),(8, '[7,0,0,0]'),
(9, '[8,0,0,0]'),(10, '[9,0,0,0]'),(11, '[10,0,0,0]'),(12, '[11,0,0,0]'),
(13, '[12,0,0,0]'),(14, '[13,0,0,0]'),(15, '[14,0,0,0]'),(16, '[15,0,0,0]');
create index idx_int_b using ivfflat on t_int(b) lists=4 op_type 'vector_l2_ops';

select group_concat(a order by a) as nearest_ids, count(*) as row_count, count(distinct a) as distinct_count
from (select a from t_int order by l2_distance(b, '[0,0,0,0]') limit 4) s;

select group_concat(a order by a) as exact_ids, count(*) as row_count, count(distinct a) as distinct_count
from (select a from t_int where a in (1,2,3,4,5,6) order by l2_distance(b, '[0,0,0,0]') limit 4) s;

create table t_str(a varchar(8) primary key, b vecf32(4));
insert into t_str values
('p01', '[0,0,0,0]'),('p02', '[1,0,0,0]'),('p03', '[2,0,0,0]'),('p04', '[3,0,0,0]'),
('p05', '[4,0,0,0]'),('p06', '[5,0,0,0]'),('p07', '[6,0,0,0]'),('p08', '[7,0,0,0]'),
('p09', '[8,0,0,0]'),('p10', '[9,0,0,0]'),('p11', '[10,0,0,0]'),('p12', '[11,0,0,0]'),
('p13', '[12,0,0,0]'),('p14', '[13,0,0,0]'),('p15', '[14,0,0,0]'),('p16', '[15,0,0,0]');
create index idx_str_b using ivfflat on t_str(b) lists=4 op_type 'vector_l2_ops';

select group_concat(a order by a) as str_ids, count(*) as row_count, count(distinct a) as distinct_count
from (select a from t_str order by l2_distance(b, '[0,0,0,0]') limit 4) s;

create table t_comp(a int, c varchar(8), b vecf32(4), primary key(a, c));
insert into t_comp values
(1, 'c01', '[0,0,0,0]'),(2, 'c02', '[1,0,0,0]'),(3, 'c03', '[2,0,0,0]'),(4, 'c04', '[3,0,0,0]'),
(5, 'c05', '[4,0,0,0]'),(6, 'c06', '[5,0,0,0]'),(7, 'c07', '[6,0,0,0]'),(8, 'c08', '[7,0,0,0]'),
(9, 'c09', '[8,0,0,0]'),(10, 'c10', '[9,0,0,0]'),(11, 'c11', '[10,0,0,0]'),(12, 'c12', '[11,0,0,0]'),
(13, 'c13', '[12,0,0,0]'),(14, 'c14', '[13,0,0,0]'),(15, 'c15', '[14,0,0,0]'),(16, 'c16', '[15,0,0,0]');
create index idx_comp_b using ivfflat on t_comp(b) lists=4 op_type 'vector_l2_ops';

select group_concat(concat(a, ':', c) order by a) as comp_ids, count(*) as row_count, count(distinct concat(a, ':', c)) as distinct_count
from (select a, c from t_comp order by l2_distance(b, '[0,0,0,0]') limit 4) s;

explain select a from t_int order by l2_distance(b, '[0,0,0,0]') limit 4;

-- @ignore:0
explain analyze select a from t_int order by l2_distance(b, '[0,0,0,0]') limit 4;

drop database vector_ivf_multicn_search;
