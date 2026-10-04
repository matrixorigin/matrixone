-- Regression for issue #25930: a packed local shuffle scope cannot be reused
-- as a single bucket by a distributed shuffle join.
drop database if exists shuffle_reuse_topology;
create database shuffle_reuse_topology;
use shuffle_reuse_topology;

set @@max_dop = 4;
set session optimizer_hints = "forceOneCN=1";

create table probe_keys (k bigint) cluster by k;
create table build_keys_a (k bigint) cluster by k;
create table build_keys_b (k bigint) cluster by k;

insert into probe_keys
select result * 300000 from generate_series(1, 64) g;
insert into build_keys_a
select result * 300000 from generate_series(1, 32) g;
insert into build_keys_b
select result * 300000 from generate_series(33, 64) g;

select table_cnt from table_stats(
    "shuffle_reuse_topology.probe_keys",
    'patch',
    '{"table_cnt":20000000,"block_number":2048,"accurate_object_number":128,"approx_object_number":128,"ndv_map":{"k":20000000},"min_val_map":{"k":1},"max_val_map":{"k":20000000},"shuffle_range_map":{"k":{"overlap":0.1,"uniform":1,"result":[1,5000000,10000000,15000000,20000000]}}}'
) g;
select table_cnt from table_stats(
    "shuffle_reuse_topology.build_keys_a",
    'patch',
    '{"table_cnt":10000000,"block_number":1024,"accurate_object_number":64,"approx_object_number":64,"ndv_map":{"k":10000000},"min_val_map":{"k":1},"max_val_map":{"k":20000000}}'
) g;
select table_cnt from table_stats(
    "shuffle_reuse_topology.build_keys_b",
    'patch',
    '{"table_cnt":10000000,"block_number":1024,"accurate_object_number":64,"approx_object_number":64,"ndv_map":{"k":10000000},"min_val_map":{"k":1},"max_val_map":{"k":20000000}}'
) g;

select p.k, u.k as build_k
from (
    select distinct k from probe_keys
) p
left join (
    select k from build_keys_a
    union all
    select k from build_keys_b
) u on p.k = u.k
order by p.k, build_k;

set session optimizer_hints = "";
set @@max_dop = 0;
drop database shuffle_reuse_topology;
