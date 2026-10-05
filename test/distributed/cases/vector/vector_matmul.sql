-- #20567: vector_matmul(topk, id, vec, queries [, options]), the top-k nearest-rows aggregate (inner product, cosine or squared L2).
drop database if exists vector_matmul_db;
create database vector_matmul_db;
use vector_matmul_db;

create table t (id int primary key, cat varchar(10), a vecf8(4), b vecf4(4));
insert into t values
  (1, 'x', '[1,0,0,0]', '[1,0,0,0]'),
  (2, 'x', '[0,1,0,0]', '[0,1,0,0]'),
  (3, 'y', '[1,1,0,0]', '[1,1,0,0]'),
  (4, 'y', '[-1,0,0,0]', '[-1,0,0,0]'),
  (5, 'x', null, null);

-- one result per query; ties ordered by id text
select vector_matmul(2, id, a, '[[1,0,0,0],[0,1,0,0]]') from t;
select vector_matmul(10, id, b, '[[1,1,0,0]]') from t;
-- pre-filter, grouping, empty input
select vector_matmul(3, id, a, '[[1,1,0,0]]', '{"mode":"cpu"}') from t where cat = 'x';
select cat, vector_matmul(1, id, a, '[[1,0,0,0]]') r from t group by cat order by cat;
select vector_matmul(2, id, a, '[[1,0,0,0],[0,0,1,0]]', '{"mode":"auto","tile_bytes":1048576}') from t where id > 100;
-- string and uuid ids
select vector_matmul(2, cat, a, '[[1,0,0,0]]') from t;
create table u (k uuid primary key, a vecf8(2));
insert into u values ('6ba7b810-9dad-11d1-80b4-00c04fd430c8', '[1,2]'), ('6ba7b811-9dad-11d1-80b4-00c04fd430c8', '[2,1]');
select vector_matmul(2, k, a, '[[1,0],[0,1]]') from u;
-- relational form of the result
select q.`index` as q_id, h.`index` as rnk,
       json_unquote(json_extract(h.value, '$[0]')) as src_id,
       cast(json_extract(h.value, '$[1]') as double) as score
from (select vector_matmul(2, id, a, '[[1,0,0,0],[0,1,0,0]]') as r from t) m
     cross apply unnest(m.r, '$') q
     cross apply unnest(q.value, '$') h
order by q_id, rnk;

-- prepared statement
prepare s from 'select vector_matmul(?, id, b, ?) from t';
set @p = 1;
set @q = '[[0,1,0,0]]';
execute s using @p, @q;
deallocate prepare s;

-- queries from a table through a user variable
create table qv (qid int primary key, v vecf32(4));
insert into qv values (1, '[1,0,0,0]'), (2, '[0,0,1,1]');
set @qs = (select json_arrayagg(v) from qv);
select vector_matmul(1, id, a, @qs) from t;

-- options: a JSON object whose "metric" is inner_product (the default), cosine or l2sq;
-- other keys are ignored; the scores are distances, nearest first: -dot, 1 - cos, |x - q|^2
select vector_matmul(2, id, a, '[[1,0,0,0]]', '{"metric":"inner_product","x":1}') from t;
select vector_matmul(2, id, a, '[[1,0,0,0]]', 'not json') from t;
select vector_matmul(2, id, a, '[[1,0,0,0]]', '{"metric":"l2"}') from t;
create table mt (id int, v vecf32(3));
insert into mt values (1, '[1,2,2]'), (2, '[0,0,0]'), (3, '[3,0,4]'), (4, '[1,1,1]');
select vector_matmul(4, id, v, '[[1,2,2],[0,1,0]]') from mt;
select id, inner_product(v, '[1,2,2]') d from mt order by d, id;
select vector_matmul(4, id, v, '[[1,2,2],[0,1,0]]', '{"metric":"cosine"}') from mt;
select id, cosine_distance(v, '[1,2,2]') d from mt order by d, id;
select vector_matmul(4, id, v, '[[1,2,2],[0,1,0]]', '{"metric":"l2sq"}') from mt;
select id, l2_distance_sq(v, '[1,2,2]') d from mt order by d, id;

-- errors
select vector_matmul('abc', id, a, '[[1,0,0,0]]') from t;
select vector_matmul(0, id, a, '[[1,0,0,0]]') from t;
select vector_matmul(2, id, a, '[[1,0,0]]') from t;
select vector_matmul(2, id, a, '[]') from t;
select vector_matmul(id, id, a, '[[1,0,0,0]]') from t;
select vector_matmul(2, id, a, cat) from t;
select vector_matmul(2, id, a) from t;
select vector_matmul(2, id, a, '[[1,0,0,0]]', '{}', 1) from t;
select vector_matmul(2, id, a, '[[1,0,0,0]]', cat) from t;
create table f (id int primary key, v vecf64(4));
select vector_matmul(2, id, v, '[[1,0,0,0]]') from f;

-- 400,000 rows, scanned in parallel: the ids and ranks equal ORDER BY inner_product ... LIMIT
create table big (id bigint primary key, a vecf8(4), b vecf4(4));
insert into big
select g.result,
       cast(concat('[', g.result % 7 - 3, ',', g.result % 11 - 5, ',', g.result % 13 - 6, ',', g.result % 5 - 2, ']') as vecf8(4)),
       cast(concat('[', g.result % 7 - 3, ',', g.result % 11 - 5, ',', g.result % 13 - 6, ',', g.result % 5 - 2, ']') as vecf4(4))
from generate_series(1, 400000) g;
select count(*) from big;
with m as (select vector_matmul(20, id, a, '[[1,2,3,4]]') r from big),
     got as (select json_unquote(json_extract(h.value, '$[0]')) as id, h.`index` as rnk
             from m cross apply unnest(m.r, '$[0]') h),
     want as (select cast(id as varchar) as id, row_number() over (order by s desc, cast(id as varchar)) - 1 as rnk
              from (select id, -inner_product(a, cast('[1,2,3,4]' as vecf8(4))) s from big
                    order by s desc, cast(id as varchar) limit 20) r)
select (select count(*) from got) as got_rows,
       (select count(*) from (select id, rnk from got except select id, rnk from want) d) as mismatches;
with m as (select vector_matmul(20, id, b, '[[-2,1,0,3]]') r from big),
     got as (select json_unquote(json_extract(h.value, '$[0]')) as id, h.`index` as rnk
             from m cross apply unnest(m.r, '$[0]') h),
     want as (select cast(id as varchar) as id, row_number() over (order by s desc, cast(id as varchar)) - 1 as rnk
              from (select id, -inner_product(b, cast('[-2,1,0,3]' as vecf4(4))) s from big
                    order by s desc, cast(id as varchar) limit 20) r)
select (select count(*) from got) as got_rows,
       (select count(*) from (select id, rnk from got except select id, rnk from want) d) as mismatches;

-- queries as a BLOB of little-endian float32 values give the JSON result; the CPU
-- kernel (the cuBLASLt result is in gpu_cases/vector/vector_matmul_gpu.sql)
set gpu_mode = 0;
create table bq (id int, v vecf4(4), w vecf32(4));
insert into bq values (1, '[1,0,0,0]', '[1,0,0,0]'), (2, '[0,1,0,0]', '[0,1,0,0]'), (3, '[2,2,0,0]', '[2,2,0,0]');
select vector_matmul(2, id, w, '[[1,0,-0.5,2],[0,3,0,-1]]') from bq;
select vector_matmul(2, id, w, cast(unhex('0000803F00000000000000BF00000040000000000000404000000000000080BF') as blob)) from bq;
select vector_matmul(2, id, v, cast(unhex('0000803F00000000000000BF00000040000000000000404000000000000080BF') as blob)) from bq;
set @bq = cast(unhex('0000803F00000000000000BF00000040000000000000404000000000000080BF') as blob);
prepare pbq from 'select vector_matmul(2, id, w, cast(? as blob)) from bq';
execute pbq using @bq;
deallocate prepare pbq;
select vector_matmul(2, id, w, cast(unhex('0000803F000000000000') as blob)) from bq;
select vector_matmul(2, id, w, 'x', cast(unhex('00') as blob)) from bq;

-- query_format vecblock: the queries are vecf8/vecf4 cells, a BLOB back to back
-- (vecblock_binary) or a JSON array of vecblock JSON objects, used without quantization
create table vq (id int, v vecf4(17), w vecf32(17));
insert into vq values
  (1, '[8.7649145,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,5.7432985]', '[8.7649145,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,5.7432985]'),
  (2, '[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]', '[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]'),
  (3, '[8,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,6]', '[8,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,6]');
set @qc = (select vecblock_binary(v) from vq where id = 1);
set @qc2 = (select concat(vecblock_binary(v), (select vecblock_binary(v) from vq where id = 2)) from vq where id = 1);
set @qj = (select concat('[', group_concat(vecblock_json(v) order by id), ']') from vq where id in (1, 2));
select vector_matmul(2, id, v, @qc, '{"query_format":"vecblock","metric":"l2sq"}') from vq;
select vector_matmul(1, id, v, @qc2, '{"query_format":"vecblock","metric":"l2sq"}') from vq;
select vector_matmul(1, id, v, @qj, '{"query_format":"vecblock","metric":"l2sq"}') from vq;
-- the displayed (decoded) values of row 1 quantize to another cell: not at distance 0
select vector_matmul(2, id, v, '[[8.764914,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,6.2606535]]', '{"metric":"l2sq"}') from vq;
select vector_matmul(1, id, v, @qc, '{"query_format":"float32"}') from vq;
select vector_matmul(1, id, w, @qc, '{"query_format":"vecblock"}') from vq;
select vector_matmul(1, id, v, '[[1,2,3]]', '{"query_format":"vecblock"}') from vq;
select vector_matmul(1, id, v, @qj, '{"query_format":"cells"}') from vq;
prepare pvq from 'select vector_matmul(1, id, v, ?, ''{"query_format":"vecblock","metric":"l2sq"}'') from vq';
execute pvq using @qc;
deallocate prepare pvq;

drop database vector_matmul_db;
