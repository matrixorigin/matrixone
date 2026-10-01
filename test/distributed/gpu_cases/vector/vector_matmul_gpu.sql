-- =====================================================================
-- vector_matmul_gpu.sql — vector_matmul on cuBLASLt: gpu_mode on vs off
--
-- GPU REQUIRED. The same vector_matmul queries run under gpu_mode = 1 (cuBLASLt
-- block-scaled matmul) and gpu_mode = 0 (CPU kernel). The vecf8 vectors hold small
-- integers and every vecf4 vector peaks at 1, values exact in MXFP8 and NVFP4, so both
-- modes return identical ids and scores (in general they agree within fp32 rounding). The
-- 400,000-row vecf8 table is scored in GPU tiles by parallel pipelines and checked
-- against ORDER BY inner_product ... LIMIT.
-- =====================================================================

drop database if exists vector_matmul_gpu;
create database vector_matmul_gpu;
use vector_matmul_gpu;

create table t (id int primary key, cat varchar(10), a vecf8(4), b vecf4(4));
insert into t values
  (1, 'x', '[1,0,0,0]', '[1,0,0,0]'),
  (2, 'x', '[0,1,0,0]', '[0,1,0,0]'),
  (3, 'y', '[1,1,0,0]', '[1,1,0,0]'),
  (4, 'y', '[-1,0,0,0]', '[-1,0,0,0]'),
  (5, 'x', null, null),
  (6, 'y', '[2,-3,4,6]', '[0.5,-1,1,1]');

create table big (id bigint primary key, a vecf8(4));
insert into big
select g.result,
       cast(concat('[', g.result % 7 - 3, ',', g.result % 11 - 5, ',', g.result % 13 - 6, ',', g.result % 5 - 2, ']') as vecf8(4))
from generate_series(1, 400000) g;

create table qv (qid int primary key, v vecf32(4));
insert into qv values (1, '[1,0,0,0]'), (2, '[0,0,1,1]');
set @qs = (select json_arrayagg(v) from qv);

-- ---- gpu_mode = 1 (cuBLASLt) ----
SET gpu_mode = 1;
select vector_matmul(2, id, a, '[[1,0,0,0],[0,1,0,0]]') from t;
select vector_matmul(10, id, b, '[[1,1,0,0],[1,-0.5,0.5,1]]') from t;
select cat, vector_matmul(1, id, a, '[[1,0,0,0]]') r from t group by cat order by cat;
select vector_matmul(1, id, b, @qs) from t;
with m as (select vector_matmul(20, id, a, '[[1,2,3,4]]') r from big),
     got as (select json_unquote(json_extract(h.value, '$[0]')) as id, h.`index` as rnk
             from m cross apply unnest(m.r, '$[0]') h),
     want as (select cast(id as varchar) as id, row_number() over (order by s desc, cast(id as varchar)) - 1 as rnk
              from (select id, -inner_product(a, cast('[1,2,3,4]' as vecf8(4))) s from big
                    order by s desc, cast(id as varchar) limit 20) r)
select (select count(*) from got) as got_rows,
       (select count(*) from (select id, rnk from got except select id, rnk from want) d) as mismatches;

-- ---- gpu_mode = 0 (CPU) — identical results ----
SET gpu_mode = 0;
select vector_matmul(2, id, a, '[[1,0,0,0],[0,1,0,0]]') from t;
select vector_matmul(10, id, b, '[[1,1,0,0],[1,-0.5,0.5,1]]') from t;
select cat, vector_matmul(1, id, a, '[[1,0,0,0]]') r from t group by cat order by cat;
select vector_matmul(1, id, b, @qs) from t;
with m as (select vector_matmul(20, id, a, '[[1,2,3,4]]') r from big),
     got as (select json_unquote(json_extract(h.value, '$[0]')) as id, h.`index` as rnk
             from m cross apply unnest(m.r, '$[0]') h),
     want as (select cast(id as varchar) as id, row_number() over (order by s desc, cast(id as varchar)) - 1 as rnk
              from (select id, -inner_product(a, cast('[1,2,3,4]' as vecf8(4))) s from big
                    order by s desc, cast(id as varchar) limit 20) r)
select (select count(*) from got) as got_rows,
       (select count(*) from (select id, rnk from got except select id, rnk from want) d) as mismatches;

SET gpu_mode = 1;
drop database vector_matmul_gpu;
