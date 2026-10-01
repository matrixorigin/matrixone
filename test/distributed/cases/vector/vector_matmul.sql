-- #20567: vector_matmul(params, id, vec, queries), the top-k dot-product aggregate over vecf8/vecf4.
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
select vector_matmul('{"limit":2}', id, a, '[[1,0,0,0],[0,1,0,0]]') from t;
select vector_matmul('{"limit":10}', id, b, '[[1,1,0,0]]') from t;
-- pre-filter, grouping, empty input
select vector_matmul('{"limit":3,"mode":"cpu"}', id, a, '[[1,1,0,0]]') from t where cat = 'x';
select cat, vector_matmul('{"limit":1}', id, a, '[[1,0,0,0]]') r from t group by cat order by cat;
select vector_matmul('{"limit":2,"mode":"auto"}', id, a, '[[1,0,0,0],[0,0,1,0]]') from t where id > 100;
-- string and uuid ids
select vector_matmul('{"limit":2}', cat, a, '[[1,0,0,0]]') from t;
create table u (k uuid primary key, a vecf8(2));
insert into u values ('6ba7b810-9dad-11d1-80b4-00c04fd430c8', '[1,2]'), ('6ba7b811-9dad-11d1-80b4-00c04fd430c8', '[2,1]');
select vector_matmul('{"limit":2}', k, a, '[[1,0],[0,1]]') from u;
-- relational form of the result
select q.`index` as q_id, h.`index` as rnk,
       json_unquote(json_extract(h.value, '$[0]')) as src_id,
       cast(json_extract(h.value, '$[1]') as double) as score
from (select vector_matmul('{"limit":2}', id, a, '[[1,0,0,0],[0,1,0,0]]') as r from t) m
     cross apply unnest(m.r, '$') q
     cross apply unnest(q.value, '$') h
order by q_id, rnk;

-- prepared statement
prepare s from 'select vector_matmul(?, id, b, ?) from t';
set @p = '{"limit":1}';
set @q = '[[0,1,0,0]]';
execute s using @p, @q;
deallocate prepare s;

-- errors
select vector_matmul('{}', id, a, '[[1,0,0,0]]') from t;
select vector_matmul('{"limit":0}', id, a, '[[1,0,0,0]]') from t;
select vector_matmul('{"limit":2,"bogus":1}', id, a, '[[1,0,0,0]]') from t;
select vector_matmul('{"limit":2,"mode":"gpu"}', id, a, '[[1,0,0,0]]') from t;
select vector_matmul('{"limit":2}', id, a, '[[1,0,0]]') from t;
select vector_matmul('{"limit":2}', id, a, '[]') from t;
select vector_matmul(cat, id, a, '[[1,0,0,0]]') from t;
select vector_matmul('{"limit":2}', id, a, cat) from t;
select vector_matmul('{"limit":2}', id, a) from t;
create table f (id int primary key, v vecf32(4));
select vector_matmul('{"limit":2}', id, v, '[[1,0,0,0]]') from f;

-- 400,000 rows, scanned in parallel: the ids and ranks equal ORDER BY inner_product ... LIMIT
create table big (id bigint primary key, a vecf8(4), b vecf4(4));
insert into big
select g.result,
       cast(concat('[', g.result % 7 - 3, ',', g.result % 11 - 5, ',', g.result % 13 - 6, ',', g.result % 5 - 2, ']') as vecf8(4)),
       cast(concat('[', g.result % 7 - 3, ',', g.result % 11 - 5, ',', g.result % 13 - 6, ',', g.result % 5 - 2, ']') as vecf4(4))
from generate_series(1, 400000) g;
select count(*) from big;
with m as (select vector_matmul('{"limit":20}', id, a, '[[1,2,3,4]]') r from big),
     got as (select json_unquote(json_extract(h.value, '$[0]')) as id, h.`index` as rnk
             from m cross apply unnest(m.r, '$[0]') h),
     want as (select cast(id as varchar) as id, row_number() over (order by s desc, cast(id as varchar)) - 1 as rnk
              from (select id, -inner_product(a, cast('[1,2,3,4]' as vecf8(4))) s from big
                    order by s desc, cast(id as varchar) limit 20) r)
select (select count(*) from got) as got_rows,
       (select count(*) from (select id, rnk from got except select id, rnk from want) d) as mismatches;
with m as (select vector_matmul('{"limit":20}', id, b, '[[-2,1,0,3]]') r from big),
     got as (select json_unquote(json_extract(h.value, '$[0]')) as id, h.`index` as rnk
             from m cross apply unnest(m.r, '$[0]') h),
     want as (select cast(id as varchar) as id, row_number() over (order by s desc, cast(id as varchar)) - 1 as rnk
              from (select id, -inner_product(b, cast('[-2,1,0,3]' as vecf4(4))) s from big
                    order by s desc, cast(id as varchar) limit 20) r)
select (select count(*) from got) as got_rows,
       (select count(*) from (select id, rnk from got except select id, rnk from want) d) as mismatches;

drop database vector_matmul_db;
