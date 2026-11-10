-- @suite

-- @case
-- @desc: MySQL compatibility cases for native NULL values with ROLLUP and GROUPING()
-- @label:bvt

drop database if exists mysql_compat_null_rollup_grouping;
create database mysql_compat_null_rollup_grouping;
use mysql_compat_null_rollup_grouping;

create table t_rollup (
  region varchar(8),
  product varchar(8),
  amount int
);

insert into t_rollup values
  (null, 'tea', 10),
  (null, 'tea', 20),
  ('east', null, 5),
  ('east', 'coffee', 7),
  ('east', 'tea', null),
  ('west', 'tea', 3);

select region_label, grp_region, product_label, grp_product,
       count_all, count_amount, sum_amount
from (
  select if(grouping(region), 'ROLLUP', if(region is null, 'NULL', region)) as region_label,
         grouping(region) as grp_region,
         if(grouping(product), 'ROLLUP', if(product is null, 'NULL', product)) as product_label,
         grouping(product) as grp_product,
         count(*) as count_all,
         count(amount) as count_amount,
         sum(amount) as sum_amount,
         case when grouping(region) then 3
              when region is null then 1
              when region = 'east' then 0
              else 2 end as sort_region,
         case when grouping(product) then 3
              when product is null then 1
              when product = 'coffee' then 0
              else 2 end as sort_product
  from t_rollup
  group by region, product with rollup
) q
order by sort_region, sort_product;

-- SELECT aliases identify the same detail and subtotal keys as source names.
select r, p, gr, gp, c, s
from (
  select region as r, product as p, grouping(region) as gr,
         grouping(product) as gp, count(*) as c, sum(amount) as s
  from t_rollup
  group by r, p with rollup
) q
order by gr, r, gp, p;

-- Ranking consumes the combined ROLLUP result after alias resolution.
select region as r, sum(amount) as s,
       row_number() over (order by sum(amount) desc) as rn
from t_rollup
 group by r with rollup
order by s desc;

-- Empty-input ROLLUP follows MySQL (#30148); explicit grouping sets keep their identity.
create table t_empty(k int, v int);
select k, sum(v) as total, count(*) as n, grouping(k) as g
from t_empty group by k with rollup;
select region, sum(amount) as total, count(*) as n, grouping(region) as g
from t_rollup where 1=0 group by region with rollup;
with input as (select * from t_empty)
select k, sum(v) as total from input group by rollup(k);
select k, row_number() over(order by k) as rn
from t_empty group by k with rollup;
-- Alias resolution and the empty-input guard both precede window ranking.
select k as x, sum(v) as s, row_number() over(order by sum(v)) as rn
from t_empty group by x with rollup;
select k, count(*) as n from t_empty group by k with rollup
having count(*)=0 or sum(v)>0;
select k, sum(v) as total, count(*) as n, grouping(k) as g
from t_empty group by k with rollup
union all select 99,99,99,0;
create table t_empty_copy as
select k, sum(v) as total, count(*) as n, grouping(k) as g
from t_empty group by k with rollup;
select count(*) as copied_rows from t_empty_copy;
insert into t_empty_copy
select k, sum(v), count(*), grouping(k) from t_empty group by k with rollup;
select count(*) as inserted_rows from t_empty_copy;
select sum(v) as total, count(*) as n from t_empty;
select k, sum(v) as total, count(*) as n, grouping(k) as g
from t_empty group by grouping sets ((k),());
insert into t_empty values(null,null);
select k, grouping(k) as g, count(*) as n, count(v) as count_v
from t_empty group by k with rollup order by grouping(k);
delete from t_empty;
select k, count(*) as n from t_empty group by k with rollup;

-- SAMPLE already suppresses empty input and must not acquire an ordinary aggregate.
select k, sample(v, 1 rows) as sampled from t_empty group by k with rollup;
select k, sample(v, 100 percent) as sampled from t_empty group by rollup(k);
insert into t_empty values(1,10);
select k, sample(v, 1 rows) as sampled from t_empty group by k with rollup;
select k, sample(v, 100 percent) as sampled from t_empty group by rollup(k);
select k, sample(v, 1 rows) as sampled from t_empty where 1=0 group by rollup(k);
delete from t_empty;

drop database mysql_compat_null_rollup_grouping;
