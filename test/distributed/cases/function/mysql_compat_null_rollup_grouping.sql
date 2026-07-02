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

drop database mysql_compat_null_rollup_grouping;
