-- @suite

-- @case
-- @desc: MySQL ONLY_FULL_GROUP_BY exceptions and MATRIXONE_NATIVE strict mode
-- @label:bvt

drop database if exists mysql_compat_only_full_group_by;
create database mysql_compat_only_full_group_by;
use mysql_compat_only_full_group_by;

set session sql_mode = 'ONLY_FULL_GROUP_BY';

create table t_sales(region varchar(10), product varchar(10), qty int);
insert into t_sales values
  ('east', 'phone', 2),
  ('east', 'phone', null),
  ('west', 'phone', 3),
  (null, 'phone', 5);

-- product is restricted to one value by WHERE.
select region, product, sum(qty) as s_qty
from t_sales
where product = 'phone'
group by region
order by region is null, region;

create table t_fd(id int primary key, name varchar(20), amount int);
insert into t_fd values (1, 'a', 10), (2, 'b', 20), (3, 'c', 30);

-- name is functionally dependent on the grouped primary key.
select id, name, sum(amount)
from t_fd
group by id
order by id;

-- A WHERE-single-valued column remains available to window arguments,
-- partition keys, and order keys after the aggregate stage.
select region,
       first_value(product) over (partition by product order by product) as first_product,
       sum(qty) as s_qty
from t_sales
where product = 'phone'
group by region
order by region is null, region;

-- Keep the original no-specification window argument form covered as well.
select region,
       first_value(product) over () as first_product,
       sum(qty) as s_qty
from t_sales
where product = 'phone'
group by region
order by region is null, region;

-- A column functionally dependent on the grouped primary key is likewise
-- materialized before its window argument, partition key, and order key.
select id,
       first_value(name) over (partition by name order by name) as first_name,
       sum(amount) as s_amount
from t_fd
group by id
order by id;

-- MATRIXONE_NATIVE keeps MatrixOne's strict ONLY_FULL_GROUP_BY behavior.
set session sql_mode = 'ONLY_FULL_GROUP_BY,MATRIXONE_NATIVE';
select region, product, sum(qty) as s_qty
from t_sales
where product = 'phone'
group by region
order by region is null, region;
select id, name, sum(amount)
from t_fd
group by id
order by id;

set session sql_mode = '';
drop database mysql_compat_only_full_group_by;
