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

-- Complete declared NOT NULL UNIQUE keys determine their own table's columns.
create table t_unique(k int not null, payload varchar(20), amount int, unique key uk_k(k));
insert into t_unique values (1, 'alpha', 10), (2, 'beta', 20);
select k, payload, sum(amount) as total from t_unique group by k order by k;
select k, payload, sum(amount) as total from t_unique group by k, payload order by k;
select k, concat(payload, '!') as label, sum(amount) as total from t_unique group by k having payload <> '' order by payload;

create table t_unique_composite(a int not null, b int not null, payload varchar(20), unique key uk_ab(a,b));
insert into t_unique_composite values (1,1,'x'),(1,2,'y'),(2,1,'z');
select a,b,payload,count(*) as n from t_unique_composite group by b,a order by a,b;
select a,payload,count(*) from t_unique_composite group by a;
select payload,count(*) from t_unique_composite group by a+1,b;

-- Join fanout does not change a same-binding dependency or justify cross-binding inference.
create table t_unique_fan(k int, v int);
insert into t_unique_fan values (1,7),(1,8),(2,9);
select u.k,u.payload,count(*) as n from t_unique u join t_unique_fan f on u.k=f.k group by u.k order by u.k;
select u.k,u.payload,count(*) as n from t_unique u join t_unique_fan f on u.k=f.k group by u.k,u.payload order by u.k;
select u.k,f.v,count(*) from t_unique u join t_unique_fan f on u.k=f.k group by u.k;

create table t_unique_nullable(k int, payload varchar(20), unique key uk_nullable(k));
insert into t_unique_nullable values (null,'x'),(null,'y'),(1,'z');
select k,payload,count(*) from t_unique_nullable group by k;
select k,payload,count(*) from t_unique_nullable where k is not null group by k;
select k,count(*) as n from t_unique_nullable group by k order by k is null,k;
select k,payload,count(*) from t_unique group by k with rollup;

-- Prepared metadata must not retain a proof after its constraint is dropped.
prepare unique_fd from 'select k,payload,sum(amount) as total from t_unique group by k order by k';
execute unique_fd;
alter table t_unique drop index uk_k;
insert into t_unique values (1,'different',30);
execute unique_fd;
deallocate prepare unique_fd;
select k,payload,sum(amount) as total from t_unique group by k,payload order by k,payload;

-- MATRIXONE_NATIVE keeps MatrixOne's strict ONLY_FULL_GROUP_BY behavior.
set session sql_mode = 'ONLY_FULL_GROUP_BY,MATRIXONE_NATIVE';
select a,b,payload,count(*) from t_unique_composite group by a,b;
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
