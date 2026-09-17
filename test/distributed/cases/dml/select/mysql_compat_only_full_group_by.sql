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

-- Issue #27983 original three-table query: job.source is functionally
-- dependent on the grouped job primary key.  Compare it with the explicit
-- GROUP BY form while retaining fanout, zero-child, unmatched-owner, and NULL
-- owner rows.
create table issue_27983_job(
  job_id bigint primary key,
  source varchar(255),
  owner_id bigint
);
create table issue_27983_cv(
  cv_id bigint primary key,
  job_id bigint
);
create table issue_27983_app_user(
  user_id bigint primary key,
  full_name varchar(255)
);
insert into issue_27983_job values
  (1,'portal',10),(2,'referral',10),(3,'direct',99),(4,'empty',null);
insert into issue_27983_cv values
  (101,1),(102,1),(103,2),(104,3),(105,3),(106,3);
insert into issue_27983_app_user values (10,'Alice'),(20,'Bob');
select job.job_id,job.source,count(cv.cv_id) as cv_count,
       job.owner_id,owner.full_name
from issue_27983_job job
left join issue_27983_cv cv using(job_id)
left join issue_27983_app_user owner on owner.user_id=job.owner_id
group by job.job_id,job.owner_id,owner.full_name
order by job.job_id;
select job.job_id,job.source,count(cv.cv_id) as cv_count,
       job.owner_id,owner.full_name
from issue_27983_job job
left join issue_27983_cv cv using(job_id)
left join issue_27983_app_user owner on owner.user_id=job.owner_id
group by job.job_id,job.source,job.owner_id,owner.full_name
order by job.job_id;

-- Public-SQL boundary coverage for supported equality domains and payloads.
create table issue_27983_payload_edges(
  id int primary key,
  payload varchar(64),
  amount int
);
insert into issue_27983_payload_edges values
  (1,null,1),(2,'',2),(3,repeat('x',64),3);
select id,payload,sum(amount) as total
from issue_27983_payload_edges
group by id
order by id;

create table issue_27983_decimal_key(
  k decimal(18,2) not null,
  payload varchar(20),
  amount int,
  unique key uk_decimal(k)
);
insert into issue_27983_decimal_key values
  (-9999999999999999.99,'min',1),(0.00,'zero',2),(9999999999999999.99,'max',3);
select k,payload,sum(amount) as total
from issue_27983_decimal_key
group by k
order by k;

create table issue_27983_date_key(
  k date not null,
  payload varchar(20),
  unique key uk_date(k)
);
insert into issue_27983_date_key values
  ('1000-01-01','min'),('2024-02-29','leap'),('9999-12-31','max');
select k,payload,count(*) as n
from issue_27983_date_key
group by k
order by k;

create table issue_27983_datetime_key(
  k datetime(6) not null,
  payload varchar(20),
  unique key uk_datetime(k)
);
insert into issue_27983_datetime_key values
  ('1000-01-01 00:00:00.000001','min'),
  ('9999-12-31 23:59:59.999999','max');
select k,payload,count(*) as n
from issue_27983_datetime_key
group by k
order by k;

create table issue_27983_binary_key(
  k varbinary(8) not null,
  payload varchar(20),
  unique key uk_binary(k)
);
insert into issue_27983_binary_key values
  ('A','upper'),('a','lower'),('A ','space');
select k,payload,count(*) as n
from issue_27983_binary_key
group by k
order by hex(k);

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

-- Remaining functional-dependency cases: projection lineage and equality joins.
create table fd_parent(id int primary key, name varchar(20));
create table fd_child(id int primary key, parent_id int, flag int, qty int);
insert into fd_parent values (1,'one'),(2,'two');
insert into fd_child values (10,1,1,5),(11,1,0,7),(12,2,1,11),(13,3,1,13),(14,null,1,17);
select id,name,count(*) as n from (select id,name from fd_parent) d group by id order by id;
with d as (select id,name from fd_parent) select id,name,count(*) as n from d group by id order by id;
create view fd_view as select id,name from fd_parent;
select id,name,count(*) as n from fd_view group by id order by id;
select c.parent_id,p.name,sum(c.qty) as total from fd_child c join fd_parent p on c.parent_id=p.id group by c.parent_id order by c.parent_id;
select c.parent_id,p.name,sum(c.qty) as total from fd_child c,fd_parent p where c.parent_id=p.id group by c.parent_id order by c.parent_id;
select parent_id,p.name,sum(c.qty) as total from fd_child c join (select id as parent_id,name from fd_parent) p using(parent_id) group by parent_id order by parent_id;
select c.parent_id,concat(p.name,'!') as label,sum(c.qty) as total from fd_child c join fd_parent p on c.parent_id=p.id group by c.parent_id having p.name<>'two' order by p.name;

-- The inferred result must equal an explicitly grouped reference, with unmatched rows retained.
select c.parent_id,p.name,sum(c.qty) as total from fd_child c left join fd_parent p on c.parent_id=p.id group by c.parent_id order by c.parent_id is null,c.parent_id;
select c.parent_id,p.name,sum(c.qty) as total from fd_child c left join fd_parent p on c.parent_id=p.id group by c.parent_id,p.name order by c.parent_id is null,c.parent_id;
select c.id,p.name,sum(c.qty) as total from fd_child c left join fd_parent p on c.parent_id=p.id and c.flag=1 group by c.id order by c.id;
select c.id,p.name,sum(c.qty) as total from fd_parent p right join fd_child c on c.parent_id=p.id and c.flag=1 group by c.id order by c.id;
select c.parent_id,p.name,sum(c.qty) from fd_child c left join fd_parent p on c.parent_id=p.id and c.flag=1 group by c.parent_id;
select p.id,c.id,count(*) from fd_child c left join fd_parent p on c.parent_id=p.id group by p.id;
select c.id,p2.name,count(*) as n from fd_child c left join fd_parent p on c.parent_id=p.id left join fd_parent p2 on p.id=p2.id group by c.id order by c.id;
with d as (select id,name from fd_parent) select a.id,b.name,count(*) from d a cross join d b group by a.id;
with d as (select id,name from fd_parent) select a.id,b.name,count(*) as n from d a join d b on a.id=b.id group by a.id order by a.id;

create table fd_nullable(a int not null,b int,c int,payload varchar(20),unique key uk_abc(a,b,c));
insert into fd_nullable values (1,null,1,'n1'),(1,null,1,'n2'),(1,1,null,'n3'),(1,1,null,'n4'),(1,1,1,'good'),(1,2,2,'other');
select a,b,c,payload,count(*) from fd_nullable group by a,b,c;
select a,b,c,payload,count(*) from fd_nullable where b is not null group by a,b,c;
select a,b,c,payload,count(*) as n from fd_nullable where b is not null and c is not null group by a,b,c order by a,b,c;
select a,b,c,payload,count(*) as n from fd_nullable where b is not null and c is not null group by a,b,c,payload order by a,b,c;
select k,payload,count(*) as n from (select k,payload from t_unique_nullable) d where k is not null group by k order by k;
select a,b,c,payload,count(*) from fd_nullable where b is not null or a=1 group by a,b,c;

-- Prepared view lineage must be revalidated after replacement changes row identity.
prepare fd_view_stmt from 'select id,name,count(*) as n from fd_view group by id order by id';
execute fd_view_stmt;
create or replace view fd_view as select parent_id as id,cast(id as char) as name from fd_child;
execute fd_view_stmt;
deallocate prepare fd_view_stmt;

-- MATRIXONE_NATIVE keeps MatrixOne's strict ONLY_FULL_GROUP_BY behavior.
set session sql_mode = 'ONLY_FULL_GROUP_BY,MATRIXONE_NATIVE';
select c.parent_id,p.name,sum(c.qty) from fd_child c join fd_parent p on c.parent_id=p.id group by c.parent_id;
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
