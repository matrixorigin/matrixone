-- Multi-table INSERT (Snowflake style): INSERT ALL / INSERT FIRST ... INTO ... SELECT
drop database if exists multi_insert_db;
create database multi_insert_db;
use multi_insert_db;

create table customers (id int primary key, name varchar(50), region varchar(10), score int);
insert into customers values (1,'alice','EU',10),(2,'bob','US',20),(3,'carol','APAC',30),(4,'dave','EU',40),(5,'erin',NULL,50);

-- targets with different schemas and index shapes
create table customers_eu (id int primary key, name varchar(50), region varchar(10), unique key uk_name(name), key ik_region(region));
create table customers_us (id int primary key, name varchar(50), region varchar(10), key ik_region(region));
create table customers_other (id int primary key, name varchar(50), region varchar(10));
create table audit_log (seq int auto_increment primary key, cust_id int, note varchar(100) default 'n/a');

-- unconditional INSERT ALL: every row to every target; positional and explicit column lists
insert all
  into customers_other
  into audit_log (cust_id, note) values (id, concat('copied ', name))
select id, name, region from customers;
select * from customers_other order by id;
select * from audit_log order by seq;

-- conditional INSERT ALL with ELSE: a NULL condition never matches, so the NULL region goes to ELSE
delete from customers_other;
insert all
  when region = 'EU' then into customers_eu (id, name, region) values (id, name, region)
  when region = 'US' then into customers_us (id, name, region) values (id, name, region)
  else into customers_other (id, name, region) values (id, name, region)
select id, name, region from customers;
select 'eu' as t, id, name, region from customers_eu order by id;
select 'us' as t, id, name, region from customers_us order by id;
select 'other' as t, id, name, region from customers_other order by id;

-- INSERT ALL: a row matching several WHENs goes to all of them
delete from customers_eu; delete from customers_us; delete from customers_other;
insert all
  when score >= 20 then into customers_us (id, name, region) values (id, name, region)
  when score >= 40 then into customers_eu (id, name, region) values (id, name, region)
  else into customers_other (id, name, region) values (id, name, region)
select id, name, region, score from customers;
select 'us' as t, id from customers_us order by id;
select 'eu' as t, id from customers_eu order by id;
select 'other' as t, id from customers_other order by id;

-- INSERT FIRST: only the first matching WHEN; ELSE for rows matching none
delete from customers_eu; delete from customers_us; delete from customers_other;
insert first
  when score >= 20 then into customers_us (id, name, region) values (id, name, region)
  when score >= 40 then into customers_eu (id, name, region) values (id, name, region)
  else into customers_other (id, name, region) values (id, name, region)
select id, name, region, score from customers;
select 'us' as t, id from customers_us order by id;
select 'eu' as t, id from customers_eu order by id;
select 'other' as t, id from customers_other order by id;

-- several INTO clauses under one WHEN, expressions in VALUES, WITH clause, ORDER BY / LIMIT in source
delete from customers_eu; delete from customers_us; delete from customers_other; delete from audit_log;
with src as (select id, name, region, score from customers where region is not null)
insert first
  when score < 25 then into customers_us (id, name, region) values (id, upper(name), region)
                       into audit_log (cust_id, note) values (id, 'small')
  else into customers_eu (id, name, region) values (id * 100, name, lower(region))
select id, name, region, score from src order by score desc limit 3;
select * from customers_us order by id;
select * from customers_eu order by id;
select cust_id, note from audit_log order by cust_id;

-- constraints are enforced per target: duplicate primary key
insert all into customers_other (id, name, region) values (id, name, region) select id, name, region from customers where id = 3;
insert all into customers_other (id, name, region) values (id, name, region) select id, name, region from customers where id = 3;
-- duplicate unique key in a target
delete from customers_eu;
insert into customers_eu values (100, 'alice', 'EU');
insert all into customers_eu (id, name, region) values (id, name, region) select id, name, region from customers where id = 1;
select count(*) from customers_eu;
-- a failing target rolls back the whole statement
delete from customers_other;
insert all
  into customers_other (id, name, region) values (id, name, region)
  into customers_eu (id, name, region) values (id, name, region)
select id, name, region from customers where id in (1, 2);
select count(*) from customers_other;

-- the same table in several INTO clauses: one write pipeline, so duplicate keys across clauses are rejected
create table wide (id int primary key, lo int, hi int);
insert into wide values (1, 10, 100), (2, 20, 200);
create table narrow (id int primary key, val int, tag varchar(10) default 'none');
insert all
  into narrow (id, val, tag) values (id, lo, 'lo')
  into narrow (id, val) values (id + 1000, hi)
select id, lo, hi from wide;
select * from narrow order by id;
insert all into narrow (id, val) values (id + 5000, lo) into narrow (id, val) values (id + 5000, hi) select id, lo, hi from wide;
select count(*) from narrow where id > 5000;
-- FIRST with the same table in both branches routes each row exactly once
insert first
  when lo < 15 then into narrow (id, val, tag) values (id + 2000, lo, 'small')
  else into narrow (id, val, tag) values (id + 2000, hi, 'big')
select id, lo, hi from wide;
select * from narrow where id > 2000 order by id;
-- auto-increment column set by one clause and left to the engine by another
create table seqs (seq int auto_increment primary key, val int);
insert all into seqs (seq, val) values (id * 100, lo) into seqs (val) values (hi) select id, lo, hi from wide;
select * from seqs order by seq;

-- targets with fulltext and ivfflat indexes get their index maintenance, including for merged clauses
create table docs (id int primary key, body varchar(200), fulltext (body));
create table vecs (id int primary key, e vecf32(3));
create index iv using ivfflat on vecs(e) lists=1 op_type "vector_l2_ops";
insert all
  into docs (id, body) values (id, concat('hello ', name))
  into docs (id, body) values (id + 10, concat('bye ', name))
  into vecs (id, e) values (id, cast(concat('[', score, ',', score, ',', score, ']') as vecf32(3)))
select id, name, score from customers where id <= 2;
select id from docs where match(body) against('bob' in natural language mode) order by id;
select id from docs where match(body) against('bye' in natural language mode) order by id;
select id, e from vecs order by id;

-- explicit transaction
begin;
insert all into customers_other (id, name, region) values (id, name, region) into customers_us (id, name, region) values (id, name, region) select id, name, region from customers where id = 5;
select count(*) from customers_other;
rollback;
select count(*) from customers_other;

-- errors
insert all into customers_other (id, name) values (id) select id, name from customers;
insert all into customers_other select id, name from customers;
insert all into customers_other (id, name, region) values (id, name, nosuch) select id, name, region from customers;
insert all when nosuch > 1 then into customers_other select id, name, region from customers;
insert all into customers_other (id, name, nosuch) values (id, name, region) select id, name, region from customers;
insert all into no_such_table select id from customers;
insert first into customers_other select id, name, region from customers;
-- foreign keys are not supported
create table parent (id int primary key);
create table child (id int primary key, pid int, foreign key (pid) references parent(id));
insert all into child (id, pid) values (id, id) select id from customers;
-- external tables are not supported
create external table ext_t (a int) url s3option{"endpoint"='http://127.0.0.1:9000', "access_key_id"='x', "secret_access_key"='y', "bucket"='b', "filepath"='f.csv'};
insert all into ext_t (a) values (id) select id from customers;

-- explain shows one sink feeding one write pipeline per target (targets without unique indexes, so no generated index table names appear)
-- @separator:table
explain insert all
  when region = 'US' then into customers_us (id, name, region) values (id, name, region)
  else into customers_other (id, name, region) values (id, name, region)
select id, name, region from customers;

drop database multi_insert_db;
