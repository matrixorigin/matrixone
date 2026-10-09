-- @suit
-- @case
-- @desc: Single-expression functional index lifecycle and metadata.
-- @label:bvt
drop database if exists functional_index_bvt;
create database functional_index_bvt;
use functional_index_bvt;
create table t (id int primary key, name varchar(40), index il ((lower(name))));
insert into t values (1,'ABC'),(2,'def'),(3,NULL);
select count(*) as matched from t force index(il) where lower(name)='abc';
update t set name='aBc' where id=2;
select count(*) as matched from t force index(il) where lower(name)='abc';
replace into t values (2,'XYZ');
select count(*) as matched from t force index(il) where lower(name)='abc';
insert into t values (2,'ABC') on duplicate key update name=values(name);
select count(*) as matched from t force index(il) where lower(name)='abc';
create index ip on t ((id+1));
select id from t force index(ip) where id+1=3;
alter table t add column extra int first;
alter table t modify name varchar(60);
select count(*) as matched from t force index(il) where lower(name)='abc';
begin;
delete from t where id=1;
rollback;
select count(*) as matched from t force index(il) where lower(name)='abc';
create table cloned like t;
insert into cloned(id,name) select id,name from t;
select count(*) as matched from cloned force index(il) where lower(name)='abc';
select index_name,column_name is null as functional,expression is not null as has_expression from information_schema.statistics where table_schema='functional_index_bvt' and table_name='t' order by index_name;
drop index il on t;
drop index ip on t;
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_index_bvt' and att_relname='t' and attr_has_generated=1;
select id,name from t order by id;
create table load_fi(id int primary key,name varchar(40),index il((lower(name))));
load data inline format='csv', data='1,ABC\n2,abc\n' into table load_fi fields terminated by ',' (id,name);
select count(*) as matched from load_fi force index(il) where lower(name)='abc';
create table multi_fi(id int primary key,tenant int,name varchar(40),index ix((lower(name)),(id+1)),index im(tenant,(lower(name))));
insert into multi_fi values(1,7,'ABC'),(2,7,'abc'),(3,7,NULL);
select count(*) as matched from multi_fi force index(ix) where lower(name)='abc' and id+1=2;
select count(*) as matched from multi_fi force index(im) where tenant=7 and lower(name)='abc';
drop index ix on multi_fi;
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_index_bvt' and att_relname='multi_fi' and attr_has_generated=1;
drop index im on multi_fi;
select count(*) as hidden_generated from mo_catalog.mo_columns where att_database='functional_index_bvt' and att_relname='multi_fi' and attr_has_generated=1;
drop database functional_index_bvt;
