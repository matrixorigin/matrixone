-- @suit
-- @case
-- @desc: COPY ALTER preserves historical data, views, allocator and foreign keys across publication and rollback
-- @label:bvt
-- ALGORITHM=COPY is existing ALTER TABLE syntax; ADD/DROP PRIMARY KEY exercises the COPY ALTER path under test.
drop database if exists alter_copy_publication;
create database alter_copy_publication;
use alter_copy_publication;
create table parent_t(id int primary key);
insert into parent_t values(1),(2);
create table t(id int auto_increment, parent_id int, v varchar(20), unique key uk(id), constraint fk foreign key(parent_id) references parent_t(id));
insert into t(parent_id,v) values(1,'first'),(2,'second');
create view v_t as select id,parent_id,v from t;
create snapshot alter_copy_publication_snap for database alter_copy_publication;
alter table t algorithm=copy, add primary key(id);
select * from v_t order by id;
insert into t(parent_id,v) values(1,'third');
select id > 2 as generated_id_valid from t where v='third';
select count(*) as row_count,count(distinct id) as unique_ids from t;
select parent_id,v from t order by id;
select * from t {snapshot='alter_copy_publication_snap'} order by id;
alter table t algorithm=copy, drop primary key;
begin;
insert into t(parent_id,v) values(2,'rollback');
alter table t algorithm=copy, add primary key(id);
rollback;
select parent_id,v from v_t order by id;
select count(*) as primary_columns from information_schema.columns where table_schema='alter_copy_publication' and table_name='t' and column_key='PRI';
alter table t algorithm=copy, add primary key(id);
select parent_id,v from t order by id;
select count(*) as foreign_keys from information_schema.table_constraints where table_schema='alter_copy_publication' and table_name='t' and constraint_type='FOREIGN KEY';
drop snapshot alter_copy_publication_snap;
drop view v_t;
drop table t;
drop table parent_t;
drop database alter_copy_publication;
