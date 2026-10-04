drop database if exists alter_set_default_restrictions;
create database alter_set_default_restrictions;
use alter_set_default_restrictions;

create table t_auto_increment(id bigint auto_increment primary key, v int);
--ERROR 1067 (HY000): Invalid default value for 'id'
alter table t_auto_increment alter column id set default 9;
desc t_auto_increment;
insert into t_auto_increment(v) values (10);
select v from t_auto_increment;

create table t_generated(a int, g int as (a + 1) stored);
--ERROR 20301 (HY000): invalid input: generated column 'g' cannot have a default value
alter table t_generated alter column g set default 9;
desc t_generated;
insert into t_generated(a) values (10);
select * from t_generated;

create table t_normal(a int, v int);
alter table t_normal alter column v set default 9;
desc t_normal;
insert into t_normal(a) values (10);
select * from t_normal;

drop database alter_set_default_restrictions;
