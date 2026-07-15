drop database if exists alter_table_expression_default;
create database alter_table_expression_default;
use alter_table_expression_default;

create table t_expr (id int primary key);
insert into t_expr values (1);
alter table t_expr add column c varchar(10) default (concat('x','y')) first;
select * from t_expr;
show create table t_expr;

create table t_def (id int primary key, v varchar(20));
alter table t_def alter column v set default (concat('m','n'));
insert into t_def(id) values (1);
select * from t_def;
desc t_def;
show create table t_def;

drop database alter_table_expression_default;
