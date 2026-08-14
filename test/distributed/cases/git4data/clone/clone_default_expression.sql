-- issue#27101: DDL reconstruction must preserve quoted arguments in default expressions.
drop database if exists clone_default_src;
drop database if exists clone_default_table_dst;
drop database if exists clone_default_db_dst;

create database clone_default_src;
use clone_default_src;

create sequence seq increment 5 start 10;
create table t_seq (
    id bigint primary key default nextval('seq'),
    v varchar(20)
);
create table t_literal (id bigint default 42);

insert into t_seq(v) values ('source');
insert into t_literal values ();

show create table t_seq;
show create table t_literal;

create database clone_default_table_dst;
create table clone_default_table_dst.t_seq clone clone_default_src.t_seq;
create table clone_default_table_dst.t_literal clone clone_default_src.t_literal;
show tables from clone_default_table_dst;
select * from clone_default_table_dst.t_seq;
select * from clone_default_table_dst.t_literal;

create database clone_default_db_dst clone clone_default_src;
show tables from clone_default_db_dst;
select * from clone_default_db_dst.t_seq;
select * from clone_default_db_dst.t_literal;

drop database if exists clone_default_src;
drop database if exists clone_default_table_dst;
drop database if exists clone_default_db_dst;
