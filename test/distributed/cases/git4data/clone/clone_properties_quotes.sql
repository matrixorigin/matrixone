-- issue#27082: PROPERTIES values and keys must survive generated DDL replay.
drop database if exists clone_prop_quote_src;
drop database if exists clone_prop_quote_db_dst;
create database clone_prop_quote_src;
use clone_prop_quote_src;

create table prop_src(id int primary key)
properties(
    'key"double' = 'value"double',
    'key''single' = 'value''single',
    'key\\backslash' = 'value\\backslash',
    'key"mixed''\\' = 'value"mixed''\\'
);
insert into prop_src values (1), (2);

show create table prop_src;
create database clone_prop_quote_db_dst clone clone_prop_quote_src;
create table clone_prop_quote_table_dst clone prop_src;
create table clone_prop_quote_like_dst like prop_src;

show create table clone_prop_quote_table_dst;
show create table clone_prop_quote_like_dst;
show create table clone_prop_quote_db_dst.prop_src;

select id from prop_src order by id;
select id from clone_prop_quote_table_dst order by id;
select id from clone_prop_quote_like_dst order by id;
select id from clone_prop_quote_db_dst.prop_src order by id;

insert into clone_prop_quote_table_dst values (3);
insert into clone_prop_quote_like_dst values (4);
insert into clone_prop_quote_db_dst.prop_src values (5);
insert into prop_src values (6);

select id from prop_src order by id;
select id from clone_prop_quote_table_dst order by id;
select id from clone_prop_quote_like_dst order by id;
select id from clone_prop_quote_db_dst.prop_src order by id;

select enable_fault_injection();
select add_fault_point('fj/cn/clone_fails',':::','echo',40,'clone_prop_quote_src.clone_prop_quote_failure_table');
create table clone_prop_quote_failure_table clone prop_src;
select disable_fault_injection();
select count(*) from mo_catalog.mo_tables
where reldatabase = 'clone_prop_quote_src' and relname = 'clone_prop_quote_failure_table';

select enable_fault_injection();
select add_fault_point('fj/cn/clone_fails',':::','echo',40,'clone_prop_quote_failure_db.prop_src');
create database clone_prop_quote_failure_db clone clone_prop_quote_src;
select disable_fault_injection();
select count(*) from mo_catalog.mo_database
where datname = 'clone_prop_quote_failure_db';

drop database if exists clone_prop_quote_src;
drop database if exists clone_prop_quote_db_dst;
drop database if exists clone_prop_quote_failure_db;
