drop database if exists charset_conversion_expressions;
create database charset_conversion_expressions;
use charset_conversion_expressions;
set @saved_div_precision = @@div_precision_increment;
set div_precision_increment = 10;
create table t (
    id int primary key,
    a decimal(10,2),
    b decimal(10,2),
    g varchar(64) generated always as (a / b) stored,
    authored varchar(64) generated always as (cast(a / b as decimal(10,4))) stored,
    d varchar(64) default (cast(1 as decimal(10,2)) / cast(3 as decimal(10,2)))
) collate utf8mb4_bin;
insert into t(id,a,b) values (1,1,3);
select id,g,authored,d from t order by id;
set div_precision_increment = 0;
alter table t add column extra int, algorithm=copy;
select id,g,authored,d from t order by id;
alter table t convert to character set utf8mb4 collate utf8mb4_bin;
select id,g,authored,d from t order by id;
insert into t(id,a,b) values (2,1,3);
select id,g,authored,d from t order by id;
alter table t convert to character set utf8mb4 collate utf8mb4_general_ci;
select id,g,authored,d from t order by id;
select column_name,collation_name from information_schema.columns where table_schema='charset_conversion_expressions' and table_name='t' and column_name in ('g','authored','d') order by column_name;
insert into t(id,a,b) values (3,1,3);
select id,g,authored,d from t order by id;
-- A changed character dependency must not re-author unrelated arithmetic.
set div_precision_increment = 10;
create table dependent (
    id int primary key,
    v varchar(8),
    a decimal(10,2),
    b decimal(10,2),
    g varchar(64) generated always as (concat(v,a/b)) stored,
    authored varchar(64) generated always as (concat(v,cast(a/b as decimal(10,4)))) stored,
    eq varchar(8) generated always as (if(v='X','yes','no')) stored,
    fixed varchar(8) generated always as (if(v collate utf8mb4_bin='X','yes','no')) stored
) collate utf8mb4_bin;
insert into dependent(id,v,a,b) values (1,'x',1,3);
select id,g,authored from dependent order by id;
set div_precision_increment = 0;
alter table dependent convert to character set utf8mb4 collate utf8mb4_general_ci;
select id,g,authored from dependent order by id;
insert into dependent(id,v,a,b) values (2,'x',1,3);
select id,g,authored from dependent order by id;
-- Affected character operations must acquire the new domain, not just its label.
alter table dependent convert to character set utf8mb4 collate utf8mb4_unicode_ci;
select id,g,authored,eq,fixed from dependent order by id;
insert into dependent(id,v,a,b) values (3,'x',1,3);
select id,g,authored,eq,fixed from dependent order by id;
set div_precision_increment = @saved_div_precision;
drop database charset_conversion_expressions;
