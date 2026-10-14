-- VARCHAR declaration limits use the effective charset, not just character count.
drop database if exists varchar_ddl_width;
create database varchar_ddl_width;
use varchar_ddl_width;

create table rejected (c bool, v varchar(65535));
create table rejected (v varchar(16384));
create table rejected (v varchar(65536));
create table rejected (v varchar(65536)) as select 'value' as v;
show tables like 'rejected';

create table boundary (v varchar(16383));
insert into boundary values ('😀');
select v, length(v), char_length(v) from boundary;
show create table boundary;

-- An explicit text override must not inherit the binary table's byte budget.
create table rejected (v varchar(16384) character set utf8mb4) character set binary;
create table binary_column (v varchar(65535) character set binary);
show create table binary_column;
create table binary_table (v varchar(65535)) character set binary;
show create table binary_table;
create table rejected (v varchar(65536) character set binary);
create table rejected (v varchar(65536)) character set binary;

-- Keep MO's omitted-length syntax, but publish a legal text-column default.
create table implicit_width (v varchar);
show create table implicit_width;

-- Both COPY and INPLACE admission reject oversize user-authored columns.
create table alter_width (v varchar(10));
insert into alter_width values ('before');
alter table alter_width add column extra varchar(16384);
alter table alter_width modify column v varchar(16384);
alter table alter_width change column v renamed varchar(16384);
alter table alter_width add column extra varchar(65536);
alter table alter_width modify column v varchar(65536);
alter table alter_width change column v renamed varchar(65536);
alter table alter_width modify column v varchar(65536), add column flag bool;
show create table alter_width;
select * from alter_width;
alter table alter_width modify column v varchar(16383);
show create table alter_width;
alter table alter_width modify column v varchar(16384), add column flag bool;
show create table alter_width;

-- General CAST and native VARBINARY retain their existing capacity errors.
select cast('value' as varchar(65536));
create table rejected (v varbinary(65536));

-- Expression widths and unmodified existing schemas are not DDL declarations.
create table inferred as select cast('old' as varchar(65535)) as v;
create table copied like inferred;
alter table inferred add column extra int;
show create table copied;
show create table inferred;
select v, extra from inferred;

use mo_catalog;
drop database varchar_ddl_width;

-- Account bootstrap must retain the binary-owned catalog declarations.
drop account if exists varchar_width_account;
create account varchar_width_account admin_name 'admin' identified by '111';
drop account varchar_width_account;
select count(*) as remaining_accounts from mo_catalog.mo_account where account_name = 'varchar_width_account';
