-- DATA BRANCH CREATE DATABASE records database identity even when there are
-- no ordinary-table receipts, and DELETE uses that identity without weakening
-- validation for ordinary databases or locally-added tables.

drop database if exists issue26068_empty_src;
drop database if exists issue26068_empty_dst;
drop database if exists issue26068_view_src;
drop database if exists issue26068_view_dst;
drop database if exists issue26068_sequence_src;
drop database if exists issue26068_sequence_dst;
drop database if exists issue26068_dropall_src;
drop database if exists issue26068_dropall_dst;
drop database if exists issue26068_local_table_src;
drop database if exists issue26068_local_table_dst;
drop database if exists issue26068_ordinary_empty;

-- Empty source database: the database marker is the only branch receipt.
create database issue26068_empty_src;
data branch create database issue26068_empty_dst from issue26068_empty_src;
select dat_type from mo_catalog.mo_database where datname = 'issue26068_empty_dst';
data branch delete database issue26068_empty_dst;
select count(*) as empty_branch_exists from mo_catalog.mo_database where datname = 'issue26068_empty_dst';

-- View-only source database: views remain usable and do not need table receipts.
create database issue26068_view_src;
create view issue26068_view_src.v as select 1 as n;
select * from issue26068_view_src.v;
data branch create database issue26068_view_dst from issue26068_view_src;
select * from issue26068_view_dst.v;
data branch delete database issue26068_view_dst;
select count(*) as view_branch_exists from mo_catalog.mo_database where datname = 'issue26068_view_dst';

-- Sequence-only source database: sequences are excluded from branch-table validation.
create database issue26068_sequence_src;
create sequence issue26068_sequence_src.s increment 3 start with 7;
select count(*) as source_sequences from mo_catalog.mo_tables
  where reldatabase = 'issue26068_sequence_src' and relkind = 'S';
data branch create database issue26068_sequence_dst from issue26068_sequence_src;
select count(*) as cloned_sequences from mo_catalog.mo_tables
  where reldatabase = 'issue26068_sequence_dst' and relkind = 'S';
data branch delete database issue26068_sequence_dst;
select count(*) as sequence_branch_exists from mo_catalog.mo_database where datname = 'issue26068_sequence_dst';

-- A database branch remains identifiable after every cloned table is dropped.
create database issue26068_dropall_src;
create table issue26068_dropall_src.t1(id int primary key);
create table issue26068_dropall_src.t2(id int primary key);
select count(*) as source_tables from mo_catalog.mo_tables
  where reldatabase = 'issue26068_dropall_src' and relname in ('t1', 't2');
data branch create database issue26068_dropall_dst from issue26068_dropall_src;
select count(*) as cloned_tables from mo_catalog.mo_tables
  where reldatabase = 'issue26068_dropall_dst' and relname in ('t1', 't2');
drop table issue26068_dropall_dst.t1;
drop table issue26068_dropall_dst.t2;
data branch delete database issue26068_dropall_dst;
select count(*) as dropped_table_branch_exists from mo_catalog.mo_database where datname = 'issue26068_dropall_dst';

-- The marker does not permit deleting a locally-added ordinary table.
create database issue26068_local_table_src;
data branch create database issue26068_local_table_dst from issue26068_local_table_src;
create table issue26068_local_table_dst.local_t(id int primary key);
data branch delete database issue26068_local_table_dst;
select count(*) as protected_local_table from mo_catalog.mo_tables
  where reldatabase = 'issue26068_local_table_dst' and relname = 'local_t';
drop table issue26068_local_table_dst.local_t;
data branch delete database issue26068_local_table_dst;
select count(*) as local_table_branch_exists from mo_catalog.mo_database where datname = 'issue26068_local_table_dst';

-- An ordinary empty database is still not a DATA BRANCH DELETE target.
create database issue26068_ordinary_empty;
data branch delete database issue26068_ordinary_empty;
select count(*) as ordinary_empty_exists from mo_catalog.mo_database where datname = 'issue26068_ordinary_empty';

drop database if exists issue26068_empty_src;
drop database if exists issue26068_empty_dst;
drop database if exists issue26068_view_src;
drop database if exists issue26068_view_dst;
drop database if exists issue26068_sequence_src;
drop database if exists issue26068_sequence_dst;
drop database if exists issue26068_dropall_src;
drop database if exists issue26068_dropall_dst;
drop database if exists issue26068_local_table_src;
drop database if exists issue26068_local_table_dst;
drop database if exists issue26068_ordinary_empty;
