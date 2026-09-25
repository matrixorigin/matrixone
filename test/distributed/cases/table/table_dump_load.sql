drop database if exists table_dump_load_bvt;
create database table_dump_load_bvt;
use table_dump_load_bvt;
drop stage if exists table_dump_load_stage;
create stage table_dump_load_stage url = 'file:///tmp/mo-table-dump-load-bvt-25782-v3/';
remove files from stage if exists 'stage://table_dump_load_stage/full/*';
remove files from stage if exists 'stage://table_dump_load_stage/full/objects/*';
remove files from stage if exists 'stage://table_dump_load_stage/metadata/*';
remove files from stage if exists 'stage://table_dump_load_stage/auto/*';
remove files from stage if exists 'stage://table_dump_load_stage/auto/objects/*';
remove files from stage if exists 'stage://table_dump_load_stage/indexed/*';
remove files from stage if exists 'stage://table_dump_load_stage/binding/*';
remove files from stage if exists 'stage://table_dump_load_stage/binding_reverse/*';

create table src (id int primary key, value varchar(32));
insert into src values (1, 'one'), (2, 'two'), (3, 'three');
-- @separator:table
select mo_ctl('dn', 'flush', 'table_dump_load_bvt.src');
delete from src where id = 2;
-- @separator:table
select mo_ctl('dn', 'flush', 'table_dump_load_bvt.src');

dump table src to 'stage://table_dump_load_stage/full';
create table dst like src;
load table dst from 'stage://table_dump_load_stage/full';
select * from dst order by id;

dump table src to 'stage://table_dump_load_stage/metadata' metadata only;
create table metadata_dst like src;
load table metadata_dst from 'stage://table_dump_load_stage/metadata';
select * from metadata_dst order by id;

create table auto_src (hist_id int auto_increment primary key, value varchar(32));
insert into auto_src (value) values ('one'), ('two');
insert into auto_src (hist_id, value) values (100000, 'explicit');
-- @separator:table
select mo_ctl('dn', 'flush', 'table_dump_load_bvt.auto_src');
dump table auto_src to 'stage://table_dump_load_stage/auto';
create table auto_dst like auto_src;
load table auto_dst from 'stage://table_dump_load_stage/auto';
insert into auto_dst (value) values ('after-load');
select count(*) from auto_dst;
select hist_id > 100000 as auto_increment_restored from auto_dst where value = 'after-load';

-- An implicit table charset and its explicit server-default spelling create
-- storage-compatible secondary-index relations. Their internal table-default
-- metadata can differ even though the indexed column types are identical.
create table index_src (id varchar(255) primary key, value varchar(32), key idx_value (value));
dump table index_src to 'stage://table_dump_load_stage/indexed' metadata only;
create table index_dst (id varchar(255) primary key, value varchar(32), key idx_value (value)) collate=utf8mb4_general_ci;
load table index_dst from 'stage://table_dump_load_stage/indexed';
select count(*) from index_dst;

-- Persisted expressions may be authored under different division increments.
-- The restored table must evaluate future rows like the source, even when its
-- CREATE skeleton was parsed under a different session value.
set div_precision_increment = 10;
create table binding_src (
    a decimal(10,2), b decimal(10,2),
    q decimal(30,12) default (a / b)
);
set div_precision_increment = 4;
alter table binding_src add column r decimal(30,12)
    generated always as (a / b) stored;
insert into binding_src(a,b) values (1,3);
select q,r from binding_src;
-- @separator:table
select mo_ctl('dn', 'flush', 'table_dump_load_bvt.binding_src');
dump table binding_src to 'stage://table_dump_load_stage/binding' metadata only;
create table binding_dst (
    a decimal(10,2), b decimal(10,2),
    q decimal(30,12) default (a / b),
    r decimal(30,12) generated always as (a / b) stored
);
load table binding_dst from 'stage://table_dump_load_stage/binding';
insert into binding_dst(a,b) values (1,3);
select q,r from binding_dst order by q;
select count(distinct q) as distinct_default_values from binding_dst;
update binding_dst set a = 2;
select count(distinct r) as distinct_updated_generated_values from binding_dst;
select distinct r from binding_dst;

set div_precision_increment = 4;
create table binding_reverse_src (
    a decimal(10,2), b decimal(10,2),
    q decimal(30,12) default (a / b)
);
set div_precision_increment = 10;
alter table binding_reverse_src add column r decimal(30,12)
    generated always as (a / b) stored;
insert into binding_reverse_src(a,b) values (1,3);
-- @separator:table
select mo_ctl('dn', 'flush', 'table_dump_load_bvt.binding_reverse_src');
dump table binding_reverse_src to 'stage://table_dump_load_stage/binding_reverse' metadata only;
set div_precision_increment = 0;
create table binding_reverse_dst (
    a decimal(10,2), b decimal(10,2),
    q decimal(30,12) default (a / b),
    r decimal(30,12) generated always as (a / b) stored
);
load table binding_reverse_dst from 'stage://table_dump_load_stage/binding_reverse';
insert into binding_reverse_dst(a,b) values (1,3);
select q,r from binding_reverse_dst order by q;
select count(distinct q) as distinct_default_values,
       count(distinct r) as distinct_generated_values from binding_reverse_dst;
update binding_reverse_dst set a = 2;
select count(distinct r) as distinct_updated_generated_values from binding_reverse_dst;
select distinct r from binding_reverse_dst;

drop database table_dump_load_bvt;
remove files from stage if exists 'stage://table_dump_load_stage/full/*';
remove files from stage if exists 'stage://table_dump_load_stage/full/objects/*';
remove files from stage if exists 'stage://table_dump_load_stage/metadata/*';
remove files from stage if exists 'stage://table_dump_load_stage/auto/*';
remove files from stage if exists 'stage://table_dump_load_stage/auto/objects/*';
remove files from stage if exists 'stage://table_dump_load_stage/indexed/*';
remove files from stage if exists 'stage://table_dump_load_stage/binding/*';
remove files from stage if exists 'stage://table_dump_load_stage/binding_reverse/*';
drop stage table_dump_load_stage;
