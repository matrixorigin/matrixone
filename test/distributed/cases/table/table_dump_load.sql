drop database if exists table_dump_load_bvt;
create database table_dump_load_bvt;
use table_dump_load_bvt;
drop stage if exists table_dump_load_stage;
create stage table_dump_load_stage url = 'file:///tmp/mo-table-dump-load-bvt-25782-v3/';
remove files from stage if exists 'stage://table_dump_load_stage/full/*';
remove files from stage if exists 'stage://table_dump_load_stage/full/objects/*';
remove files from stage if exists 'stage://table_dump_load_stage/metadata/*';

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

drop database table_dump_load_bvt;
remove files from stage if exists 'stage://table_dump_load_stage/full/*';
remove files from stage if exists 'stage://table_dump_load_stage/full/objects/*';
remove files from stage if exists 'stage://table_dump_load_stage/metadata/*';
drop stage table_dump_load_stage;
