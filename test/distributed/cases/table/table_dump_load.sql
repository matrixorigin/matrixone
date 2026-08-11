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

drop database table_dump_load_bvt;
remove files from stage if exists 'stage://table_dump_load_stage/full/*';
remove files from stage if exists 'stage://table_dump_load_stage/full/objects/*';
remove files from stage if exists 'stage://table_dump_load_stage/metadata/*';
remove files from stage if exists 'stage://table_dump_load_stage/auto/*';
remove files from stage if exists 'stage://table_dump_load_stage/auto/objects/*';
drop stage table_dump_load_stage;
