-- DELETE uses stored ENUM/SET values, including when an index was rebuilt.
drop table if exists enum_delete_rebuild_42;
create table enum_delete_rebuild_42 (
    id int primary key,
    status enum('new','paid','shipped'),
    key idx_status(status)
);
insert into enum_delete_rebuild_42 values (1,'new'),(2,'paid');
delete from enum_delete_rebuild_42 where id=2;
select row_count();
select id,status from enum_delete_rebuild_42 order by id;
drop table enum_delete_rebuild_42;

-- ALTER backfill and DELETE must serialize composite ENUM keys identically.
drop table if exists enum_delete_backfill_42;
create table enum_delete_backfill_42 (
    id int primary key,
    tag varchar(16),
    status enum('new','paid','shipped')
);
insert into enum_delete_backfill_42 values (1,'a','new'),(2,'b','paid');
alter table enum_delete_backfill_42 add key idx_tag_status(tag,status);
delete from enum_delete_backfill_42 where id=1;
select row_count();
select id,tag,status from enum_delete_backfill_42 order by id;
drop table enum_delete_backfill_42;

-- Rebuilding a UNIQUE ENUM index distinguishes ordinal zero from an empty label.
drop table if exists enum_unique_backfill_42;
create table enum_unique_backfill_42 (
    id int primary key,
    status enum('','a')
);
insert ignore into enum_unique_backfill_42 values (1,'invalid'),(2,'');
alter table enum_unique_backfill_42 add unique key uk_status(status);
delete from enum_unique_backfill_42 where id=1;
select row_count();
insert ignore into enum_unique_backfill_42 values (3,'invalid');
select id from enum_unique_backfill_42 order by id;
drop table enum_unique_backfill_42;

-- Ordered DELETE keeps the physical SET bitmap across the result projection.
drop table if exists set_delete_backfill_42;
create table set_delete_backfill_42 (
    id int primary key,
    opts set('','a')
);
insert into set_delete_backfill_42 values (1,1),(2,2);
alter table set_delete_backfill_42 add key idx_opts_id(opts,id);
delete from set_delete_backfill_42 where id in (1,2) order by id limit 1;
select row_count();
select id,opts from set_delete_backfill_42 order by id;
drop table set_delete_backfill_42;

-- UPDATE must retain the empty SET member in its old and new images.
drop table if exists set_update_storage_42;
create table set_update_storage_42 (id int primary key, opts set('','a'), n int, key idx_opts(opts));
insert into set_update_storage_42 values (1,1,0),(2,1,0);
update set_update_storage_42 set n=9 where id=1;
select row_count();
select id,opts+0,n from set_update_storage_42 order by id;
update set_update_storage_42 set opts=2 where id=2;
select row_count();
select id,opts+0,n from set_update_storage_42 order by id;
drop table set_update_storage_42;

-- A prefix of the primary key does not identify a secondary-index owner.
drop table if exists enum_delete_prefix_42;
create table enum_delete_prefix_42 (
    id varchar(8) primary key,
    e enum('x','y'),
    n int,
    key ix(id(1),e,n)
);
insert into enum_delete_prefix_42 values ('aa','x',0),('ab','x',0);
delete from enum_delete_prefix_42 where id='ab';
select row_count();
select id from enum_delete_prefix_42 use index() order by id;
delete from enum_delete_prefix_42 where id='aa';
select row_count();
select count(*) from enum_delete_prefix_42;
drop table enum_delete_prefix_42;

-- Multi-target DELETE follows the legacy index-maintenance path.
drop table if exists set_delete_prefix_42;
drop table if exists set_delete_prefix_aux_42;
create table set_delete_prefix_42 (
    id varchar(8) primary key,
    s set('','a'),
    n int,
    key ix(id(1),s,n)
);
create table set_delete_prefix_aux_42(id int primary key);
insert into set_delete_prefix_42 values ('aa',1,0),('ab',1,0);
insert into set_delete_prefix_aux_42 values (1);
delete t,a from set_delete_prefix_42 t join set_delete_prefix_aux_42 a on a.id=1 where t.id='ab';
select row_count();
select id from set_delete_prefix_42 use index() order by id;
delete from set_delete_prefix_42 where id='aa';
select row_count();
select count(*) from set_delete_prefix_42;
drop table set_delete_prefix_aux_42;
drop table set_delete_prefix_42;
