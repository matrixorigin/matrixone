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
