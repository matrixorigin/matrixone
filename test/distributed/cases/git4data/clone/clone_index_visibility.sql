-- issue#27078: DDL reconstruction must preserve secondary-index visibility.
drop database if exists clone_index_visibility_src;
drop database if exists clone_index_visibility_table_dst;
drop database if exists clone_index_visibility_db_dst;

create database clone_index_visibility_src;
create database clone_index_visibility_table_dst;
use clone_index_visibility_src;

create table src (
    id int primary key,
    visible_key varchar(32),
    invisible_key varchar(32),
    key idx_visible(visible_key),
    key idx_invisible(invisible_key) comment 'kept-invisible'
);
insert into src values (1, 'visible-1', 'invisible-1'), (2, 'visible-2', 'invisible-2');
alter table src alter index idx_invisible invisible;

show index from src;
show create table src;

create table clone_index_visibility_table_dst.table_clone clone clone_index_visibility_src.src;
show index from clone_index_visibility_table_dst.table_clone;
show create table clone_index_visibility_table_dst.table_clone;
select id, visible_key, invisible_key from clone_index_visibility_table_dst.table_clone where invisible_key = 'invisible-2';

create table clone_index_visibility_table_dst.like_clone like clone_index_visibility_src.src;
show index from clone_index_visibility_table_dst.like_clone;
show create table clone_index_visibility_table_dst.like_clone;

create database clone_index_visibility_db_dst clone clone_index_visibility_src;
show index from clone_index_visibility_db_dst.src;
show create table clone_index_visibility_db_dst.src;
select id, visible_key, invisible_key from clone_index_visibility_db_dst.src where visible_key = 'visible-1';

drop database if exists clone_index_visibility_src;
drop database if exists clone_index_visibility_table_dst;
drop database if exists clone_index_visibility_db_dst;
