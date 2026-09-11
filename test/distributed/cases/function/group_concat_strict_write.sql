-- @suite
-- @case

drop database if exists group_concat_strict_write_28664;
create database group_concat_strict_write_28664;
use group_concat_strict_write_28664;

create table src (id int primary key, v varchar(20));
insert into src values (1, 'aa'), (2, 'bbb'), (3, 'cccc');
create table dst (id int primary key, gc varchar(100));
create table dst_insert (gc varchar(100));
create table dst_ignore (gc varchar(100));
create table dst_nonstrict (gc varchar(100));
insert into dst values (1, 'sentinel');

set @group_concat_strict_saved_mode = @@sql_mode;
set @group_concat_strict_saved_max_len = @@group_concat_max_len;
set session group_concat_max_len = 4;
set session sql_mode = 'STRICT_TRANS_TABLES';

select group_concat(v order by id separator '|') from src;
show warnings;

insert into dst_insert select group_concat(v order by id separator '|') from src;
select count(*) from dst_insert;

create table ctas_strict as
select group_concat(v order by id separator '|') as gc from src;
select count(*) from information_schema.tables
where table_schema = 'group_concat_strict_write_28664' and table_name = 'ctas_strict';

update dst set gc = (select group_concat(v order by id separator '|') from src) where id = 1;
select gc from dst where id = 1;

replace into dst select 1, group_concat(v order by id separator '|') from src;
select gc from dst where id = 1;

insert into dst select 1, group_concat(v order by id separator '|') from src
on duplicate key update gc = values(gc);
select gc from dst where id = 1;

insert ignore into dst_ignore
select group_concat(v order by id separator '|') from src;
select gc from dst_ignore;
show warnings;

set session sql_mode = '';
insert into dst_nonstrict
select group_concat(v order by id separator '|') from src;
select gc from dst_nonstrict;

set session sql_mode = @group_concat_strict_saved_mode;
set session group_concat_max_len = @group_concat_strict_saved_max_len;
drop database group_concat_strict_write_28664;
