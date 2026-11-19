drop database if exists load_last_insert_id_bvt;
create database load_last_insert_id_bvt;
use load_last_insert_id_bvt;

create table generated_ids (id int auto_increment primary key, value varchar(20));
-- A LOAD publishes its first generated id, and the next INSERT advances it.
load data inline format='csv', data='first\nsecond' into table generated_ids fields terminated by ',' (value);
select last_insert_id();
select id, value from generated_ids order by id;
insert into generated_ids (value) values ('third');
select last_insert_id();
-- Empty input preserves the last generated id.
load data infile '$resources/file_test/empty.txt' into table generated_ids fields terminated by ',' (value);
select last_insert_id();
-- Explicit IDs do not overwrite the session's previous generated id.
load data inline format='csv', data='20,explicit' into table generated_ids fields terminated by ',';
select last_insert_id();
select id, value from generated_ids order by id;
-- A table without a user auto-increment column must not expose a hidden id.
create table plain_values (value varchar(20));
load data inline format='csv', data='plain' into table plain_values fields terminated by ',';
select last_insert_id();
select value from plain_values;

drop database load_last_insert_id_bvt;
show databases like 'load_last_insert_id_bvt';
