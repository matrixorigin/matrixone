-- A table without a primary key has a hidden auto-increment fake pk column.
-- It must not show up as the table's AUTO_INCREMENT value.
drop database if exists ai_no_pk_meta;
create database ai_no_pk_meta;
use ai_no_pk_meta;
create table no_pk(a int, b varchar(10));
insert into no_pk values (1, 'x'), (2, 'y'), (3, 'z');
create table with_pk(a int primary key, b int);
insert into with_pk values (1, 1);
create table with_ai(id int auto_increment primary key, b int);
insert into with_ai(b) values (1), (2);
create table no_pk_ai(id int auto_increment, b int, unique key(id));
insert into no_pk_ai(b) values (1), (2), (3);
select table_name, auto_increment from information_schema.tables where table_schema = 'ai_no_pk_meta' order by table_name;
select internal_auto_increment('ai_no_pk_meta', 'no_pk') is null;
select internal_auto_increment('ai_no_pk_meta', 'with_ai');
select internal_auto_increment('ai_no_pk_meta', 'no_pk_ai');
-- A visible user column that reuses the fake pk name is a real
-- AUTO_INCREMENT column; the fake-pk skip must apply only to the hidden
-- synthetic column.
create table visible_fake_pk(a int primary key, `__mo_fake_pk_col` bigint unsigned auto_increment unique key);
insert into visible_fake_pk(a) values (1), (2);
select table_name, auto_increment from information_schema.tables where table_schema = 'ai_no_pk_meta' and table_name = 'visible_fake_pk';
select internal_auto_increment('ai_no_pk_meta', 'visible_fake_pk');
drop database ai_no_pk_meta;
