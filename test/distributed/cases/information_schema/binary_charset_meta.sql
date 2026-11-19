-- @suite
-- @case
-- @desc: issue #31160 - information_schema.COLUMNS must report NULL charset/collation for BINARY/VARBINARY/BLOB
-- @label:bvt

drop database if exists qa_bug31160;
create database qa_bug31160;
create table qa_bug31160.t (id int primary key, bin binary(4), vb varbinary(8), bl blob, vc varchar(10) character set utf8mb4 collate utf8mb4_bin, tc text, ch char(5));
alter table qa_bug31160.t add column nb binary(2);
create table qa_bug31160.t2 as select * from qa_bug31160.t;
select count(*) from information_schema.columns where table_schema='qa_bug31160' and table_name in ('t','t2') and data_type in ('binary','varbinary','blob') and character_set_name is null and collation_name is null;
select count(*) from information_schema.columns where table_schema='qa_bug31160' and table_name in ('t','t2') and data_type in ('binary','varbinary','blob') and (character_set_name is not null or collation_name is not null);
select count(*) from information_schema.columns where table_schema='qa_bug31160' and column_name='vc' and character_set_name='utf8mb4' and collation_name='utf8mb4_bin';
select count(*) from information_schema.columns where table_schema='qa_bug31160' and character_set_name='binary';
drop database qa_bug31160;
