drop database if exists charset_defaults_contract;
create database charset_defaults_contract collate utf8mb4_bin;
use charset_defaults_contract;

-- Default-only ALTER preserves existing data and governs future declarations.
create table defaults_target(v varchar(8), c char(8), t tinytext, raw varbinary(8), n int, index iv(v));
insert into defaults_target values ('a','b','c','d',1),('B','C','D','e',2);
alter table defaults_target add column before_default varchar(8), default collate utf8mb4_general_ci;
alter table defaults_target default collate utf8mb4_bin, add column after_default varchar(8);
select column_name,character_set_name,collation_name from information_schema.columns
where table_schema='charset_defaults_contract' and table_name='defaults_target' order by ordinal_position;
select table_collation from information_schema.tables where table_schema='charset_defaults_contract' and table_name='defaults_target';
select v,c,t,hex(raw),n from defaults_target order by n;
alter table defaults_target default collate utf8mb4_general_ci, drop index iv;
select count(*) from information_schema.statistics where table_schema='charset_defaults_contract' and table_name='defaults_target' and index_name='iv';
alter table defaults_target add index iv(v), default collate utf8mb4_bin;
select count(*) from defaults_target force index(iv) where v='a';

-- Durable native defaults use their admitted identity and revision.
alter database charset_defaults_contract collate utf8mb4_unicode_ci;
create table native_defaults(v varchar(8), g varchar(8) generated always as (lower(v)) stored, raw varbinary(8), n int);
insert into native_defaults(v,raw,n) values ('ß','x',1),('ss','y',2);
select @@character_set_database,@@collation_database;
show create database charset_defaults_contract;
show create table native_defaults;
show full columns from native_defaults;
select column_name,character_set_name,collation_name from information_schema.columns
where table_schema='charset_defaults_contract' and table_name='native_defaults' order by ordinal_position;
select table_collation from information_schema.tables where table_schema='charset_defaults_contract' and table_name='native_defaults';
select count(*) from native_defaults where v='ss';
create temporary table native_temporary(v varchar(8));
show create table native_temporary;
drop table native_temporary;
create table native_like like native_defaults;
create table native_ctas as select v from native_defaults;
select table_name,column_name,collation_name from information_schema.columns
where table_schema='charset_defaults_contract' and table_name in ('native_like','native_ctas') and column_name='v' order by table_name;

-- CONVERT owns carried columns and rebuilt secondary-index keys.
create table converted(id int primary key, v varchar(8), index iv(v)) collate=utf8mb4_bin;
insert into converted values (1,'ß'),(2,'ss');
alter table converted convert to character set utf8mb4 collate utf8mb4_unicode_ci;
select count(*) from converted force index(iv) where v='ss';
show create table converted;
show full columns from converted;
alter table converted default collate=utf8mb4_bin, add column added varchar(8), convert to character set utf8mb4 collate utf8mb4_unicode_ci;
select column_name,collation_name from information_schema.columns where table_schema='charset_defaults_contract' and table_name='converted' order by ordinal_position;
select table_collation from information_schema.tables where table_schema='charset_defaults_contract' and table_name='converted';
select count(*) from converted force index(iv) where v='ss';

-- Disabled domains and native unique-key formats remain closed.
create table unique_conversion(v varchar(8) unique) collate=utf8mb4_bin;
insert into unique_conversion values ('ß'),('ss');
alter table unique_conversion convert to character set utf8mb4 collate utf8mb4_unicode_ci;
select count(*),min(v),max(v) from unique_conversion;
select collation_name from information_schema.columns where table_schema='charset_defaults_contract' and table_name='unique_conversion' and column_name='v';
alter table converted convert to character set latin1;
alter table converted convert to character set gbk;
create table repertoire_conversion(v varchar(8)) collate=utf8mb4_bin;
insert into repertoire_conversion values ('😀');
alter table repertoire_conversion convert to character set utf8 collate utf8_unicode_ci;
select count(*),hex(min(v)) from repertoire_conversion;
select collation_name from information_schema.columns where table_schema='charset_defaults_contract' and table_name='repertoire_conversion' and column_name='v';

-- Byte capacity, fixed padding, and computed target values are validated before publication.
create table binary_conversion(v varchar(1),c char(1),t tinytext) collate=utf8mb4_bin;
insert into binary_conversion values ('😀','😀','😀');
alter table binary_conversion convert to character set binary;
select hex(v),hex(c),hex(t) from binary_conversion;
show create table binary_conversion;
alter table binary_conversion modify v varchar(8), modify c char(8);
alter table binary_conversion convert to character set binary;
select hex(v),hex(c),hex(t) from binary_conversion;
show create table binary_conversion;
create table binary_generated(v varchar(4),g varchar(3) generated always as (concat(v,'x')) stored) collate=utf8mb4_bin;
insert into binary_generated(v) values ('🧪');
alter table binary_generated convert to character set binary;
select hex(v),hex(g) from binary_generated;
show create table binary_generated;
alter table binary_generated modify g varchar(8) generated always as (concat(v,'x')) stored;
alter table binary_generated convert to character set binary;
select hex(v),hex(g) from binary_generated;
show create table binary_generated;

-- COPY checks the destination assignment without removing the user's CAST.
create table explicit_binary(v varchar(4),g binary(2) generated always as (cast(v as binary(2))) stored) collate=utf8mb4_bin;
insert into explicit_binary(v) values ('🧪');
alter table explicit_binary convert to character set binary;
select hex(v),hex(g) from explicit_binary;
show create table explicit_binary;

-- Replay and inheritance never mutate the originating declaration.
create table source_like(v varchar(8)) collate=utf8mb4_bin;
create table target_like like source_like;
alter table target_like default collate=utf8mb4_general_ci;
alter table source_like add column added varchar(8);
alter table target_like add column added varchar(8);
select table_name,column_name,collation_name from information_schema.columns
where table_schema='charset_defaults_contract' and table_name in ('source_like','target_like') order by table_name,ordinal_position;
select table_name from information_schema.tables where table_schema='charset_defaults_contract' and table_name like '%_copy_%' order by table_name;
-- Session-owned temporary declarations follow the table default as well.
create temporary table temporary_default(v varchar(8)) collate utf8mb4_unicode_ci;
insert into temporary_default values ('ß'),('ss');
alter table temporary_default add column added varchar(8) default 'ß';
select count(*) from temporary_default where added='ss';
alter table temporary_default default collate utf8mb4_bin;
select count(*) from temporary_default where v='ss';
alter table temporary_default add column future varchar(8) default 'ß';
select count(*) from temporary_default where future='ss';
alter table temporary_default modify added varchar(8);
select count(*) from temporary_default where added='ss';
drop temporary table temporary_default;
drop database charset_defaults_contract;
