-- An external table whose filepath is a pattern reads every match, and more
-- than one match is distributed whole -- one file per reader thread -- rather
-- than split by byte range.  The shard order is not defined, so every query
-- here is an aggregate or carries ORDER BY.
--
-- Each source file starts with a 9999,HEADER row, so IGNORE 1 LINES has to be
-- re-applied to every file: a leaked header would show up as id = 9999.
drop database if exists ext_glob;
create database ext_glob;
use ext_glob;

-- 1. plain csv pattern, three matches
drop table if exists eg_csv;
create external table eg_csv(id int, name varchar(50), v int) infile{"filepath"='$resources/load_data/glob/part_*.csv'} fields terminated by ',' lines terminated by '\n' ignore 1 lines;
select count(*), sum(id), min(id), max(id) from eg_csv;
select count(*) from eg_csv where id = 9999;
select count(distinct __mo_filepath) as files from eg_csv;
select id, name, v from eg_csv order by id;

-- 2. lz4 pattern: one reader per file, no file split
drop table if exists eg_lz4;
create external table eg_lz4(id int, name varchar(50), v int) infile{"filepath"='$resources/load_data/glob/zpart_*.csv.lz4'} fields terminated by ',' lines terminated by '\n' ignore 1 lines;
select count(*), sum(id) from eg_lz4;
select count(*) from eg_lz4 where id = 9999;
select count(distinct __mo_filepath) as files from eg_lz4;

-- 3. explicit compression covers every match
drop table if exists eg_lz4_explicit;
create external table eg_lz4_explicit(id int, name varchar(50), v int) infile{"filepath"='$resources/load_data/glob/zpart_*.csv.lz4',"compression"='lz4'} fields terminated by ',' lines terminated by '\n' ignore 1 lines;
select count(*), sum(id) from eg_lz4_explicit;

-- 4. mixed compression under one pattern, detected per file
drop table if exists eg_mixed;
create external table eg_mixed(id int, name varchar(50), v int) infile{"filepath"='$resources/load_data/glob/mix/m_*'} fields terminated by ',' lines terminated by '\n' ignore 1 lines;
select count(*), sum(id), min(id), max(id) from eg_mixed;
select count(*) from eg_mixed where id = 9999;

-- 5. without IGNORE the header is data, once per file
drop table if exists eg_noignore;
create external table eg_noignore(id int, name varchar(50), v int) infile{"filepath"='$resources/load_data/glob/part_*.csv'} fields terminated by ',' lines terminated by '\n';
select count(*) from eg_noignore;
select count(*) from eg_noignore where id = 9999;

-- 6. a pattern matching exactly one file
drop table if exists eg_one;
create external table eg_one(id int, name varchar(50), v int) infile{"filepath"='$resources/load_data/glob/one/solo_*.csv'} fields terminated by ',' lines terminated by '\n' ignore 1 lines;
select count(*), sum(id), count(distinct __mo_filepath) from eg_one;

-- 7. __mo_filepath still prunes to a single shard's file
select count(*) from eg_csv where __mo_filepath like '%part_1%';
select sum(id) from eg_csv where __mo_filepath not like '%part_0%';

-- 8. the scan feeds DML: every shard's rows must reach the target
drop table if exists eg_copy;
create table eg_copy(id int, name varchar(50), v int);
insert into eg_copy select id, name, v from eg_csv;
select count(*), sum(id) from eg_copy;

-- 9. and a table with a primary key, which adds the lock/dedup fan-in
drop table if exists eg_copy_pk;
create table eg_copy_pk(id int primary key, name varchar(50), v int);
insert into eg_copy_pk select id, name, v from eg_lz4;
select count(*), sum(id) from eg_copy_pk;

-- 10. joins and aggregates over a fanned-out scan
select count(*) from eg_csv a join eg_lz4 b on a.id = b.id;
select v % 10 as bucket, count(*) as n from eg_csv group by bucket order by bucket;

drop database ext_glob;
