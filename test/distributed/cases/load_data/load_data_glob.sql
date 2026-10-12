-- LOAD DATA with a filepath pattern: every match is loaded, whole files fan out
-- across scopes, and IGNORE n LINES applies to each file rather than only the
-- first.  Compressed sources can only be read one thread per file, so the file
-- count is the unit of parallelism there.
drop database if exists load_glob;
create database load_glob;
use load_glob;

-- Each shard carries a 9999,HEADER row; a per-file IGNORE drops all three.
-- Anything but 0 below means a header leaked in as data.

-- 1. plain csv pattern, three matches
drop table if exists g_csv;
create table g_csv(id int, name varchar(50), v int);
load data infile {'filepath'='$resources/load_data/glob/part_*.csv'} into table g_csv fields terminated by ',' lines terminated by '\n' ignore 1 lines parallel 'true';
select count(*), sum(id), min(id), max(id) from g_csv;
select count(*) from g_csv where id = 9999;
select id, name, v from g_csv order by id;

-- 2. lz4 pattern: each file is decompressed by its own reader
drop table if exists g_lz4;
create table g_lz4(id int, name varchar(50), v int);
load data infile {'filepath'='$resources/load_data/glob/zpart_*.csv.lz4'} into table g_lz4 fields terminated by ',' lines terminated by '\n' ignore 1 lines parallel 'true';
select count(*), sum(id), min(id), max(id) from g_lz4;
select count(*) from g_lz4 where id = 9999;

-- 3. explicit compression applies to every match
drop table if exists g_lz4_explicit;
create table g_lz4_explicit(id int, name varchar(50), v int);
load data infile {'filepath'='$resources/load_data/glob/zpart_*.csv.lz4', 'compression'='lz4'} into table g_lz4_explicit fields terminated by ',' lines terminated by '\n' ignore 1 lines parallel 'true';
select count(*), sum(id) from g_lz4_explicit;

-- 4. mixed compression under one pattern, detected per file
drop table if exists g_mixed;
create table g_mixed(id int, name varchar(50), v int);
load data infile {'filepath'='$resources/load_data/glob/mix/m_*'} into table g_mixed fields terminated by ',' lines terminated by '\n' ignore 1 lines parallel 'true';
select count(*), sum(id), min(id), max(id) from g_mixed;
select count(*) from g_mixed where id = 9999;

-- 5. same pattern without parallel: serial, same rows
drop table if exists g_serial;
create table g_serial(id int, name varchar(50), v int);
load data infile {'filepath'='$resources/load_data/glob/part_*.csv'} into table g_serial fields terminated by ',' lines terminated by '\n' ignore 1 lines;
select count(*), sum(id) from g_serial;
select count(*) from g_serial where id = 9999;

-- 6. a pattern matching exactly one file behaves like naming that file
drop table if exists g_one;
create table g_one(id int, name varchar(50), v int);
load data infile {'filepath'='$resources/load_data/glob/one/solo_*.csv'} into table g_one fields terminated by ',' lines terminated by '\n' ignore 1 lines parallel 'true';
select count(*), sum(id) from g_one;
select count(*) from g_one where id = 9999;

-- 7. no IGNORE: the header rows are data, one per file
drop table if exists g_noignore;
create table g_noignore(id int, name varchar(50), v int);
load data infile {'filepath'='$resources/load_data/glob/part_*.csv'} into table g_noignore fields terminated by ',' lines terminated by '\n' parallel 'true';
select count(*) from g_noignore;
select count(*) from g_noignore where id = 9999;

-- 8. loading twice appends every match again
load data infile {'filepath'='$resources/load_data/glob/part_*.csv'} into table g_csv fields terminated by ',' lines terminated by '\n' ignore 1 lines parallel 'true';
select count(*), sum(id) from g_csv;

-- 9. a pattern matching nothing is an error, not an empty load
drop table if exists g_empty;
create table g_empty(id int, name varchar(50), v int);
load data infile {'filepath'='$resources/load_data/glob/nosuch_*.csv'} into table g_empty fields terminated by ',' lines terminated by '\n' parallel 'true';
select count(*) from g_empty;

-- 10. the pattern survives into SHOW, and a rolled back glob load leaves nothing
drop table if exists g_rollback;
create table g_rollback(id int, name varchar(50), v int);
insert into g_rollback values(-1, 'seed', -1);
begin;
load data infile {'filepath'='$resources/load_data/glob/part_*.csv'} into table g_rollback fields terminated by ',' lines terminated by '\n' ignore 1 lines parallel 'true';
select count(*) from g_rollback;
rollback;
select count(*), min(id) from g_rollback;

-- 11. a table with a primary key: the fanout feeds the dedup/lock plan shape
drop table if exists g_pk;
create table g_pk(id int primary key, name varchar(50), v int);
load data infile {'filepath'='$resources/load_data/glob/part_*.csv'} into table g_pk fields terminated by ',' lines terminated by '\n' ignore 1 lines parallel 'true';
select count(*), sum(id) from g_pk;

-- 12. an irregular (fulltext) index puts a SINK between the scan and the
--     update, a different fan-in point than the plans above
set experimental_fulltext_index=1;
drop table if exists g_ft;
create table g_ft(id int primary key, name varchar(50), v int, fulltext(name));
load data infile {'filepath'='$resources/load_data/glob/part_*.csv'} into table g_ft fields terminated by ',' lines terminated by '\n' ignore 1 lines parallel 'true';
select count(*), sum(id) from g_ft;
select count(*) from g_ft where id = 9999;

drop database load_glob;
