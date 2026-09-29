-- LOAD DATA and external tables over zstd- and zip-compressed files.  The
-- resources were written by the zstd and Info-ZIP command-line tools.  A zip
-- archive is read as a stream, so it also works for LOAD DATA LOCAL; the entry
-- loaded is the first one that is neither a directory nor hidden, as for tar.
drop database if exists load_zz;
create database load_zz;
use load_zz;

create table t(id int, name varchar(20));

-- 1. zstd, detected from the .zst extension
load data infile {'filepath'='$resources/load_data/zipzstd/rows_zst.csv.zst'} into table t fields terminated by ',' lines terminated by '\n' ignore 1 lines;
select id, name from t order by id;

-- 2. zstd, explicit compression option
delete from t;
load data infile {'filepath'='$resources/load_data/zipzstd/rows_zst.csv.zst', 'compression'='zstd'} into table t fields terminated by ',' lines terminated by '\n' ignore 1 lines;
select count(*), sum(id) from t;

-- 3. zip with one deflated entry
delete from t;
load data infile {'filepath'='$resources/load_data/zipzstd/rows_zip.csv.zip'} into table t fields terminated by ',' lines terminated by '\n' ignore 1 lines;
select id, name from t order by id;

-- 4. zip with one stored (uncompressed) entry
delete from t;
load data infile {'filepath'='$resources/load_data/zipzstd/stored.zip', 'compression'='zip'} into table t fields terminated by ',' lines terminated by '\n' ignore 1 lines;
select id, name from t order by id;

-- 5. zip with two files: only the first (export/a.csv) is loaded
delete from t;
load data infile {'filepath'='$resources/load_data/zipzstd/two.zip'} into table t fields terminated by ',' lines terminated by '\n' ignore 1 lines;
select id, name from t order by id;

-- 6. LOAD DATA LOCAL streams the client's bytes: zip and zstd both work
delete from t;
load data local infile {'filepath'='$resources/load_data/zipzstd/rows_zip.csv.zip'} into table t fields terminated by ',' lines terminated by '\n' ignore 1 lines;
load data local infile {'filepath'='$resources/load_data/zipzstd/rows_zst.csv.zst'} into table t fields terminated by ',' lines terminated by '\n' ignore 1 lines;
select count(*), sum(id) from t;

-- 7. a glob mixing zstd, lz4, zip and plain csv, detected per file
delete from t;
load data infile {'filepath'='$resources/load_data/zipzstd/mix/m_*'} into table t fields terminated by ',' lines terminated by '\n' ignore 1 lines parallel 'true';
select id, name from t order by id;

-- 8. a file that is not a zip archive, named as one, is an error rather
-- than rows of garbage
delete from t;
load data infile {'filepath'='$resources/load_data/zipzstd/mix/m_3.csv', 'compression'='zip'} into table t fields terminated by ',' lines terminated by '\n' ignore 1 lines;
select count(*) from t;

-- 9. external tables read the same formats
create external table ext_zst(id int, name varchar(20)) infile{'filepath'='$resources/load_data/zipzstd/rows_zst.csv.zst'} fields terminated by ',' lines terminated by '\n' ignore 1 lines;
select id, name from ext_zst order by id;
create external table ext_zip(id int, name varchar(20)) infile{'filepath'='$resources/load_data/zipzstd/rows_zip.csv.zip'} fields terminated by ',' lines terminated by '\n' ignore 1 lines;
select id, name from ext_zip order by id;
create external table ext_mix(id int, name varchar(20)) infile{'filepath'='$resources/load_data/zipzstd/mix/m_*'} fields terminated by ',' lines terminated by '\n' ignore 1 lines;
select count(*), sum(id) from ext_mix;

drop database load_zz;
