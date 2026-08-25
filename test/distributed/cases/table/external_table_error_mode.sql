-- external table error mode: __mo_file_line, __mo_error_message, __mo_error_text
drop database if exists exterr;
create database exterr;
use exterr;

-- ---------------------------------------------------------------- CSV
create external table em_csv (a int, s varchar(20))
infile{'filepath'='$resources/external_table_file/error_mode.csv'}
fields terminated by ',' lines terminated by '\n';

-- the three columns are hidden: not in `select *`, not in the table shape
select * from em_csv;
desc em_csv;
show create table em_csv;

-- a query that does not ask to see errors fails on the first bad record,
-- exactly as it did before error mode existed
select a, s from em_csv;
select count(*) from em_csv;
-- __mo_file_line alone is position metadata, not a request to tolerate
select a, s, __mo_file_line from em_csv;

-- asking for either error column reports the bad records instead
select a, s, __mo_file_line, __mo_error_message, __mo_error_text from em_csv;
select a, s, __mo_file_line, __mo_error_text from em_csv;
select count(*) from (select __mo_error_message from em_csv) t;

-- a record's failure is a property of the record, not of the projection:
-- these all report the same one bad record even though the column that fails
-- to convert is not selected
select __mo_error_message from em_csv;
select s, __mo_error_message from em_csv;
select __mo_file_line, __mo_error_text from em_csv where __mo_error_message is not null;

-- the good records are usable on their own
select a, s from (select a, s, __mo_error_message from em_csv) t where __mo_error_message is null;
select sum(a) from (select a, __mo_error_message from em_csv) t where __mo_error_message is null;
-- and the bad ones can be counted or loaded into a rejects table
select count(*) from (select __mo_error_message from em_csv) t where __mo_error_message is not null;

drop table if exists em_good;
create table em_good (a int, s varchar(20));
insert into em_good select a, s from (select a, s, __mo_error_message from em_csv) t where __mo_error_message is null;
select * from em_good order by a;

drop table if exists em_rejects;
create table em_rejects (line bigint, msg varchar(500), txt varchar(500));
insert into em_rejects
  select __mo_file_line, __mo_error_message, __mo_error_text from em_csv
  where __mo_error_message is not null;
select line, txt from em_rejects order by line;

-- a record spanning two physical lines reports the line it starts on
create external table em_multi (a int, s varchar(20))
infile{'filepath'='$resources/external_table_file/error_mode_multiline.csv'}
fields terminated by ',' enclosed by '"' lines terminated by '\n';
select a, s, __mo_file_line, __mo_error_message from em_multi;

-- ---------------------------------------------------------------- JSONLINE
create external table em_json (a int, s varchar(20))
infile{'filepath'='$resources/external_table_file/error_mode.jsonl', 'format'='jsonline', 'jsondata'='object'};

select * from em_json;
select a, s from em_json;
select a, s, __mo_file_line, __mo_error_message, __mo_error_text from em_json;

-- a blank line is not a record, and does not shift the lines after it
create external table em_json_blank (a int, s varchar(20))
infile{'filepath'='$resources/external_table_file/error_mode_blank.jsonl', 'format'='jsonline', 'jsondata'='object'};
select a, s, __mo_file_line, __mo_error_message from em_json_blank;

-- an object the file never closes is reported, not dropped
create external table em_json_trunc (a int, s varchar(20))
infile{'filepath'='$resources/external_table_file/error_mode_trunc.jsonl', 'format'='jsonline', 'jsondata'='object'};
select a, s, __mo_file_line, __mo_error_message, __mo_error_text from em_json_trunc;

-- ---------------------------------------------------------------- parquet
-- Parquet is decoded as typed columnar values, not text: there is no line and
-- no record text to report, so the error-mode columns do not resolve there.
-- Ordinary reads are unaffected.
create external table em_parquet (`sepal.length` double, `sepal.width` double,
  `petal.length` double, `petal.width` double, variety varchar(20))
infile{'filepath'='$resources/parquet/Iris.parquet', 'format'='parquet'};
select count(*) from em_parquet;
select variety, __mo_error_message from em_parquet limit 1;

-- ---------------------------------------------------------------- reserved names
-- the column names are reserved: a user table cannot declare them
drop table if exists em_reserved;
create table em_reserved (a int, __mo_error_message varchar(10));
create table em_reserved (a int, __mo_file_line int);
create external table em_reserved (a int, __mo_error_text varchar(10))
infile{'filepath'='$resources/external_table_file/error_mode.csv'};

drop database if exists exterr;
