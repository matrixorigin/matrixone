-- Splitting a messy source into a data table and a rejects table by routing on
-- the external error-mode columns. This is the shape a real load takes: the
-- good records land in the destination, every bad record lands in a rejects
-- table with its line number, message and source text, and neither statement
-- fails. (On main this is written as one multi-table INSERT FIRST; 4.2-dev has
-- no multi-table INSERT, so the same routing is expressed as one INSERT per
-- destination over the same scan.)
drop database if exists exterrload;
create database exterrload;
use exterrload;

create table dest (id int, name varchar(20), amount decimal(10,2), ts timestamp);
create table rejects (line bigint, msg varchar(500), txt varchar(500));

-- ---------------------------------------------------------------- CSV
-- errmix.csv carries, in order: a good record, a non-numeric int, a
-- non-numeric decimal, an unparsable timestamp, too few fields, too many
-- fields, an out-of-range int, an embedded quote, an impossible date
-- (2024-02-30), and a good record.
create external table src_csv (id int, name varchar(20), amount decimal(10,2), ts timestamp)
infile{'filepath'='$resources/external_table_file/errmix.csv'}
fields terminated by ',' enclosed by '"' lines terminated by '\n';

-- what the scan makes of each line
select __mo_file_line, id, name, amount, ts, __mo_error_message from src_csv;

-- two destinations, routed on __mo_error_message
insert into dest (id, name, amount, ts)
select id, name, amount, ts from src_csv where __mo_error_message is null;
insert into rejects (line, msg, txt)
select __mo_file_line, __mo_error_message, __mo_error_text
from src_csv where __mo_error_message is not null;

select * from dest order by id;
select line, msg, txt from rejects order by line;
select count(*) as good_rows from dest;
select count(*) as rejected_rows from rejects;
-- every source line is accounted for exactly once
select (select count(*) from dest) + (select count(*) from rejects) as total;

-- the same load without the error columns still fails on the first bad record
truncate table dest;
insert into dest (id, name, amount, ts) select id, name, amount, ts from src_csv;
select count(*) as dest_after_failed_load from dest;

-- ---------------------------------------------------------------- JSONLINE
-- errmix.jsonl carries the same kinds plus two JSON-specific ones: a line that
-- is not JSON at all, and an object the line never closes.
truncate table dest;
truncate table rejects;
create external table src_json (id int, name varchar(20), amount decimal(10,2), ts timestamp)
infile{'filepath'='$resources/external_table_file/errmix.jsonl', 'format'='jsonline', 'jsondata'='object'};

select __mo_file_line, id, name, amount, ts, __mo_error_message from src_json;

insert into dest (id, name, amount, ts)
select id, name, amount, ts from src_json where __mo_error_message is null;
insert into rejects (line, msg, txt)
select __mo_file_line, __mo_error_message, __mo_error_text
from src_json where __mo_error_message is not null;

select * from dest order by id;
select line, msg, txt from rejects order by line;
select (select count(*) from dest) + (select count(*) from rejects) as total;

-- ------------------------------------------------------------- AUDIT OF ALL
-- An audit table that receives EVERY record, good and bad, alongside the split
-- into dest/rejects: the error columns stay readable for a row that also feeds
-- another destination. (On main this is one INSERT ALL with an always-true
-- audit branch.)
drop table if exists audit;
create table audit (line bigint, failed int);
truncate table dest;
truncate table rejects;
insert into dest (id, name, amount, ts)
select id, name, amount, ts from src_csv where __mo_error_message is null;
insert into rejects (line, msg, txt)
select __mo_file_line, __mo_error_message, __mo_error_text
from src_csv where __mo_error_message is not null;
insert into audit (line, failed)
select __mo_file_line, case when __mo_error_message is null then 0 else 1 end
from src_csv;

select count(*) as dest_rows from dest;
select count(*) as reject_rows from rejects;
select failed, count(*) as cnt from audit group by failed order by failed;
select count(*) as audit_rows from audit;

drop database if exists exterrload;
