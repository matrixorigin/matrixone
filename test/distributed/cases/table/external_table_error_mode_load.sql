-- Splitting a messy source into a data table and a rejects table in ONE
-- statement, by combining the external error-mode columns with multi-table
-- INSERT. This is the shape a real load takes: the good records land in the
-- destination, every bad record lands in a rejects table with its line number,
-- message and source text, and the statement never fails.
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

-- one statement, two destinations
insert first
  when errmsg is null then into dest (id, name, amount, ts) values (id, name, amount, ts)
  else into rejects (line, msg, txt) values (ln, errmsg, errtxt)
select id, name, amount, ts,
       __mo_file_line as ln, __mo_error_message as errmsg, __mo_error_text as errtxt
from src_csv;

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

insert first
  when errmsg is null then into dest (id, name, amount, ts) values (id, name, amount, ts)
  else into rejects (line, msg, txt) values (ln, errmsg, errtxt)
select id, name, amount, ts,
       __mo_file_line as ln, __mo_error_message as errmsg, __mo_error_text as errtxt
from src_json;

select * from dest order by id;
select line, msg, txt from rejects order by line;
select (select count(*) from dest) + (select count(*) from rejects) as total;

-- ---------------------------------------------------------------- INSERT ALL
-- INSERT ALL is not first-match: a row goes to EVERY branch whose condition it
-- satisfies. The always-true audit branch therefore receives every record,
-- good and bad, while the first two branches split them.
drop table if exists audit;
create table audit (line bigint, failed int);
truncate table dest;
truncate table rejects;
insert all
  when errmsg is null then into dest (id, name, amount, ts) values (id, name, amount, ts)
  when errmsg is not null then into rejects (line, msg, txt) values (ln, errmsg, errtxt)
  when 1 = 1 then into audit (line, failed) values (ln, case when errmsg is null then 0 else 1 end)
select id, name, amount, ts,
       __mo_file_line as ln, __mo_error_message as errmsg, __mo_error_text as errtxt
from src_csv;

select count(*) as dest_rows from dest;
select count(*) as reject_rows from rejects;
select failed, count(*) as cnt from audit group by failed order by failed;
select count(*) as audit_rows from audit;

drop database if exists exterrload;
