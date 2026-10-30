-- External CSV preserves decimal assignment values and whitespace field records.
-- Oversized mantissas must be scaled and rounded before destination range checks.
-- Numeric text uses exact assignment rounding; approximate SQL FLOAT ties have
-- separate semantics. The whitespace fixture includes a genuine blank line,
-- quoted empty/space, NULL, and a final space without a line terminator.
drop database if exists csv_value_assignment;
create database csv_value_assignment;
use csv_value_assignment;

create external table csv_numbers(a tinyint, b smallint, c int, d bigint, ua tinyint unsigned, ub smallint unsigned, uc int unsigned, ud bigint unsigned, m mediumint, um mediumint unsigned, exact_value bigint)
infile{'filepath'='$resources/external_table_file/csv_integer_assignment.csv', 'format'='csv'}
fields terminated by ',' lines terminated by '\n';
select * from csv_numbers;
create table native_numbers(a tinyint, b smallint, c int, d bigint, ua tinyint unsigned, ub smallint unsigned, uc int unsigned, ud bigint unsigned, m mediumint, um mediumint unsigned, exact_value bigint);
insert into native_numbers values (1.9,2.4,1e2,-1.9,2.5,3.9,4.5,18446744073709551615.0,8388606.9,16777214.9,9007199254740993.0);
select count(*) as mismatches from csv_numbers e cross join native_numbers n
where e.a <> n.a or e.b <> n.b or e.c <> n.c or e.d <> n.d
or e.ua <> n.ua or e.ub <> n.ub or e.uc <> n.uc or e.ud <> n.ud
or e.m <> n.m or e.um <> n.um or e.exact_value <> n.exact_value;

create external table csv_spaces(v varchar(8))
infile{'filepath'='$resources/external_table_file/csv_whitespace_records.csv', 'format'='csv'}
fields terminated by ',' enclosed by '"' escaped by '\\' lines terminated by '\n';
select count(*) as records, count(v) as non_null from csv_spaces;
select __mo_file_line as line, concat('[',hex(v),']') as value_hex from csv_spaces order by line;

-- The shared parser preserves the same VARCHAR records for LOAD DATA.
create table loaded_spaces(v varchar(8));
load data infile '$resources/external_table_file/csv_whitespace_records.csv'
into table loaded_spaces fields terminated by ',' enclosed by '"' escaped by '\\' lines terminated by '\n';
select count(*) as records, count(v) as non_null from loaded_spaces;
select coalesce(concat('[',hex(v),']'),'NULL') as value_hex, count(*) as n
from loaded_spaces group by v order by value_hex;

-- Check range after rounding, before narrowing or publishing a value.
create external table csv_overflow(v tinyint)
infile{'filepath'='$resources/external_table_file/csv_integer_overflow.csv', 'format'='csv'}
fields terminated by ',' lines terminated by '\n';
select v from csv_overflow;
select v is null as rejected, __mo_file_line as line,
__mo_error_message like '%not int8 type%' as conversion_error from csv_overflow;
-- JSONLINE retains its format-specific blank-record policy.
create external table json_lines(a int, s varchar(8))
infile{'filepath'='$resources/external_table_file/jsonline_whitespace_records.jsonl', 'format'='jsonline', 'jsondata'='object'};
select a, s, __mo_file_line as line from json_lines order by a;
select a, s, __mo_file_line as line, __mo_error_message is null as valid_record
from json_lines order by a;
create table loaded_json_lines(a int, s varchar(8));
load data infile{'filepath'='$resources/external_table_file/jsonline_whitespace_records.jsonl', 'format'='jsonline', 'jsondata'='object'} into table loaded_json_lines;
select a, s from loaded_json_lines order by a;
drop database csv_value_assignment;
