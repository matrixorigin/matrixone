drop database if exists test_temporary;
create database test_temporary;
use test_temporary;
create temporary table t1(
col1 tinyint,
col2 smallint,
col3 int,
col4 bigint,
col5 tinyint unsigned,
col6 smallint unsigned,
col7 int unsigned,
col8 bigint unsigned
);

-- import data
import data infile '$resources/load_data/integer_numbers_1.csv' into table t1;
select * from t1;

-- into outfile
select * from t1 into outfile '$resources/into_outfile_2/outfile_integer_numbers_2.csv';
delete from t1;

-- import data
import data infile '$resources/into_outfile_2/outfile_integer_numbers_2.csv' into table t1 ignore 1 lines;
select * from t1;
delete from t1;

import data infile '$resources/load_data/integer_numbers_2.csv' into table t1 fields terminated by'*';
select * from t1;
delete from t1;
drop table t1;