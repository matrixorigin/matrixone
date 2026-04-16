-- test load data with ignore error rows
drop table if exists t_ignore_error;
create table t_ignore_error(
col1 int,
col2 varchar(50),
col3 int
);

-- without IGNORE, csv parse error causes failure
-- @bvt:issue#23937
load data infile '$resources/load_data/ignore_parse_error.csv' into table t_ignore_error fields terminated by ',';
-- @bvt:issue

truncate table t_ignore_error;

-- with IGNORE, only csv parse error rows are skipped
load data infile '$resources/load_data/ignore_parse_error.csv' ignore into table t_ignore_error fields terminated by ',';
select * from t_ignore_error order by col1;

truncate table t_ignore_error;

-- with IGNORE, type conversion error should still fail
-- @bvt:issue#23937
load data infile '$resources/load_data/ignore_error.csv' ignore into table t_ignore_error fields terminated by ',';
-- @bvt:issue

drop table t_ignore_error;
