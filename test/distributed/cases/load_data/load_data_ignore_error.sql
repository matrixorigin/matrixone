-- test load data with ignore error rows
drop table if exists t_ignore_error;
create table t_ignore_error(
col1 int,
col2 varchar(50),
col3 int
);

-- without IGNORE, error rows cause failure (col3 'abc' is not int)
-- @bvt:issue#23937
load data infile '$resources/load_data/ignore_error.csv' into table t_ignore_error fields terminated by ',';
-- @bvt:issue

-- with IGNORE, error rows are skipped
load data infile '$resources/load_data/ignore_error.csv' ignore into table t_ignore_error fields terminated by ',';
select * from t_ignore_error order by col1;

drop table t_ignore_error;
