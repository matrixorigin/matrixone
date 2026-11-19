-- A final unclosed enclosure is literal data for serial CSV LOAD.
set @saved_sql_mode = @@sql_mode;
set sql_mode = default;
drop database if exists load_unterminated_enclosure;
create database load_unterminated_enclosure;
use load_unterminated_enclosure;
create table t(a int, b varchar(20));

-- Default sql_mode: server and LOCAL input.
load data infile '$resources/load_data/unterminated_enclosure.csv' into table t fields terminated by ',' lines terminated by '\n';
show count(*) warnings;
select a, hex(b), length(b) from t order by a;
truncate table t;
load data local infile '$resources/load_data/unterminated_enclosure.csv' into table t fields terminated by ',' lines terminated by '\n';
show count(*) warnings;
select a, hex(b), length(b) from t order by a;

-- Empty sql_mode: server and LOCAL input.
set sql_mode = '';
truncate table t;
load data infile '$resources/load_data/unterminated_enclosure.csv' into table t fields terminated by ',' lines terminated by '\n';
show count(*) warnings;
select a, hex(b), length(b) from t order by a;
truncate table t;
load data local infile '$resources/load_data/unterminated_enclosure.csv' into table t fields terminated by ',' lines terminated by '\n';
show count(*) warnings;
select a, hex(b), length(b) from t order by a;

-- Explicit enclosure and a normally closed final field, both input paths.
truncate table t;
load data infile '$resources/load_data/unterminated_enclosure.csv' into table t fields terminated by ',' enclosed by '"' lines terminated by '\n';
show count(*) warnings;
select a, hex(b), length(b) from t order by a;
truncate table t;
load data infile '$resources/load_data/closed_enclosure.csv' into table t fields terminated by ',' lines terminated by '\n';
show count(*) warnings;
select a, hex(b), length(b) from t order by a;
truncate table t;
load data local infile '$resources/load_data/closed_enclosure.csv' into table t fields terminated by ',' lines terminated by '\n';
show count(*) warnings;
select a, hex(b), length(b) from t order by a;

drop database load_unterminated_enclosure;
set sql_mode = @saved_sql_mode;
