drop table if exists t1;
create table t1(ts timestamp);

set time_zone = '+00:00';
load data infile '$resources/load_data/timestamp_timezone.csv' into table t1 fields terminated by ',';
select * from t1;
set time_zone = '+08:00';
select * from t1;
delete from t1;

set time_zone = '-05:00';
load data infile '$resources/load_data/timestamp_timezone.csv' into table t1 fields terminated by ',';
select * from t1;
set time_zone = '+00:00';
select * from t1;

drop table t1;
set time_zone = 'SYSTEM';
