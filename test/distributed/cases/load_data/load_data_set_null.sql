drop table if exists t1;
create table t1(
col1 int,
col2 varchar(100),
col3 float,
col4 date,
col5 text
);

load data infile "$resources/load_data/set_null_1.csv" into table t1 set a=nullif(col1,'1');

load data infile "$resources/load_data/set_null_1.csv" into table t1 set col2=nullif(col3,'1');

load data infile "$resources/load_data/not_exists.csv" into table t1;

load data infile "$resources/load_data/set_null_1.csv" into table t1 set col1=nullif(col1,'1');
select * from t1;

load data infile "$resources/load_data/set_null_1.csv" into table t1 set col2=nullif(col2,'1');
select * from t1;

load data infile "$resources/load_data/set_null_1.csv" into table t1 set col2=nullif(col3,'"1111-11-11"');
select * from t1;

load data infile "$resources/load_data/set_null_1.csv" into table t1 set col4=nullif(col4,'1');
select * from t1;

load data infile "$resources/load_data/set_null_1.csv" into table t1 set col1=nullif(col1,1), col2=nullif(col2,1),col3=nullif(col3,1) ,col4=nullif(col4,'1111-11-11'),col5=nullif(col5,1);
select * from t1;

load data infile "$resources/load_data/set_null_2.csv" into table t1 set col1=nullif(col1,1), col2=nullif(col2,2),col3=nullif(col3,2) ,col4=nullif(col4,'1111-04-11'),col5=nullif(col5,5);
select * from t1;

drop table t1;

drop table if exists t2;
create table t2(
col1 int primary key auto_increment,
col2 varchar(100)
);

load data infile "$resources/load_data/set_null_3.csv" into table t2 set col1=nullif(col1,'null');
select * from t2;
delete from t2;
insert into t2 values();
select * from t2;

-- test load character set
load data infile "$resources/load_data/set_null_3.csv" into table t2 character set utf8 fields terminated by ',' set col1=nullif(col1,'null');

drop table t2;
