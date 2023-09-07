drop table if exists test_table;
create table test_table(
col1 int,
col2 float,
col3 bool,
col4 Date,
col5 varchar(255),
col6 text
);

-- @setup
set global save_query_result = on;

-- show sql result cache
show create table test_table;
select * from result_scan(last_query_id()) as u;
show tables;
select * from result_scan(last_query_id()) as u;
show databases like "mysql";
select * from result_scan(last_query_id()) as u;

-- test select table result cache
load data infile '$resources/load_data/test_1.csv' into table test_table;
/* save_result */select * from test_table;
select * from result_scan(last_query_id()) as u;
/* save_result */select col1 from test_table;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from test_table where col1 > 30;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from test_table where col1 < 10;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from test_table where col1 = 10;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from test_table limit 1;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from test_table order by col1 asc;
select * from result_scan(last_query_id()) as u;

/* save_result */select t1.col1,t2.col1 from test_table t1  left join test_table t2 on t1.col1=t2.col1;
select * from result_scan(last_query_id()) as u;
/* save_result */select t1.col1,t2.col1 from test_table t1  right join test_table t2 on t1.col1=t2.col1;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from test_table union select * from test_table;
select * from result_scan(last_query_id()) as u;

/* save_result */SELECT col1 FROM test_table where col1 < 30 MINUS SELECT col1  FROM test_table where col1 < 20;
select * from result_scan(last_query_id()) as u;

-- test view result cache
create view test_view as select * from test_table;
show tables;
select * from result_scan(last_query_id()) as u;
show create view test_view;
select * from result_scan(last_query_id()) as u;

/* save_result */select * from test_view;
select * from result_scan(last_query_id()) as u;
/* save_result */select col1 from test_view;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from test_view where col1 > 30;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from test_view where col1 < 10;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from test_view where col1 = 10;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from test_view limit 1;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from test_view order by col1 asc;
select * from result_scan(last_query_id()) as u;


/* save_result */select t1.col1,t2.col1 from test_view t1  left join test_view t2 on t1.col1=t2.col1;
select * from result_scan(last_query_id()) as u;
/* save_result */select t1.col1,t2.col1 from test_view t1  right join test_view t2 on t1.col1=t2.col1;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from test_view union select * from test_view;
select * from result_scan(last_query_id()) as u;

/* save_result */SELECT col1 FROM test_view where col1 < 30 MINUS SELECT col1  FROM test_view where col1 < 20;
select * from result_scan(last_query_id()) as u;


-- test transactions result cache
begin;
/* save_result */select * from test_table;
select * from result_scan(last_query_id()) as u;
/* save_result */select col1 from test_table;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from test_table where col1 > 30;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from test_table where col1 < 10;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from test_table where col1 = 10;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from test_table limit 1;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from test_table order by col1 asc;
select * from result_scan(last_query_id()) as u;

/* save_result */select t1.col1,t2.col1 from test_table t1  left join test_table t2 on t1.col1=t2.col1;
select * from result_scan(last_query_id()) as u;
/* save_result */select t1.col1,t2.col1 from test_table t1  right join test_table t2 on t1.col1=t2.col1;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from test_table union select * from test_table;
select * from result_scan(last_query_id()) as u;

/* save_result */SELECT col1 FROM test_view where col1 < 30 MINUS SELECT col1  FROM test_view where col1 < 20;
select * from result_scan(last_query_id()) as u;
rollback;

begin;
/* save_result */select * from test_table;
rollback;
select * from result_scan(last_query_id(-2)) as u;

-- test prepare select sql
set @xxx=10;
prepare s1 from select * from test_table where col1<?;
execute s1 using @xxx;
select * from result_scan(last_query_id(-1)) as u;
deallocate prepare s1;


-- test save_query_result config
set global save_query_result = off;
/* save_result */select * from test_table;
select * from result_scan(last_query_id()) as u;
set global save_query_result = on;

-- @teardown
set global save_query_result = off;

set global save_query_result = on;
drop table if exists t1;
create table t1(a int);
show columns from t1;
select * from result_scan(last_query_id()) as t;
set global save_query_result = off;

