-- issue #7501: saved DML RETURNING results become visible only through meta publication.
drop database if exists query_result_dml_returning;
create database query_result_dml_returning;
use query_result_dml_returning;
create table t(id int primary key, v varchar(20));

set save_query_result = on;
/* save_result */insert into t values (1, 'inserted') returning id, v;
set @insert_result = last_query_id();
select * from result_scan(@insert_result) as r;

/* save_result */update t set v = 'updated' where id = 1 returning id, v;
set @update_result = last_query_id();
select * from result_scan(@update_result) as r;

/* save_result */delete from t where id = 1 returning id, v;
set @delete_result = last_query_id();
select * from result_scan(@delete_result) as r;

/* save_result */delete from t where id = 999 returning id, v;
set @empty_result = last_query_id();
select count(*) from result_scan(@empty_result) as r;

set query_result_maxsize = 0;
/* save_result */insert into t values (3, 'truncated') returning id, v;
set @truncated_result = last_query_id();
select * from result_scan(@truncated_result) as r;
set query_result_maxsize = 100;

set save_query_result = off;
insert into t values (2, 'not-saved') returning id, v;
select * from result_scan(last_query_id()) as r;

drop database query_result_dml_returning;
