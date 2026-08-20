drop database if exists found_rows_regression;
create database found_rows_regression;
use found_rows_regression;

create table t(id bigint);
insert into t select result from generate_series(1, 100000, 1) g;

-- Without LIMIT, both the returned and saved counts are the complete result.
select sql_calc_found_rows id from t where id <= 3 order by id;
select found_rows() as calc_without_limit;

-- LIMIT 0 must still scan the complete qualifying input.
select sql_calc_found_rows id from t limit 0;
select found_rows() as after_zero;

-- The final coordinator owns counting across parallel and remote scan scopes.
select sql_calc_found_rows id from t order by id limit 2;
select found_rows() as after_parallel;

-- OFFSET changes the rows returned, not the complete qualifying count.
select sql_calc_found_rows id from t order by id limit 2 offset 10;
select found_rows() as after_offset;

-- An empty qualifying result publishes zero rather than retaining stale state.
select sql_calc_found_rows id from t where id < 0 limit 1;
select found_rows() as after_empty;

-- Nested semantic limits must not own or publish the outer FOUND_ROWS count.
select sql_calc_found_rows *
from (select id from t order by id limit 5) d
where id <= 3
limit 1;
select found_rows() as after_derived_limit;

with d as (select id from t order by id limit 7)
select sql_calc_found_rows * from d where id <= 4 limit 1;
select found_rows() as after_cte_limit;

-- A cached prepared pipeline must initialize FOUND_ROWS state on every execute.
prepare ps_found_rows from 'select sql_calc_found_rows id from t order by id limit 1';
execute ps_found_rows;
select found_rows() as after_prepared_first;
execute ps_found_rows;
select found_rows() as after_prepared_second;
deallocate prepare ps_found_rows;

prepare ps_nested_found_rows from 'select sql_calc_found_rows * from (select id from t order by id limit 5) d where id <= 3 limit 1';
execute ps_nested_found_rows;
select found_rows() as after_nested_prepared_first;
execute ps_nested_found_rows;
select found_rows() as after_nested_prepared_second;
deallocate prepare ps_nested_found_rows;

select id from t order by id limit 2;
select found_rows() as after_plain;

select id from t where id <= 3 order by id;
select found_rows() as plain_without_limit;

-- Status statements and failed queries do not overwrite the last successful
-- result-set count.
select id from t order by id limit 2;
create table status_does_not_replace_found_rows(id int);
select found_rows() as after_status;

select id from t order by id limit 2;
-- @regex("does not exist",true)
select * from missing_found_rows_table;
select found_rows() as after_error;

drop database found_rows_regression;
