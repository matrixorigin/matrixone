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

-- Pagination confined to a derived table or CTE bounds the outer result. It
-- must not be mistaken for final-result pagination when the outer SELECT has
-- no explicit LIMIT/OFFSET.
select sql_calc_found_rows *
from (select id from t order by id limit 5) d;
select found_rows() as after_derived_only_limit;

with d as (select id from t order by id limit 7)
select sql_calc_found_rows * from d;
select found_rows() as after_cte_only_limit;

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

-- Ordered regular-index Top-K pushdown must not truncate the stream counted by
-- SQL_CALC_FOUND_ROWS.
create table indexed_t(id bigint primary key, grp int, index idx_grp(grp));
insert into indexed_t values (1, 1), (2, 1), (3, 1), (4, 2);
select sql_calc_found_rows id from indexed_t where grp = 1 order by id limit 1;
select found_rows() as after_ordered_index;

-- FOUND_ROWS() itself must be evaluated on the coordinator when the scan is
-- distributed to remote CNs.
select sql_calc_found_rows id from t where id <= 3 limit 1;
select found_rows(), id from t where id <= 3 order by id;

-- The session row cap is an implicit top-level LIMIT and must own the complete
-- SQL_CALC_FOUND_ROWS count for ordinary and cached prepared execution.
set sql_select_limit = 1;
select sql_calc_found_rows id from t where id <= 3 order by id;
select found_rows() as after_sql_select_limit;
select sql_calc_found_rows * from (select id from t order by id limit 5) d;
select found_rows() as after_nested_sql_select_limit;
prepare ps_session_limit from 'select sql_calc_found_rows id from t where id <= 3 order by id';
execute ps_session_limit;
select found_rows() as after_prepared_sql_select_limit;
execute ps_session_limit;
select found_rows() as after_prepared_sql_select_limit_second;
deallocate prepare ps_session_limit;

-- Repeated prepared execution must let the dynamic final session limit own
-- counting even when the statement also contains nested semantic pagination.
prepare ps_nested_session_limit from 'select sql_calc_found_rows * from (select id from t order by id limit 5) d';
execute ps_nested_session_limit;
select found_rows() as after_nested_session_limit_first;
execute ps_nested_session_limit;
select found_rows() as after_nested_session_limit_second;
deallocate prepare ps_nested_session_limit;

-- A dynamic session LIMIT above an explicit final OFFSET must drain so the
-- OFFSET owner can observe EOF and publish the pre-offset count. Reuse must
-- also survive finite/unlimited session-limit transitions.
prepare ps_offset_session_limit from 'select sql_calc_found_rows id from t where id <= 5 order by id offset 2';
execute ps_offset_session_limit;
select found_rows() as after_offset_session_limit_first;
execute ps_offset_session_limit;
select found_rows() as after_offset_session_limit_second;
set sql_select_limit = 0;
execute ps_offset_session_limit;
-- The explicit LIMIT lets this diagnostic SELECT run while the session cap is
-- zero; it must observe the count published by the preceding execution.
select found_rows() as after_offset_session_limit_zero limit 1;
set sql_select_limit = 18446744073709551615;
execute ps_offset_session_limit;
select found_rows() as after_offset_session_limit_unlimited;
set sql_select_limit = 1;
execute ps_offset_session_limit;
select found_rows() as after_offset_session_limit_restored;
deallocate prepare ps_offset_session_limit;
set sql_select_limit = 18446744073709551615;

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
