-- @suite
-- @case
-- @desc: #30180 statistical aggregates consume BOOL values and ENUM ordinals
-- @label:bvt

drop database if exists stat_bool_enum;
create database stat_bool_enum;
use stat_bool_enum;
create table stat_inputs (id int primary key, g int, b boolean, e enum('b','a','c'));
insert into stat_inputs values (1,1,true,'b'), (2,1,false,'a'), (3,1,true,'c'), (4,1,null,null), (5,2,false,'a'), (6,3,null,null);

-- Independent rounded numeric oracles: BOOL is 1,0,1; ENUM is 1,2,3.
select cast(var_pop(b) as decimal(18,6)) as b_var_pop, cast(var_samp(b) as decimal(18,6)) as b_var_samp, cast(stddev_pop(b) as decimal(18,6)) as b_stddev_pop, cast(stddev_samp(b) as decimal(18,6)) as b_stddev_samp from stat_inputs where g = 1;
select cast(var_pop(e) as decimal(18,6)) as e_var_pop, cast(var_samp(e) as decimal(18,6)) as e_var_samp, cast(stddev_pop(e) as decimal(18,6)) as e_stddev_pop, cast(stddev_samp(e) as decimal(18,6)) as e_stddev_samp from stat_inputs where g = 1;
select variance(b) = var_pop(b) as b_variance, std(b) = stddev_pop(b) as b_std, stddev(b) = stddev_pop(b) as b_stddev, variance(e) = var_pop(e) as e_variance, std(e) = stddev_pop(e) as e_std, stddev(e) = stddev_pop(e) as e_stddev from stat_inputs where g = 1;

-- Duplicates must be removed after numeric coercion, NULL must be skipped.
select cast(var_pop(distinct b) as decimal(18,6)) as b_pop, cast(var_samp(distinct b) as decimal(18,6)) as b_sample, cast(var_pop(distinct e) as decimal(18,6)) as e_pop from stat_inputs where g = 1;

-- Group 2 is a singleton; group 3 is all NULL. Empty input remains NULL.
select g, cast(var_pop(b) as decimal(18,6)) as b_var_pop, cast(var_samp(b) as decimal(18,6)) as b_var_samp, cast(stddev_pop(b) as decimal(18,6)) as b_stddev_pop, cast(stddev_samp(b) as decimal(18,6)) as b_stddev_samp, cast(var_pop(e) as decimal(18,6)) as e_var_pop, cast(var_samp(e) as decimal(18,6)) as e_var_samp, cast(stddev_pop(e) as decimal(18,6)) as e_stddev_pop, cast(stddev_samp(e) as decimal(18,6)) as e_stddev_samp from stat_inputs group by g order by g;
select cast(var_pop(b) as decimal(18,6)) as b_var_pop, cast(var_samp(b) as decimal(18,6)) as b_var_samp, cast(stddev_pop(b) as decimal(18,6)) as b_stddev_pop, cast(stddev_samp(b) as decimal(18,6)) as b_stddev_samp, cast(var_pop(e) as decimal(18,6)) as e_var_pop, cast(var_samp(e) as decimal(18,6)) as e_var_samp, cast(stddev_pop(e) as decimal(18,6)) as e_stddev_pop, cast(stddev_samp(e) as decimal(18,6)) as e_stddev_samp from stat_inputs where id = 0;

-- Window binding shares the numeric contract; the fourth row is NULL.
select id, cast(var_pop(b) over (order by id rows between unbounded preceding and current row) as decimal(18,6)) as b_pop, cast(var_samp(b) over (order by id rows between unbounded preceding and current row) as decimal(18,6)) as b_sample, cast(var_pop(e) over (order by id rows between unbounded preceding and current row) as decimal(18,6)) as e_pop, cast(var_samp(e) over (order by id rows between unbounded preceding and current row) as decimal(18,6)) as e_sample from stat_inputs where g = 1 order by id;

-- Retained ENUM provenance across a materialized projection must use ordinals.
select cast(var_pop(e) as decimal(18,6)) as e_var_pop, cast(var_samp(e) as decimal(18,6)) as e_var_samp, cast(stddev_pop(e) as decimal(18,6)) as e_stddev_pop, cast(stddev_samp(e) as decimal(18,6)) as e_stddev_samp from (select e from stat_inputs where g = 1 order by id limit 3) d;
select cast(var_pop(cast(e as char)) as decimal(18,6)) as label_variance from stat_inputs where g = 1;

-- Repeated prepared execution preserves both typed table-column domains.
prepare stat_columns from 'select cast(var_pop(b) as decimal(18,6)) as b_var_pop, cast(var_samp(b) as decimal(18,6)) as b_var_samp, cast(stddev_pop(b) as decimal(18,6)) as b_stddev_pop, cast(stddev_samp(b) as decimal(18,6)) as b_stddev_samp, cast(var_pop(e) as decimal(18,6)) as e_var_pop, cast(var_samp(e) as decimal(18,6)) as e_var_samp, cast(stddev_pop(e) as decimal(18,6)) as e_stddev_pop, cast(stddev_samp(e) as decimal(18,6)) as e_stddev_samp from stat_inputs where g = ?';
set @stat_group = 1;
execute stat_columns using @stat_group;
set @stat_group = 2;
execute stat_columns using @stat_group;
set @stat_group = 1;
execute stat_columns using @stat_group;
deallocate prepare stat_columns;

-- A parameter inside a BOOL predicate keeps its integer adapter at EXECUTE.
prepare stat_predicate from 'select cast(var_pop(id > ?) as decimal(18,6)) as p, cast(var_samp(id > ?) as decimal(18,6)) as s from stat_inputs where g = 1';
set @stat_limit = 1;
execute stat_predicate using @stat_limit, @stat_limit;
set @stat_limit = 2;
execute stat_predicate using @stat_limit, @stat_limit;
deallocate prepare stat_predicate;
set @stat_group = null;
set @stat_limit = null;
drop database stat_bool_enum;
