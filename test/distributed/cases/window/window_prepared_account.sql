-- @case
-- @desc: prepared Window releases inherited accounts between executions (issue #31089)
-- @label:bvt

drop database if exists window_prepared_account;
create database window_prepared_account;
use window_prepared_account;
create table t(id int primary key, g int, v int);
insert into t values(1,1,10),(2,1,20),(3,1,30);
select x.id, sum(y.v) over (order by x.id rows unbounded preceding) as running_sum from t x join t y on y.id=x.id where x.id>=1 order by x.id;
prepare window_rows from 'select x.id, sum(y.v) over (order by x.id rows unbounded preceding) as running_sum from t x join t y on y.id=x.id where x.id>=? order by x.id';
set @window_min=1;
execute window_rows using @window_min;
set @window_min=2;
execute window_rows using @window_min;
set @window_min=1;
execute window_rows using @window_min;
deallocate prepare window_rows;
prepare window_range from 'select x.id, sum(y.v) over (order by x.id) as running_sum from t x join t y on y.id=x.id where x.id>=? order by x.id';
execute window_range using @window_min;
deallocate prepare window_range;
prepare window_partitioned from 'select x.id, sum(y.v) over (partition by x.g order by x.id rows unbounded preceding) as running_sum from t x join t y on y.id=x.id where x.id>=? order by x.id';
execute window_partitioned using @window_min;
deallocate prepare window_partitioned;
drop database window_prepared_account;
select count(*) as remaining_schemas from information_schema.schemata where schema_name='window_prepared_account';
