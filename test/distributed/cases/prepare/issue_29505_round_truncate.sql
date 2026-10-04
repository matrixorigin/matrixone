-- @case
-- @desc: Prepared ROUND and TRUNCATE retain runtime value domains through projections.
-- @label:bvt

prepare round_value from 'select cast(round(?,1) as double) as v';
set @v = '1.46';
execute round_value using @v;
set @v = 2;
execute round_value using @v;
set @v = '2.5';
execute round_value using @v;
deallocate prepare round_value;

prepare round_scalar from 'select cast(round((select ?),0) as double) as v';
execute round_scalar using @v;
deallocate prepare round_scalar;

prepare truncate_derived from 'select cast(truncate(x,1) as double) as v from (select ? x limit 1) d';
set @v = '1.46';
execute truncate_derived using @v;
deallocate prepare truncate_derived;
