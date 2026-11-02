-- #31720: a NaN/+Inf/-Inf DOUBLE passed through EXECUTE ... USING must keep
-- its value, so prepared PERCENTILE_CONT/PERCENTILE_DISC/APPROX_PERCENTILE
-- reject it like the direct constant instead of reading it as 0.
drop database if exists prepared_nonfinite_float_param;
create database prepared_nonfinite_float_param;
use prepared_nonfinite_float_param;
create table vals(v int);
insert into vals values (1),(2),(3);

prepare cont_p from 'select percentile_cont(?) within group (order by v) from vals';
prepare disc_p from 'select percentile_disc(?) within group (order by v) from vals';
prepare approx_p from 'select approx_percentile(v, ?) from vals';
prepare mul_p from 'select cast(? * 1e0 as char) as product, cast(cast(? as double) as char) as as_double';

set @p = bit_cast(unhex('000000000000f87f') as double);
select cast(@p as char) as p, cast(@p * 1e0 as char) as product;
execute mul_p using @p, @p;
execute cont_p using @p;
execute disc_p using @p;
execute approx_p using @p;

set @p = bit_cast(unhex('000000000000f07f') as double);
execute mul_p using @p, @p;
execute cont_p using @p;
execute disc_p using @p;

set @p = bit_cast(unhex('000000000000f0ff') as double);
execute mul_p using @p, @p;
execute cont_p using @p;
execute disc_p using @p;

-- Controls: valid and out-of-range finite values, text parameters.
set @p = 0.5e0;
execute cont_p using @p;
execute disc_p using @p;
set @p = 1.5e0;
execute cont_p using @p;
set @p = '0.5';
execute cont_p using @p;
set @p = 'NaN';
execute mul_p using @p, @p;

deallocate prepare cont_p;
deallocate prepare disc_p;
deallocate prepare approx_p;
deallocate prepare mul_p;
drop database prepared_nonfinite_float_param;
