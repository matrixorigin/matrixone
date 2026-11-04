drop database if exists decimal_temporal_arithmetic_domain;
create database decimal_temporal_arithmetic_domain;
use decimal_temporal_arithmetic_domain;
select cast(1000000000000000000 as signed) + cast('00:00:00' as time) as n;
select cast('00:00:00' as time) + cast(1000000000000000000 as signed) as n;
select cast(-1000000000000000000 as signed) + cast('00:00:00' as time) as n;
select cast('00:00:00' as time) + cast(-1000000000000000000 as signed) as n;
create table t(i bigint, tm time(0));
insert into t values(-1000000000000000000,'00:00:02'),(-999999999999999999,'00:00:02'),(999999999999999999,'00:00:02'),(1000000000000000000,'00:00:02'),(null,'00:00:02');
select i,
       i+tm = cast(i as decimal(38,0))+2 as plus_ok,
       tm+i = cast(i as decimal(38,0))+2 as reverse_plus_ok,
       i-tm = cast(i as decimal(38,0))-2 as minus_ok,
       tm-i = 2-cast(i as decimal(38,0)) as reverse_minus_ok,
       i*tm = cast(i as decimal(38,0))*2 as multiply_ok,
       tm*i = cast(i as decimal(38,0))*2 as reverse_multiply_ok,
       i%tm = i%2 as modulo_ok,
       tm%i = 2 as reverse_modulo_ok,
       i div tm = i div 2 as div_ok,
       tm div i = 0 as reverse_div_ok,
       i/tm = cast(i as decimal(38,0))/2 as quotient_ok
from t order by i;
create table tf(i bigint, u bigint unsigned, b bit(64), tm time(6));
insert into tf values(1000000000000000000,18446744073709551615,18446744073709551615,'00:00:01.125000');
select i+tm = cast('1000000000000000001.125000' as decimal(30,6)) as signed_ok,
       u+tm = cast('18446744073709551616.125000' as decimal(30,6)) as unsigned_ok,
       tm+u = cast('18446744073709551616.125000' as decimal(30,6)) as reverse_unsigned_ok,
       b+tm = cast('18446744073709551616.125000' as decimal(30,6)) as bit_ok,
       tm+b = cast('18446744073709551616.125000' as decimal(30,6)) as reverse_bit_ok
from tf;
prepare p from "select cast(? as signed)+cast('00:00:00' as time) as n";
set @n=1000000000000000000;
execute p using @n;
set @n=-1000000000000000000;
execute p using @n;
set @n=null;
execute p using @n;
deallocate prepare p;
set @saved_arith_sql_mode=@@session.sql_mode;
set session sql_mode='STRICT_TRANS_TABLES';
create table narrow_t(d decimal(18,0));
insert into narrow_t select i from t where i=1000000000000000000;
select count(*) as rejected_rows from narrow_t;
set session sql_mode=@saved_arith_sql_mode;
drop database decimal_temporal_arithmetic_domain;
