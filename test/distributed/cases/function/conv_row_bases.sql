-- @suite
-- @case
drop database if exists conv_row_bases;
create database conv_row_bases;
use conv_row_bases;
create table t(id int primary key,n varchar(64),f bigint,t bigint);
insert into t values (1,'ff',16,10),(2,'1010',2,16),(3,'-10',10,-16),(4,'z',36,10),(5,'10',null,10),(6,'10',1,10);
select id,conv(n,f,t) from t order by id;
select id,conv(n,cast(f as int),cast(t as int)) from t order by id;
select id,conv(n,case when id=1 then 16 else f end,t) from t order by id;
select conv('10',cast(18446744073709551615 as unsigned),10),conv('10',-9223372036854775808,10);
prepare p from 'select conv(?,?,?)';
set @n='ff',@f=16,@t=10;
execute p using @n,@f,@t;
set @n='1010',@f=2,@t=16;
execute p using @n,@f,@t;
set @f=null;
execute p using @n,@f,@t;
deallocate prepare p;
drop database conv_row_bases;
