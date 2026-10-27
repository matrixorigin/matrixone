-- @suite
-- @case
-- @desc: decimal rounding and MySQL textual FLOAT/DOUBLE conversion diagnostics
-- @label:bvt

drop database if exists float_cast_mysql;
create database float_cast_mysql;
use float_cast_mysql;
set @saved_float_cast_mode=@@sql_mode;

-- Literal and runtime DOUBLE paths round the same shortest decimal digits.
select cast(1.005e0 as decimal(10,2)) as positive_tie,
       cast(1.015e0 as decimal(20,2)) as next_tie,
       cast(-1.005e0 as decimal(10,2)) as negative_tie,
       cast('1.005' as decimal(10,2)) as string_control;
select cast(1.0000000000000002e0 as decimal(38,18)) as high_scale,
       cast(1e-18 as decimal(18,18)) as small_value;
create table src(id int, d double, s varchar(32));
insert into src values(1,1.005e0,'1.25'),(2,-1.005e0,'inf'),(3,1.015e0,'nan'),(4,null,null);
select id,cast(d as decimal(10,2)),cast(d as decimal(20,2)),cast(d as decimal(65,2)) from src order by id;

-- Tiny runtime values cover both sides of the fast-path scale limit.
create table tiny_src(id int, d double);
insert into tiny_src values(1,1e-20),(2,-1e-20),(3,null);
select id,cast(d as decimal(16,6)) as tiny64,
       cast(d as decimal(38,19)) as tiny19,
       cast(d as decimal(38,20)) as tiny20
from tiny_src order by id;

-- DOUBLE rounding overflow retains explicit clamping and warning ownership.
insert into tiny_src values(4,999.995e0),(5,-999.995e0);
select id,cast(d as decimal(5,2)) as clamped from tiny_src where id>=4 order by id;
show warnings;

-- SELECT retains truncation warnings even in strict mode; IEEE word tokens
-- are ordinary nonnumeric text while radix remains an explicit extension.
set sql_mode='STRICT_TRANS_TABLES';
select cast('inf' as double),cast('nan' as double),cast('information' as double),cast('-INFINITY' as float),cast('0x10' as double);
show warnings;
select id,cast(s as double) from src order by id;
show warnings;
create table dst(id int, d double);

-- A valid row preceding an invalid row must not leave a partial write.
insert into dst values(1,'1.25'),(2,'inf');
show warnings;
select count(*) from dst;
insert into dst values(1,cast('1.25' as double)),(2,cast('inf' as double));
show warnings;
select count(*) from dst;
insert into dst select id,s from src where id<=2 order by id;
show warnings;
select count(*) from dst;
insert ignore into dst values(1,'inf');
show warnings;
insert ignore into dst values(2,cast('nan' as double));
show warnings;
select * from dst order by id;

-- PREPARE does not evaluate invalid assignments. Each EXECUTE resolves the
-- current mode and binding, including invalid literals in a cached plan.
prepare direct_float from 'insert into dst values(3,?)';
prepare explicit_float from 'insert into dst values(4,cast(? as double))';
prepare literal_float from 'insert into dst values(5,''inf'')';
prepare literal_explicit_float from 'insert into dst values(6,cast(''inf'' as double))';
set @float_text='inf';
execute direct_float using @float_text;
execute explicit_float using @float_text;
execute literal_float;
execute literal_explicit_float;
select count(*) from dst;
set sql_mode='';
execute direct_float using @float_text;
show warnings;
set @float_text='nan';
execute explicit_float using @float_text;
show warnings;
execute literal_float;
execute literal_explicit_float;
select * from dst order by id;
set sql_mode='STRICT_TRANS_TABLES';
set @float_text='2.5';
execute direct_float using @float_text;
select d from dst where id=3 order by d;

-- Strict UPDATE rolls back and an unevaluated branch has no diagnostic.
update dst set d=cast(case when id=1 then '2.5' else 'inf' end as double) where id in (1,2);
show warnings;
select * from dst where id in (1,2) order by id;
select case when false then cast('inf' as double) else 1e0 end;
show warnings;
select id,case when id<0 then cast(s as double) else 1e0 end from src order by id;
show warnings;

deallocate prepare direct_float;
deallocate prepare explicit_float;
deallocate prepare literal_float;
deallocate prepare literal_explicit_float;

-- Stored DOUBLE assignments and fractional high scales use shortest values.
create table decimal_src(id int, d double);
insert into decimal_src values(1,1.005e0),(2,0.12345678901234568e0),(3,null);
create table decimal_dst(id int, d64 decimal(10,2), d128 decimal(38,20));
insert into decimal_dst select id,d,d from decimal_src;
select * from decimal_dst order by id;
select id,cast(d as decimal(38,20)),cast(d as decimal(38,38)) from decimal_src where id>=2 order by id;

-- A valid row before overflow must not survive a failed strict assignment.
set sql_mode='STRICT_TRANS_TABLES';
insert into decimal_src values(4,999.995e0);
create table decimal_narrow(d decimal(5,2));
insert into decimal_narrow select d from decimal_src where id in (1,4) order by id;
show warnings;
select count(*) from decimal_narrow;

prepare decimal_float from 'select cast(? as decimal(10,2)),cast(? as decimal(38,20))';
set @decimal_float_value=1.005e0;
execute decimal_float using @decimal_float_value,@decimal_float_value;
set @decimal_float_value=1.0049999999999997e0;
execute decimal_float using @decimal_float_value,@decimal_float_value;
deallocate prepare decimal_float;
set sql_mode=@saved_float_cast_mode;
drop database float_cast_mysql;
