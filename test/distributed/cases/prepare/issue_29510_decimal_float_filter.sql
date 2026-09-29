-- @case
-- @desc: Unique DOUBLE peers retain native DECIMAL filters; ambiguous peers keep FLOAT comparison.
-- @label:bvt

drop database if exists issue_29510_decimal_float;
create database issue_29510_decimal_float;
use issue_29510_decimal_float;
create table t(d decimal(12,2));
insert into t select result from generate_series(1, 20000) g;
insert into t values (0.10), (0.11), (-12345.00);

explain select count(*) from t where d = cast(12345 as double);
-- @ignore:0
explain analyze select count(*) from t where d = cast(12345 as double);
select count(*) from t where d = cast(12345 as double);
explain select count(*) from t where d = cast(12345.00 as double);
select count(*) from t where d = cast(12345.00 as double);
select count(*) from t where d = cast(0.1 as double);
explain select count(*) from t where d = cast(0.104 as double);
select count(*) from t where d = cast(0.104 as double);
select count(*) from t where d < cast(0.104 as double);
select count(*) from t where d <= cast(0.1 as double);
select count(*) from t where d = cast(-12345 as double);
select count(*) from t where d = cast(100000000000 as double);

prepare p from 'select count(*) from t where d = ?';
set @v = cast(12345 as double);
explain force execute p using @v;
-- @ignore:0
explain analyze force execute p using @v;
execute p using @v;
set @v = cast(0.104 as double);
execute p using @v;
deallocate prepare p;

create table wide(d decimal(20,0));
insert into wide values (9007199254740992), (9007199254740993);
explain select count(*) from wide where d = cast(9007199254740992 as double);
select count(*) from wide where d = cast(9007199254740992 as double);

drop database issue_29510_decimal_float;
