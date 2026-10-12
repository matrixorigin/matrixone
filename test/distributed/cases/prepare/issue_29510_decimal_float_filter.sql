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
select count(*) from t where d = cast(0.11 as double(3,1));
select count(*) from t where d = cast(0.14 as double(3,1));
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

-- Scientific literals, IN and range predicates must share the same proof.
select count(*) from wide where d = 9.007199254740992e15;
select count(*) from wide where d in (9.007199254740992e15, -9e20);
select count(*) from t where d >= 1.04e-1 and d < 1e0;
create table precise(d decimal(20,20));
insert into precise values (0.00000000000000000003), (null);
select count(*) from precise where d = 2.9999999999999997e-20;
select count(*) from precise where d > 2.9999999999999997e-20;
prepare p from 'select count(*) from precise where d = ?';
set @v = 2.9999999999999997e-20;
execute p using @v;
set @v = 3e-20;
execute p using @v;
deallocate prepare p;

-- FLOAT and bounded floating CASTs obey the same comparison-domain contract.
create table floating(f float, bounded_f float(4,1), bounded_d double(4,1));
insert into floating values (0.1, 1.3, 1.3), (16777216, null, null);
select count(*) from floating where f = 1e-1;
select count(*) from floating where f = abs(cast(16777217 as signed));
select count(*) from floating where bounded_f = 1.25e0;
select count(*) from floating where bounded_d between 1.25e0 and 1.25e0;


-- Safe bounded constants retain native filtering without accepting rounded peers.
insert into floating values (null, 1, 1), (null, 0.5, 0.5), (null, -0.5, -0.5);
select count(*) from floating where bounded_f = 1;
select count(*) from floating where bounded_f in (1, 5e-1, -5e-1);
select count(*) from floating where bounded_f not in (1, 5e-1, -5e-1);
select count(*) from floating where bounded_f between -5e-1 and 5e-1;
select count(*) from floating where bounded_f > 1000;
select count(*) from floating where bounded_f = 1.2999999523162842e0;
prepare p from 'select count(*) from floating where bounded_f = cast(? as double)';
set @v = 5e-1;
execute p using @v;
set @v = 1.25e0;
execute p using @v;
deallocate prepare p;

drop database issue_29510_decimal_float;
