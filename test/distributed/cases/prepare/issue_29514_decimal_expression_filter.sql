-- @case
-- @desc: Foldable DOUBLE peers keep selective DECIMAL filtering across scalar, function and singleton-derived forms.
-- @label:bvt

drop database if exists issue_29514_decimal_expr;
create database issue_29514_decimal_expr;
use issue_29514_decimal_expr;
create table t(d decimal(12,2));
insert into t select result from generate_series(1, 100000) g;

explain select count(*) from t where d = (select cast(54321 as double));
-- @ignore:0
explain analyze select count(*) from t where d = (select cast(54321 as double));
select count(*) from t where d = (select cast(54321 as double));
explain select count(*) from t where d = abs(cast(54321 as double));
-- @ignore:0
explain analyze select count(*) from t where d = abs(cast(54321 as double));
select count(*) from t where d = abs(cast(54321 as double));
explain select count(*) from t join (select cast(54321 as double) as v) x on t.d = x.v;
-- @ignore:0
explain analyze select count(*) from t join (select cast(54321 as double) as v) x on t.d = x.v;
select count(*) from t join (select cast(54321 as double) as v) x on t.d = x.v;
explain select count(*) from t join (select abs(cast(54321 as double)) as v) x on t.d = x.v;
-- @ignore:0
explain analyze select count(*) from t join (select abs(cast(54321 as double)) as v) x on t.d = x.v;
select count(*) from t join (select abs(cast(54321 as double)) as v) x on t.d = x.v;

select count(*) from t where d = (select cast(54321.104 as double));
select count(*) from t where d = abs(cast(54321.104 as double));
select count(*) from t join (select cast(54321.104 as double) as v) x on t.d = x.v;
select count(*) from t join (select abs(cast(54321.104 as double)) as v) x on t.d = x.v;
select count(*) from t where d = (select cast(null as double));
select count(*) from t join (select cast(null as double) as v) x on t.d = x.v;
select count(*) from t join (select d as v from t where d in (54321, 54322)) x on t.d = x.v;

-- #29515: execution-time DOUBLE parameters in equivalent expression shapes.
set @v = cast(54321 as double);
prepare scalar_peer from 'select count(*) from t where d = (select ?)';
-- @ignore:0
explain analyze force execute scalar_peer using @v;
execute scalar_peer using @v;
prepare abs_peer from 'select count(*) from t where d = abs(?)';
-- @ignore:0
explain analyze force execute abs_peer using @v;
execute abs_peer using @v;
prepare derived_peer from 'select count(*) from t join (select ? as v) x on t.d = x.v';
-- @ignore:0
explain analyze force execute derived_peer using @v;
execute derived_peer using @v;
prepare derived_abs_peer from 'select count(*) from t join (select abs(?) as v) x on t.d = x.v';
-- @ignore:0
explain analyze force execute derived_abs_peer using @v;
execute derived_abs_peer using @v;
set @one = 1;
prepare second_peer from 'select count(*) from t where ? = 1 and d = abs(?)';
-- @ignore:0
explain analyze force execute second_peer using @one, @v;
execute second_peer using @one, @v;
set @v = cast(0.104 as double);
execute scalar_peer using @v;
execute abs_peer using @v;
execute derived_peer using @v;
execute derived_abs_peer using @v;
execute second_peer using @one, @v;
set @v = null;
execute scalar_peer using @v;
execute abs_peer using @v;
execute derived_peer using @v;
execute derived_abs_peer using @v;
execute second_peer using @one, @v;
deallocate prepare scalar_peer;
deallocate prepare abs_peer;
deallocate prepare derived_peer;
deallocate prepare derived_abs_peer;
deallocate prepare second_peer;

-- #29516: the explicit CAST type argument must participate in safe folding.
set @v = cast(54321 as double);
prepare cast_peer from 'select count(*) from t where d = cast(? as double)';
-- @ignore:0
explain analyze force execute cast_peer using @v;
execute cast_peer using @v;
prepare scalar_cast_peer from 'select count(*) from t where d = (select cast(? as double))';
-- @ignore:0
explain analyze force execute scalar_cast_peer using @v;
execute scalar_cast_peer using @v;
prepare derived_cast_peer from 'select count(*) from t join (select cast(? as double) as v) x on t.d = x.v';
-- @ignore:0
explain analyze force execute derived_cast_peer using @v;
execute derived_cast_peer using @v;
prepare precision_cast_peer from 'select count(*) from t where d = cast(? as double(5,0))';
execute precision_cast_peer using @v;
set @v = cast(0.104 as double);
execute cast_peer using @v;
execute scalar_cast_peer using @v;
execute derived_cast_peer using @v;
execute precision_cast_peer using @v;
set @v = null;
execute cast_peer using @v;
execute scalar_cast_peer using @v;
execute derived_cast_peer using @v;
set @v = '54321junk';
execute cast_peer using @v;
set @v = 'abc';
execute cast_peer using @v;
deallocate prepare cast_peer;
deallocate prepare scalar_cast_peer;
deallocate prepare derived_cast_peer;
deallocate prepare precision_cast_peer;

-- #29517: complete text parameters inside explicit DOUBLE casts can be
-- folded for the uniqueness proof; partial numeric text must retain warnings.
set @v = '54321';
prepare text_cast_peer from 'select count(*) from t where d = cast(? as double)';
-- @ignore:0
explain analyze force execute text_cast_peer using @v;
execute text_cast_peer using @v;
prepare text_scalar_cast_peer from 'select count(*) from t where d = (select cast(? as double))';
-- @ignore:0
explain analyze force execute text_scalar_cast_peer using @v;
execute text_scalar_cast_peer using @v;
prepare text_derived_cast_peer from 'select count(*) from t join (select cast(? as double) as v) x on t.d = x.v';
-- @ignore:0
explain analyze force execute text_derived_cast_peer using @v;
execute text_derived_cast_peer using @v;
prepare text_abs_cast_peer from 'select count(*) from t where d = abs(cast(? as double))';
execute text_abs_cast_peer using @v;
set @v = ' 54321 ';
execute text_cast_peer using @v;
set @v = '+54321';
execute text_cast_peer using @v;
set @v = '5.4321e4';
execute text_cast_peer using @v;
set @v = '0.104';
-- @ignore:0
explain analyze force execute text_cast_peer using @v;
execute text_cast_peer using @v;
set @v = '54321junk';
execute text_cast_peer using @v;
show warnings;
set @v = 'abc';
execute text_cast_peer using @v;
show warnings;
set @v = null;
execute text_cast_peer using @v;
deallocate prepare text_cast_peer;
deallocate prepare text_scalar_cast_peer;
deallocate prepare text_derived_cast_peer;
deallocate prepare text_abs_cast_peer;

create table wide(d decimal(20,0));
insert into wide values (9007199254740992), (9007199254740993);
select count(*) from wide where d = (select cast(9007199254740992 as double));
select count(*) from wide join (select cast(9007199254740992 as double) as v) x on wide.d = x.v;
set @v = cast(9007199254740992 as double);
prepare wide_scalar_peer from 'select count(*) from wide where d = (select ?)';
execute wide_scalar_peer using @v;
deallocate prepare wide_scalar_peer;
prepare wide_derived_peer from 'select count(*) from wide join (select ? as v) x on wide.d = x.v';
execute wide_derived_peer using @v;
deallocate prepare wide_derived_peer;
set @v = cast(9007199254740992 as double);
prepare wide_cast_peer from 'select count(*) from wide where d = cast(? as double)';
execute wide_cast_peer using @v;
deallocate prepare wide_cast_peer;
set @v = '9007199254740992';
prepare wide_text_cast_peer from 'select count(*) from wide where d = cast(? as double)';
execute wide_text_cast_peer using @v;
deallocate prepare wide_text_cast_peer;

-- Boolean and multi-peer predicates must retain native DECIMAL pruning.
set @a = cast(54321 as double), @b = cast(54322 as double);
prepare boolean_peers from 'select count(*) from t where d = abs(?) or d = abs(?)';
explain force execute boolean_peers using @a, @b;
execute boolean_peers using @a, @b;
deallocate prepare boolean_peers;
prepare list_peers from 'select count(*) from t where d in (abs(?), abs(?))';
explain force execute list_peers using @a, @b;
execute list_peers using @a, @b;
set @b = cast(54322.104 as double);
execute list_peers using @a, @b;
set @b = null;
execute list_peers using @a, @b;
set @b = cast(54322 as double);
execute list_peers using @a, @b;
deallocate prepare list_peers;
prepare range_peers from 'select count(*) from t where d between abs(?) and abs(?)';
explain force execute range_peers using @a, @b;
execute range_peers using @a, @b;
set @a = cast(54321.104 as double);
execute range_peers using @a, @b;
set @a = cast(54323 as double);
execute range_peers using @a, @b;
deallocate prepare range_peers;

drop database issue_29514_decimal_expr;
