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

create table wide(d decimal(20,0));
insert into wide values (9007199254740992), (9007199254740993);
select count(*) from wide where d = (select cast(9007199254740992 as double));
select count(*) from wide join (select cast(9007199254740992 as double) as v) x on wide.d = x.v;

drop database issue_29514_decimal_expr;
