-- @suite
-- @case
-- @desc: MySQL-compatible string and numeric comparison in IN lists

drop table if exists in_mixed_type;
create table in_mixed_type (s varchar(20));
insert into in_mixed_type values ('9.50'), ('8'), ('7');

-- Keep the integer before the matching decimal. This previously attempted to
-- cast '9.50' to INT64 and failed before evaluating the matching value.
select '9.50' in (7, '8', 9.5) as scalar_in;
select '9.50' in (9.5, '8', 7) as scalar_in_reordered;
select '9.50' not in (7, '8', 9.5) as scalar_not_in;
select '9.50' = 9.5 as scalar_equal;

select s from in_mixed_type where s in (7, '8', 9.5) order by s;

drop table in_mixed_type;

-- Numeric left operands must retain their native exact-comparison path. In
-- particular, adjacent BIGINT and DECIMAL(40,0) values cannot be coerced to
-- FLOAT64, because both pairs round to the same IEEE-754 value.
drop table if exists in_mixed_numeric_precision;
create table in_mixed_numeric_precision (i bigint, d decimal(40, 0));
insert into in_mixed_numeric_precision values
    (9223372036854775807, 9999999999999999999999999999999999999999);
select i in ('9223372036854775806') as bigint_adjacent,
       d in ('9999999999999999999999999999999999999998') as decimal_adjacent
from in_mixed_numeric_precision;
select i not in ('9223372036854775806') as bigint_not_adjacent,
       d not in ('9999999999999999999999999999999999999998') as decimal_not_adjacent
from in_mixed_numeric_precision;
drop table in_mixed_numeric_precision;
