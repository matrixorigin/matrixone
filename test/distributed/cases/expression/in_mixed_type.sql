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
