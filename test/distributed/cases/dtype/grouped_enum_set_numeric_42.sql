-- @suite
-- @case
-- @desc: preserve ENUM ordinal and SET bitmap semantics in numeric contexts after grouping
-- @label:bvt

drop database if exists issue_28687_grouped_numeric;
create database issue_28687_grouped_numeric;
use issue_28687_grouped_numeric;

create table grouped_values (
    id int primary key,
    e enum('red', 'green', 'blue'),
    s set('a', 'b', 'c')
);

insert into grouped_values values
    (1, 'red', 'a'),
    (2, 'red', 'a,b'),
    (3, 'green', 'c'),
    (4, 'blue', 'a,b,c'),
    (5, null, null);

-- An invalid ENUM label is stored as ordinal 0 under INSERT IGNORE. Its empty
-- display must remain distinguishable from NULL when the group is converted.
insert ignore into grouped_values values (6, 'invalid', null);

select id, e + 0, s + 0 from grouped_values order by id;
select e, e + 0, count(*) from grouped_values group by e order by e + 0;
select s, s + 0, count(*) from grouped_values group by s order by s + 0;

select e, e in (1, 2), e not in (1, 2)
from grouped_values group by e order by e + 0;

-- NULL does not change a numeric list's comparison type. Check matching and
-- nonmatching values as well as string-list and row-level controls.
select id, e in (1, null), e not in (1, null), s in (1, null), s not in (1, null)
from grouped_values order by id;
select e + 0, e in (1, null), e not in (1, null), e in ('red', null)
from grouped_values group by e order by e + 0;
select s + 0, s in (1, null), s not in (1, null), s in ('a', null)
from grouped_values group by s order by s + 0;

-- Nested correlated references retain the grouped value's original domain.
select e + 0, (select e + 0), (select (select e + 0))
from grouped_values group by e order by e + 0;
select s + 0, (select s + 0)
from grouped_values group by s order by s + 0;
select e + 0, (select 1 in (select grouped_values.e))
from grouped_values group by e order by e + 0;
select s + 0, (select 1 in (select grouped_values.s))
from grouped_values group by s order by s + 0;

select 1 in (select e from grouped_values group by e);
select 1 not in (select e from grouped_values group by e);
select 99 not in (select e from grouped_values group by e);
select (e, 1) in (select 1, e from grouped_values group by e)
from grouped_values group by e order by e + 0;
select 1 = any (select e from grouped_values group by e);

select d.e + 0
from (select e from grouped_values group by e) d
order by d.e + 0;

-- 4.2 may encode ENUM members containing commas with framed metadata.
create table encoded_values (e enum('a,b','c'));
insert into encoded_values values ('a,b'),('a,b'),('c'),(null);
select e,e+0,count(*) from encoded_values group by e order by e+0;

-- Explicit numeric casts must use the same grouped provenance.
select cast(e as signed), count(*) from grouped_values group by e order by e+0;
select cast(s as unsigned), count(*) from grouped_values group by s order by s+0;

-- Compare a grouped SET bitmap with numeric subquery results.
select 3 in (select s from grouped_values group by s);
select 99 not in (select s from grouped_values group by s);

-- Parameter reuse must bind the numeric group result for each execution.
prepare grouped_numeric from 'select e+0,count(*) from grouped_values group by e having e+0 >= ? order by e+0';
set @bound=2;
execute grouped_numeric using @bound;
set @bound=1;
execute grouped_numeric using @bound;
deallocate prepare grouped_numeric;

drop database issue_28687_grouped_numeric;
