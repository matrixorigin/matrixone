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

-- Scalar numeric comparisons use the same ordinal and bitmap as arithmetic.
select e + 0, e = 1, e <> 1, e < 2
from grouped_values group by e order by e + 0;
select s + 0, s = 1, s <> 1, s < 2
from grouped_values group by s order by s + 0;

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

-- A correlated scalar with its own FROM retains grouped numeric provenance.
select o.id, o.e + 0, (select o.e + 0 from grouped_values i where i.id = o.id and i.id <> 2),
       o.s + 0, (select o.s + 0 from grouped_values i where i.id = o.id and i.id <> 2)
from grouped_values o group by o.id, o.e, o.s order by o.id;

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
select o.e + 0, (select o.e + 0 from grouped_values i where i.id = 1)
from encoded_values o group by o.e order by o.e + 0;

-- Numeric-looking labels must still recover their definition positions.
create table numeric_labels (id int primary key, e enum('1','01','red'), s set('1','2','red'));
insert into numeric_labels values (1,'1','1'),(2,'01','1,red'),(3,'red','2'),(4,null,null);
select o.id, (select o.e + 0 from numeric_labels i where i.id = o.id),
       (select o.s + 0 from numeric_labels i where i.id = o.id)
from numeric_labels o group by o.id, o.e, o.s order by o.id;

-- A singleton without FROM retains its column's numeric contract. The same
-- scalar in a lexical context keeps its display value.
select o.id, (select o.e) + 0, (select o.s) + 0,
       cast((select o.e) as signed), (select o.e) = 2,
       (select o.e) in (1, 2), (select o.e) = '01', concat((select o.e), ''),
       o.e + o.s, (select o.e) + (select o.s)
from numeric_labels o group by o.id, o.e, o.s order by o.id;
select o.id, (select o.e) + 0, (select o.s) + 0,
       (select o.e) + (select o.s) from numeric_labels o order by o.id;
select o.id, (select o.e) in (1,null), (select o.e) not in (1,null),
       (select o.s) in (5,null), (select o.s) not in (5,null)
from numeric_labels o group by o.id,o.e,o.s order by o.id;
-- Correlated row-level columns retain their raw ordinal/bitmap for numeric IN.
select o.id, (select o.e in (1,2,null)), (select o.e not in (1,2,null)),
       (select o.s in (1,5,null)), (select o.s not in (1,5,null))
from numeric_labels o order by o.id;
-- Tuple IN is the existing owner of left-operand expansion. Grouped recovery
-- remains valid for each candidate; lexical and row-level raw comparisons
-- continue through their existing paths.
select o.id, ((select o.e),o.id) in ((1,1),(2,2)) as matched
from numeric_labels o group by o.id,o.e order by o.id;
select o.id, ((select o.e),o.id) not in ((1,1),(2,2)) as matched
from numeric_labels o group by o.id,o.e order by o.id;
select o.id, (cast((select o.e) as signed),o.id) in ((1,1),(2,2)) as matched
from numeric_labels o group by o.id,o.e order by o.id;
select o.id, (0+(select o.e),o.id) in ((1,1),(2,2)) as matched
from numeric_labels o group by o.id,o.e order by o.id;
select o.id, ((select o.e),o.id) in ((2,2)) from numeric_labels o group by o.id,o.e order by o.id;
select o.id, ((select o.e),o.id) in (('1',1),('01',2)) from numeric_labels o group by o.id,o.e order by o.id;
select o.id, ((select o.e),o.id) in ((1,1),(2,2)) from numeric_labels o order by o.id;
-- @regex("numeric scalar IN list needs a proven lossless shared type",true)
select (select o.e) in (1.5,2,null) from numeric_labels o group by o.e;
-- @regex("numeric scalar IN list needs a proven lossless shared type",true)
select (select o.e) not in (1.5,2,null) from numeric_labels o group by o.e;
-- @regex("correlated special IN comparison requires a proven numeric contract",true)
select (select o.e in (2,'x') from grouped_values i where i.id=o.id)
from numeric_labels o group by o.id,o.e;
select (select o.e from grouped_values i where i.id = 1) + 0
from numeric_labels o where o.id = 2 group by o.e;
select (select o.e limit 0) + 0 from numeric_labels o where o.id = 2;
prepare scalar_numeric from 'select (select o.e) + ? from numeric_labels o where o.id = 2 group by o.e';
set @scalar_arg = 3;
execute scalar_numeric using @scalar_arg;
set @scalar_arg = 8;
execute scalar_numeric using @scalar_arg;
deallocate prepare scalar_numeric;
select (select o.e in (select 2) from grouped_values i where i.id=o.id),
       (select 2 in (select o.e) from grouped_values i where i.id=o.id)
from numeric_labels o where o.id=2 group by o.id,o.e;
-- @regex("dynamic quantified comparison with correlated ENUM/SET display",true)
prepare scalar_quantified from 'select (select o.e in (select ?) from grouped_values i where i.id=o.id) from numeric_labels o group by o.id,o.e';
-- @regex("dynamic quantified comparison with correlated ENUM/SET display",true)
prepare scalar_quantified_reverse from 'select (select ? in (select o.e) from grouped_values i where i.id=o.id) from numeric_labels o group by o.id,o.e';

-- An empty member makes grouped displays ambiguous with the zero value.
create table ambiguous_labels (id int, e enum('', '1'), s set('', '1'));
insert into ambiguous_labels values (1,'1','1'),(2,'',''),(3,null,null);
select (select a.e) + 0, (select a.s) + 0 from ambiguous_labels a where id=1;
-- @regex("numeric scalar with non-reversible ENUM/SET display",true)
select (select a.e) + 0 from ambiguous_labels a group by a.e;
-- @regex("correlated special IN comparison requires a proven numeric contract",true)
select (select a.e in (1,2) from grouped_values i where i.id=a.id)
from ambiguous_labels a group by a.id,a.e;
-- @regex("correlated special IN comparison requires a proven numeric contract",true)
select (select a.e in (2,'x') from grouped_values i where i.id=a.id)
from ambiguous_labels a group by a.id,a.e;
-- @regex("correlated special IN comparison requires a proven numeric contract",true)
select (select 1 in (a.e,3) from grouped_values i where i.id=a.id)
from ambiguous_labels a group by a.id,a.e;
-- @regex("numeric scalar IN list needs a proven lossless shared type",true)
select 2 in ((select a.e),3) from ambiguous_labels a group by a.e;
-- @regex("numeric correlated value with non-reversible ENUM/SET display",true)
select (select a.e in (select 2) from grouped_values i where i.id=a.id)
from ambiguous_labels a group by a.id,a.e;
-- @regex("numeric correlated value with non-reversible ENUM/SET display",true)
select (select 2 in (select a.e) from grouped_values i where i.id=a.id)
from ambiguous_labels a group by a.id,a.e;
-- @regex("numeric scalar IN list needs a proven lossless shared type",true)
select (select a.e) in (2,'x') from ambiguous_labels a group by a.e;
select (select a.e) in ('1','x') from ambiguous_labels a where id=1 group by a.e;
select (select a.e='1' from grouped_values i where i.id=a.id)
from ambiguous_labels a where a.id=1 group by a.id,a.e;
-- @regex("numeric correlated value with non-reversible ENUM/SET display",true)
select (select cast(a.s as unsigned) from grouped_values i where i.id=a.id)
from ambiguous_labels a group by a.id,a.s;
select a.id, (select concat(a.e,'') from grouped_values i where i.id=a.id)
from ambiguous_labels a group by a.id,a.e order by a.id;

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
