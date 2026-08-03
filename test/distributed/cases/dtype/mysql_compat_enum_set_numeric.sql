-- MySQL uses ENUM ordinals and SET bitmaps in numeric operand contracts,
-- while retaining their labels in string operand contracts.
drop table if exists mysql_compat_enum_set_numeric;
create table mysql_compat_enum_set_numeric (
    e enum('a', 'b', ''),
    s set('x', 'y', 'z'),
    i int
);
insert into mysql_compat_enum_set_numeric values ('a', 'x', 1), ('b', 'x,y', 2);

select cast(e as signed), abs(e), e = i, e between 1 and 2, e in (i), e = +1,
       length(e), e = 'a'
from mysql_compat_enum_set_numeric order by i;

select cast(s as signed), abs(s), s = i, s between 1 and 2, s in (i), s = +1,
       length(s), s = 'x'
from mysql_compat_enum_set_numeric order by i;

select e in (select i from mysql_compat_enum_set_numeric),
       e not in (select i from mysql_compat_enum_set_numeric),
       e = any (select i from mysql_compat_enum_set_numeric),
       s in (select i from mysql_compat_enum_set_numeric),
       s not in (select i from mysql_compat_enum_set_numeric)
from mysql_compat_enum_set_numeric order by i;

-- String-typed subqueries retain label comparison semantics.
select e in (select cast(i as char) from mysql_compat_enum_set_numeric),
       s in (select cast(i as char) from mysql_compat_enum_set_numeric)
from mysql_compat_enum_set_numeric order by i;

drop table mysql_compat_enum_set_numeric;
