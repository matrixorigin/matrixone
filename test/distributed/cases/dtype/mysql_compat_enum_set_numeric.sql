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

-- Numeric left operands and row constructors also use subquery ordinals/bitmaps.
select i in (select e from mysql_compat_enum_set_numeric),
       i = any (select e from mysql_compat_enum_set_numeric),
       i in (select s from mysql_compat_enum_set_numeric),
       (e, i) in (select i, i from mysql_compat_enum_set_numeric),
       (e, i) not in (select i, i from mysql_compat_enum_set_numeric),
       (e, i) = any (select i, i from mysql_compat_enum_set_numeric)
from mysql_compat_enum_set_numeric order by i;

-- String-typed subqueries retain label comparison semantics.
select e in (select cast(i as char) from mysql_compat_enum_set_numeric),
       s in (select cast(i as char) from mysql_compat_enum_set_numeric)
from mysql_compat_enum_set_numeric order by i;

drop table mysql_compat_enum_set_numeric;

-- SUM/AVG consume stored ordinals/bitmaps before aggregate DISTINCT, not labels.
drop table if exists special_numeric_agg;
create table special_numeric_agg (id int primary key, g int,
    e enum('red', 'green', 'blue'), s set('a', 'b', 'c'));
insert into special_numeric_agg values
    (1, 1, 'red', 'a'), (2, 1, 'red', 'a'), (3, 1, 'green', 'a,b'),
    (4, 2, 'blue', 'c'), (5, 2, null, null);
select sum(e) as se, avg(e) as ae, sum(s) as ss, avg(s) as avs from special_numeric_agg;
select sum(distinct e) as se, cast(avg(distinct e) as decimal(12,4)) as ae,
       sum(distinct s) as ss, cast(avg(distinct s) as decimal(12,4)) as avs from special_numeric_agg;
select g, sum(e) as se, cast(avg(e) as decimal(12,4)) as ae, sum(s) as ss, cast(avg(s) as decimal(12,4)) as avs
from special_numeric_agg group by g order by g;
select sum(e + 0) as se, avg(e + 0) as ae, sum(s + 0) as ss, avg(s + 0) as avs from special_numeric_agg;
select sum(e) as se, avg(e) as ae, sum(s) as ss, avg(s) as avs
from (select e, s from special_numeric_agg) d;
select sum(e) as se, avg(e) as ae, sum(s) as ss, avg(s) as avs from special_numeric_agg where id = 5;
select sum(e) as se, avg(e) as ae, sum(s) as ss, avg(s) as avs from special_numeric_agg where id < 0;
drop table special_numeric_agg;

-- Numeric-looking labels and empty labels must not replace numeric identity.
drop table if exists special_numeric_empty;
create table special_numeric_empty (id int primary key, e enum('', '99'), s set('', '99'));
insert into special_numeric_empty values (1, 1, 1), (2, 2, 0), (3, null, null);
select sum(e) as se, avg(e) as ae, sum(s) as ss, avg(s) as avs,
       sum(distinct s) as ds, avg(distinct s) as da from special_numeric_empty;
select sum(s) as ss, avg(s) as avs from (select s from special_numeric_empty) d;
select sum(s) as ss, avg(s) as avs from (select distinct s from special_numeric_empty) d;
select sum(s) as ss, avg(s) as avs from (select s from special_numeric_empty limit 3) d;
insert into special_numeric_empty values (4, 2, 2);
select id, cast(s as decimal(10,2)) as decimal_bitmap, cast(s as double) as double_bitmap
from (select id, s from special_numeric_empty) d order by id;
select id, cast(s as decimal(10,2)) as decimal_bitmap, cast(s as double) as double_bitmap
from (select id, s from special_numeric_empty order by id limit 4) d order by id;
delete from special_numeric_empty where id = 4;
drop view if exists special_numeric_view;
create view special_numeric_view as select e, s from special_numeric_empty;
select column_name, data_type from information_schema.columns
where table_schema = database() and table_name = 'special_numeric_view' order by ordinal_position;
select sum(e) as se, avg(e) as ae, sum(s) as ss, avg(s) as avs from special_numeric_view;
drop table if exists special_numeric_text;
create table special_numeric_text as select cast(e as char) as e, cast(s as char) as s from special_numeric_empty;
select column_name, data_type from information_schema.columns
where table_schema = database() and table_name = 'special_numeric_text' order by ordinal_position;
select concat('[', e, ']') as e, concat('[', s, ']') as s from special_numeric_text order by e, s;
select sum(s) from special_numeric_text;
drop table special_numeric_text;
drop view special_numeric_view;
insert ignore into special_numeric_empty values (4, 0, null);
select sum(e) as se, avg(e) as ae from (select e from special_numeric_empty) d;
drop table special_numeric_empty;
