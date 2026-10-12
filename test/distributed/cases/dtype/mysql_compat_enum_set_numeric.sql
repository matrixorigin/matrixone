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

-- A single empty member retains ENUM/SET identity, distinct from NULL and uint64.
drop table if exists special_single_empty;
create table special_single_empty (id int primary key, e enum('') default '', s set('') default '', u bigint unsigned);
insert into special_single_empty values (1, '', '', 2), (2, 1, 1, 18446744073709551615), (3, null, null, null);
select id, concat('[',e,']') as label, e+0 as ordinal, concat('[',s,']') as members, s+0 as bitmap,
       find_in_set('',s) as empty_member_position, u, e is null, s is null
from special_single_empty order by id;
insert ignore into special_single_empty values (4, 'bad', 'bad', 4), (5, 65537, 3, 5), (6, -1, -1, 6);
select id, e+0 as ordinal, s+0 as bitmap from special_single_empty order by id;
-- Failed statements must not partially write or poison subsequent assignments.
insert into special_single_empty values (7, 1, 1, 7), (8, 2, 1, 8);
update special_single_empty set s=2 where id=3;
select id, e+0 as ordinal, s+0 as bitmap from special_single_empty order by id;
update special_single_empty set e='',s='' where id=3;
insert into special_single_empty (id,u) values (9,9);
select id,e+0 as ordinal,s+0 as bitmap from special_single_empty where id in (3,9) order by id;
show create table special_single_empty;
show columns from special_single_empty;
select column_name,data_type,column_type from information_schema.columns
where table_schema=database() and table_name='special_single_empty' order by ordinal_position;
-- Transparent views and same-domain copies retain ordinal zero and bitmap one.
create view special_single_empty_view as select id,e,s,u from special_single_empty;
select id,e+0 as ordinal,s+0 as bitmap from special_single_empty_view order by id;
create table special_single_empty_copy like special_single_empty;
insert into special_single_empty_copy select * from special_single_empty;
select id,e+0 as ordinal,s+0 as bitmap from special_single_empty_copy order by id;
create table special_single_empty_clone clone special_single_empty;
select id,e+0 as ordinal,s+0 as bitmap from special_single_empty_clone order by id;
-- Transparent forwarding preserves ordinals; materialization and explicit strings keep display semantics.
insert into special_single_empty_copy(id,e,s,u) select 12,e,0,12 from (select distinct e from special_single_empty where id in (1,4)) d;
insert into special_single_empty_copy(id,e,s,u) select 13,e,0,13 from special_single_empty where id=1 union select 13,e,0,13 from special_single_empty where id=4;
insert into special_single_empty_copy(id,e) select 14,e from special_single_empty where id in (1,4) group by 2;
insert into special_single_empty_copy(id,e) select 15,(e) from (select e from special_single_empty where id=4) d;
insert into special_single_empty_copy(id,e) select 16,cast(e as char) from special_single_empty where id=4;
insert into special_single_empty_copy(id,e) with d as (select e from special_single_empty where id=4) select 17,e from d;
select id,e+0 as ordinal from special_single_empty_copy where id>=12 order by id;
-- Prepared parameters reuse the same domain for strings, ordinals, and NULL.
prepare single_empty_insert from 'insert into special_single_empty_copy values (?, ?, ?, ?)';
set @empty_id=10,@empty_label='',@empty_bitmap=1,@empty_unsigned=10;
execute single_empty_insert using @empty_id,@empty_label,@empty_bitmap,@empty_unsigned;
set @empty_id=11,@empty_label=null,@empty_bitmap=null;
execute single_empty_insert using @empty_id,@empty_label,@empty_bitmap,@empty_unsigned;
deallocate prepare single_empty_insert;
select id,e+0 as ordinal,s+0 as bitmap from special_single_empty_copy where id>=10 order by id;
create table special_single_empty_load like special_single_empty;
load data inline format='csv', data='1,,,1\n2,1,1,2\n' into table special_single_empty_load fields terminated by ',';
select id,e+0 as ordinal,s+0 as bitmap from special_single_empty_load order by id;
truncate table special_single_empty_load;
load data inline format='jsonline', data='{"id":1,"e":"","s":"","u":1}\n{"id":2,"e":null,"s":null,"u":2}\n', jsontype='object' into table special_single_empty_load;
select id,e+0 as ordinal,s+0 as bitmap from special_single_empty_load order by id;
drop table special_single_empty_load;
drop table special_single_empty_clone;
drop table special_single_empty_copy;
drop view special_single_empty_view;
drop table special_single_empty;
