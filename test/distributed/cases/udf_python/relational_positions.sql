-- Exercise the current SQL execution positions that must preserve the
-- Python call's row mapping: WHERE, JOIN ON, outer join, aggregate, window,
-- and a dependent call g(f(x)).
drop database if exists udf_python_positions_bvt;
create database udf_python_positions_bvt;
use udf_python_positions_bvt;

create function python_bvt_position_add (x int) returns int language python as 'def python_bvt_position_add(ctx, x): return None if x is None else x + 1' handler 'python_bvt_position_add';
create function python_bvt_position_twice (x int) returns int language python as 'def python_bvt_position_twice(ctx, x): return None if x is None else x * 2' handler 'python_bvt_position_twice';

create table position_left (id int, join_key int, value int);
create table position_right (join_key int, tag varchar(8));
insert into position_left values (1, 10, 1), (2, 20, 2), (3, 30, null);
insert into position_right values (11, 'a'), (21, 'b'), (40, 'd');

-- The predicate and projection both use Python, with NULL filtered by SQL.
select id, python_bvt_position_add(value) as plus
from position_left
where python_bvt_position_add(value) > 1
order by id;

-- The UDF remains part of JOIN ON and must not be rewritten as a post-join
-- filter, so the LEFT JOIN keeps the unmatched outer row.
select l.id, r.tag
from position_left l join position_right r
  on python_bvt_position_add(l.join_key) = r.join_key
order by l.id;

select l.id, r.tag
from position_left l left join position_right r
  on python_bvt_position_add(l.join_key) = r.join_key
order by l.id;

-- Aggregate input and window ordering both consume a Python expression.
select join_key, sum(python_bvt_position_add(value)) as total
from position_left
group by join_key
order by join_key;

select id, python_bvt_position_add(value) as plus,
       row_number() over (order by python_bvt_position_add(value)) as rn
from position_left
order by id;

-- The outer call consumes the inner call's result and must retain NULL.
select id, python_bvt_position_twice(python_bvt_position_add(value)) as nested
from position_left
order by id;

drop function python_bvt_position_add(int);
drop function python_bvt_position_twice(int);
drop table position_left;
drop table position_right;
drop database udf_python_positions_bvt;
