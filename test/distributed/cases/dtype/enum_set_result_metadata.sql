-- ENUM/SET publish CHAR metadata while retaining label strings, including
-- empty labels and NULL. Ordinary strings and computed expressions are controls.
drop table if exists jdbc_enum_set_metadata;
create table jdbc_enum_set_metadata (
    id int,
    e enum('a','bb',''),
    s set('x','yy'),
    c char(2),
    v varchar(2)
);
insert into jdbc_enum_set_metadata values
    (1, 'a', 'x,yy', 'ab', 'ab'), (2, '', '', '', ''), (3, null, null, null, null);
-- @metacmp(true)
select e, s, c, v from jdbc_enum_set_metadata where 1 = 0;
-- @metacmp(true)
select e, s, c, v from jdbc_enum_set_metadata order by id;
-- @metacmp(true)
select e as enum_alias, s as set_alias
from (select id, e, s from jdbc_enum_set_metadata order by id limit 3) d order by id;
-- @metacmp(true)
select concat(e) as expr_e, cast(s as char) as expr_s from jdbc_enum_set_metadata where 1 = 0;
-- @metacmp(true)
select e, s from jdbc_enum_set_metadata where 1 = 0
union all select e, s from jdbc_enum_set_metadata where 1 = 0;
drop table jdbc_enum_set_metadata;
