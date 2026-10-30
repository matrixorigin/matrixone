-- @suite

-- @case
-- @desc: IF/CASE/COALESCE/IFNULL with string and BOOL value branches return a string; TRUE/FALSE become '1'/'0' (MySQL semantics)
-- @label:bvt

drop database if exists func_string_bool_branches;
create database func_string_bool_branches;
use func_string_bool_branches;

select if(1, 'x', true) as a, if(0, 'x', true) as b, if(0, true, 'y') as c, if(1, false, 'y') as d;
select case when 1 then 'x' else false end as a, case when 0 then 'x' else false end as b, case 2 when 1 then true when 2 then 'two' end as c;
select if(1, 'true', false) as a, if(0, 'true', false) as b, case when 1 then 'false' else true end as c;
select coalesce(null, 'x', true) as a, coalesce(null, true, 'x') as b, ifnull(null, 'x') as c, ifnull(false, 'x') as d;
select if(1, 'x', null) as a, if(1, true, null) as b, if(1, true, 2) as c;

create table t(id int primary key, s varchar(10), b bool);
insert into t values (1, 'abc', true), (2, 'def', false), (3, null, null), (4, '', true);
select id, if(b, s, b) as a, if(b, b, s) as c, case when b then s else b end as d, coalesce(s, b) as e, ifnull(b, s) as f from t order by id;
select id, concat(if(id > 2, b, s), '!') as g from t order by id;
select id from t where if(b, s, b) = 'abc' order by id;

-- a TEXT-family branch next to BOOL keeps its full length (no VARCHAR(65535) truncation)
create table lt(id int primary key, m mediumtext);
insert into lt values (1, repeat('x', 70000)), (2, 'short');
select id, length(if(id = 1, m, false)) as a, length(case when id = 1 then m else true end) as b, length(coalesce(m, false)) as c from lt order by id;
select length(if(true, cast(repeat('y', 65536) as mediumtext), false)) as a;
-- a short string next to BOOL keeps a rebuildable width
create table bool_ctas as select coalesce(cast('x' as char(10)), false) as v, if(true, cast('x' as char(10)), false) as w;
show create table bool_ctas;
select * from bool_ctas;

drop database if exists func_string_bool_branches;
