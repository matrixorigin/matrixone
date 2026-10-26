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

drop database if exists func_string_bool_branches;
