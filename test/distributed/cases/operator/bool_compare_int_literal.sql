-- @suite

-- @case
-- @desc: BOOL compared with an integer literal other than 0/1: TRUE/FALSE are 1/0, so b = 2 matches nothing (MySQL semantics)
-- @label:bvt

drop database if exists bool_compare_int_literal;
create database bool_compare_int_literal;
use bool_compare_int_literal;

select 2 = true as a, true = 2 as b, -1 = true as c, 2 <> true as d, 2 > true as e, true < 2 as f, 2 <=> true as g, 0 = false as h, 1 = true as i;
select 2 in (true) as a, true in (2, 3) as b, 1 in (true) as c;

create table t(id int primary key, b bool);
insert into t values (1, true), (2, false), (3, null);
select id, b = 2 as eq2, b <> 2 as ne2, b < 2 as lt2, b >= 2 as ge2, b > -1 as gtm1, b = 1 as eq1, b = 0 as eq0, b <=> 2 as nse2 from t order by id;
select id from t where b = 2 order by id;
select id from t where b != 2 order by id;
select id from t where b = 1 order by id;
select id from t where 0 = b order by id;
select id from t where b < 2 and b > -1 order by id;
select id from t where b and 2 order by id;

drop database if exists bool_compare_int_literal;
