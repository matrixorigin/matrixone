-- @suite

-- @case
-- @desc: BIT columns in AND/OR/XOR/NOT: non-zero is TRUE, zero is FALSE, NULL stays NULL (MySQL semantics)
-- @label:bvt

drop database if exists bit_logical_operators;
create database bit_logical_operators;
use bit_logical_operators;

create table tb(id int primary key, b bit(1), w bit(8));
insert into tb values (1, 0, 0), (2, 1, 1), (3, 1, 128), (4, null, null);

-- AND/OR/XOR/NOT with a BIT column as first operand; 128 of BIT(8) is TRUE
select id, b and 1 as b_and, b or 0 as b_or, not b as not_b, b xor 1 as b_xor from tb order by id;
select id, w and 1 as w_and, w or 0 as w_or, not w as not_w, w xor 0 as w_xor from tb order by id;
-- BIT column as second operand and in both positions
select id, 1 and w as and_1_w, 0 or w as or_0_w, 1 xor w as xor_1_w from tb order by id;
select id, b and w as b_and_w, b or w as b_or_w from tb order by id;
-- symbolic aliases &&, ||, !
select id, b && w as ss_and, b || w as ss_or, !w as bang_w from tb order by id;
-- three-valued logic: 0 AND NULL = FALSE, 1 OR NULL = TRUE, NULL operand keeps NULL
select id, b and cast(null as bit(8)) as b_and_null, b or cast(null as bit(8)) as b_or_null from tb order by id;
-- WHERE truthiness through the logical operators
select id from tb where not w order by id;
select id from tb where b and w order by id;
select id from tb where b or w is null order by id;
-- boolean contexts that share the cast: CASE WHEN and IF stay consistent
select id, case when w and true then 'y' else 'n' end as case_w from tb order by id;
select id, if(not b, 'f', 't') as if_not_b from tb order by id;
-- BIT(64) high bit is TRUE, not negative and not FALSE
create table tb64(id int primary key, v bit(64));
insert into tb64 values (1, 0), (2, 1), (3, 9223372036854775808), (4, 18446744073709551615), (5, null);
select id, v and 1 as v_and, not v as not_v, v or 0 as v_or from tb64 order by id;
-- numeric use of BIT columns is unchanged
select id, b + 0 as b_num, w + 0 as w_num from tb order by id;

-- conditional value branches: BIT-only branches keep their BIT values; a
-- BOOL collapse would surface here as 1 instead of 128 or the BIT(64) high
-- bit. Both branch orders are exercised for COALESCE, IF, and CASE.
select id, coalesce(w, b) + 0 as coalesce_w_b, coalesce(b, w) + 0 as coalesce_b_w from tb order by id;
select id, if(true, w, b) + 0 as if_w_b, if(false, b, w) + 0 as if_b_w from tb order by id;
select id, case when true then w else b end + 0 as case_w_b, case when true then b else w end + 0 as case_b_w from tb order by id;
select id, coalesce(v, cast(0 as bit(64))) + 0 as coalesce_v, if(true, v, cast(0 as bit(64))) + 0 as if_v_t, if(false, cast(0 as bit(64)), v) + 0 as if_v_f from tb64 order by id;
select id from tb where coalesce(w, b) = 128 order by id;

-- BIT mixed with BOOL value branches stays a plan-time error instead of
-- silently collapsing non-zero BIT values to TRUE: the operators' BIT
-- acceptance is scoped to boolean argument positions (issue #31180 review)
select coalesce(w, true) from tb order by id;
select coalesce(true, w) from tb order by id;
select if(true, w, true) from tb;
select if(true, true, w) from tb;
select case when true then w else true end from tb;
select case when true then true else w end from tb;

drop table tb64;
drop table tb;
drop database bit_logical_operators;
