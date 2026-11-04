-- @suite

-- @case
-- @desc: DECIMAL values in a boolean context: non-zero is TRUE, zero is FALSE, NULL stays NULL (MySQL semantics)
-- @label:bvt

drop database if exists decimal_bool_context;
create database decimal_bool_context;
use decimal_bool_context;

select 0.5 and 1 as dec_and, 0.0 and 1 as zero_and, 0.5 or 0 as dec_or, 0.0 or 0 as zero_or;
select not 0.5 as not_dec, not 0.0 as not_zero, not -0.001 as not_neg, not cast(null as decimal(10,2)) as not_null;
select 0.5 xor 0 as dec_xor_false, 0.5 xor 1 as dec_xor_true, 0.0 xor 0.0 as zero_xor_zero;
select case when 0.5 then 'y' else 'n' end as case_dec, case when 0.0 then 'y' else 'n' end as case_zero;
select 1 as where_dec from dual where 0.5;
select 1 as where_zero from dual where 0.0;
select 0.5 is true as dec_is_true, 0.0 is false as zero_is_false, -2.25 is not false as neg_is_not_false;
select cast(0.001 as decimal(38,10)) and 1 as d128_and, cast(0 as decimal(38,10)) or 0 as d128_zero_or;
select cast(-12345678901234567890.123456789 as decimal(38,9)) and 1 as d128_big_neg;

drop table if exists t_dec_bool;
create table t_dec_bool(id int, a decimal(10,3), b decimal(38,20));
insert into t_dec_bool values
    (1, 0.5, 0.00000000000000000001),
    (2, 0, 0),
    (3, -2.25, -1),
    (4, null, null),
    (5, 0.001, 0);
select id, a and 1, a or 0, not a, a xor 1, b and 1, not b from t_dec_bool order by id;
select id from t_dec_bool where a order by id;
select id from t_dec_bool where not a order by id;
select id from t_dec_bool where b order by id;
select id from t_dec_bool where a and b order by id;
select id, case when a then 'true' when a is null then 'null' else 'false' end from t_dec_bool order by id;
select count(*) from t_dec_bool where b or a;

drop table t_dec_bool;
drop database decimal_bool_context;
