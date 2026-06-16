-- @suite
-- @case
-- @desc:smoke test: NUMERIC is a SQL alias for DECIMAL
-- @label:bvt

-- Expression-level alias
select cast(12345.678 as numeric(10,3));
select cast(-99.99 as numeric(5,2)) + cast(100 as numeric(5,2));

-- DDL: create table with NUMERIC columns
drop table if exists t_numeric_smoke;
create table t_numeric_smoke (
  id int auto_increment primary key,
  val numeric(20,4) not null,
  big numeric(38,10)
);
insert into t_numeric_smoke (val, big) values (1234.5678, 99999999999999999999999999.1234567890);
insert into t_numeric_smoke (val, big) values (-0.0001, -1);
select val, big from t_numeric_smoke order by id;
select sum(val), avg(big) from t_numeric_smoke;
drop table t_numeric_smoke;
