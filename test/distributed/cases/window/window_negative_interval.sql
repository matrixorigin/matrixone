-- @suite

-- @case
-- @desc: reject negative temporal INTERVAL bounds in RANGE window frames
-- @label:bvt

drop database if exists window_negative_interval;
create database window_negative_interval;
use window_negative_interval;

create table t(id int primary key, ts timestamp(6), v int);
insert into t values
  (1, '2024-01-01 00:00:00.000000', 10),
  (2, '2024-01-01 00:00:01.000000', 20),
  (3, '2024-01-01 00:00:02.000000', 30);

select id, sum(v) over (order by ts range between interval -1 microsecond preceding and current row) as s from t order by id;
select id, sum(v) over (order by ts range between interval -1 second preceding and current row) as s from t order by id;
select id, sum(v) over (order by ts range between interval -1 minute preceding and current row) as s from t order by id;
select id, sum(v) over (order by ts range between interval -1 hour preceding and current row) as s from t order by id;
select id, sum(v) over (order by ts range between interval -1 day preceding and current row) as s from t order by id;
select id, sum(v) over (order by ts range between interval -1 month preceding and current row) as s from t order by id;
select id, sum(v) over (order by ts range between interval -1 year preceding and current row) as s from t order by id;

-- Positive and zero bounds remain valid, and the connection remains usable after errors.
select id, sum(v) over (order by ts range between interval 1 second preceding and current row) as s from t order by id;
select id, sum(v) over (order by ts range between interval 0 second preceding and current row) as s from t order by id;
select count(*) from t;

drop database window_negative_interval;
