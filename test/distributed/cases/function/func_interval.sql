-- @suite
-- @case
-- @desc:test for mysql interval() function boundary comparison semantics
-- @label:bvt

-- MySQL examples and basic sorted threshold lookup.
select interval(23, 1, 15, 17, 30, 44, 200);
select interval(10, 1, 10, 100, 1000);
select interval(22, 23, 30, 44, 200);

-- Strict comparison coverage: equality should advance to the next threshold.
select interval(1, 1, 10, 100);
select interval(10, 1, 10, 100);
select interval(100, 1, 10, 100);
select interval(101, 1, 10, 100);

-- Values immediately around thresholds.
select interval(0, 1, 10, 100);
select interval(2, 1, 10, 100);
select interval(9, 1, 10, 100);
select interval(11, 1, 10, 100);
select interval(99, 1, 10, 100);

-- Duplicate thresholds: equal values should continue past all equal thresholds.
select interval(10, 1, 10, 10, 10, 100);
select interval(9, 1, 10, 10, 10, 100);
select interval(11, 1, 10, 10, 10, 100);

-- Null handling.
select interval(null, 1, 10, 100);
select interval(null, null, 10, 100);
select interval(10, null, 10, 100);
select interval(10, 1, null, 100);

-- Signed values and negative boundaries.
select interval(-10, -20, -10, 0, 10);
select interval(-9, -20, -10, 0, 10);
select interval(-21, -20, -10, 0, 10);

-- Type conversion to integer.
select interval(2.9, 1, 2, 3, 4);
select interval('10', '1', '10', '100');
select interval('10.9', '1', '10', '100');

-- First argument can be an expression.
select interval(5 + 5, 1, 10, 100);
select interval(20 div 2, 1, 10, 100);
select interval(21 mod 11, 1, 10, 100);

-- Minimum valid argument count.
select interval(0, 1);
select interval(1, 1);
select interval(2, 1);

-- Mixed real and decimal comparison.
select interval(2, 1.5, 2.5, 3.5);
select interval(cast(2.90 as decimal(10,2)), cast(1.0 as decimal(10,1)), cast(2.90 as decimal(10,2)), cast(3.0 as decimal(10,1)));
select interval(cast(2.90 as decimal(10,2)), 1, 2, 3);

-- Mixed regression: new INTERVAL(...) builtin must not steal legacy INTERVAL expr syntax.
select interval(5, 1, 5, 10) as builtin_interval, date_add('2024-01-01', INTERVAL 1 DAY) as legacy_date_add, '2024-01-01' + INTERVAL 1 DAY as legacy_plus_interval;
select interval(5, '1', '5', '10') as builtin_interval_string_arg, date_sub('2024-01-02', INTERVAL 1 DAY) as legacy_date_sub, '2024-01-02' - INTERVAL 1 DAY as legacy_minus_interval;
select interval(10, 1, 10, 100) as builtin_interval, date_add('2024-01-01 00:00:00', INTERVAL '1:30' HOUR_MINUTE) as legacy_compound_unit;
select interval(-1, -10, -1, 0) as builtin_negative_interval, date_add('2024-01-02', INTERVAL -1 DAY) as legacy_negative_interval;
select case when interval(5, 1, 5, 10) = 2 then date_add('2024-01-01', INTERVAL 1 DAY) else date_sub('2024-01-01', INTERVAL 1 DAY) end as mixed_case_interval;
select date_add('2024-01-01', INTERVAL 1 DAY) as legacy_interval_in_select where interval(5, 1, 5, 10) = 2;

-- Should errors
select interval('', 1, 10, 100);
select interval('   ', 1, 10, 100);
select interval('1abc', 1, 10, 100);
select interval('abc', 1, 10, 100);
select interval('10.9abc', 1, 10, 100);

-- Non-constant inputs: table fields, null fields, and const/non-const mixed arguments.
drop table if exists t_func_interval;
create table t_func_interval(
    id int,
    n int,
    f double,
    d decimal(10,2),
    s varchar(20),
    t1 int,
    t2 int,
    t3 int,
    st1 varchar(20),
    st2 varchar(20)
);
insert into t_func_interval values
    (1, 0, 0.5, 0.50, '0', 1, 10, 100, '1', '10'),
    (2, 5, 2.9, 2.90, '2.9', 1, 10, 100, '1', '2'),
    (3, 10, 10.9, 10.90, '10.9', 1, null, 100, '1', '10'),
    (4, null, null, null, null, 1, 10, 100, '1', '10');
select id, interval(n, t1, t2, t3) from t_func_interval order by id;
select id, interval(f, 1, t2, 100) from t_func_interval order by id;
select id, interval(d, cast(1 as decimal(10,2)), cast(2.90 as decimal(10,2)), cast(100 as decimal(10,2))) from t_func_interval order by id;
select id, interval(s, st1, st2, '100') from t_func_interval order by id;
select id, interval(n, null, t2, t3) from t_func_interval order by id;
select id, interval(n, t1, t2, t3), date_add('2024-01-01', INTERVAL 1 DAY) from t_func_interval where interval(n, t1, t2, t3) >= 0 order by id;
drop table t_func_interval;

-- Mixed regression with time-window rewrite and INTERVAL(...) builtin in one SQL path.
drop table if exists t_interval_time_window;
create table t_interval_time_window(ts timestamp primary key, n int);
insert into t_interval_time_window values
    ('2024-01-01 00:00:00', 1),
    ('2024-01-02 00:00:00', 5),
    ('2024-01-03 00:00:00', 10);
select _wstart, _wend, interval(max(n), 1, 5, 10) from t_interval_time_window where ts >= '2024-01-01 00:00:00' and ts < '2024-01-04 00:00:00' interval(ts, 1, day) order by _wstart;
select _wstart, _wend, interval(count(n), 1, 2, 3) from t_interval_time_window where ts >= '2024-01-01 00:00:00' and ts < '2024-01-04 00:00:00' interval(ts, 1, day) sliding(1, day) fill(none) order by _wstart;
select _wstart, _wend, interval(max(n), 1, 5, 10), date_add(max(ts), INTERVAL 1 DAY) from t_interval_time_window where ts >= '2024-01-01 00:00:00' and ts < '2024-01-04 00:00:00' interval(ts, 1, day) order by _wstart;
drop table t_interval_time_window;
