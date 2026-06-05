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
drop table t_func_interval;
