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
