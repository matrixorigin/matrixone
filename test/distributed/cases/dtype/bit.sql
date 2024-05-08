-- @suite
-- @case
-- @desc:test for bit type
-- @label:bvt

-- create
drop table if exists t1;
create table t1 (a int, b bit(10));
show create table t1;
desc t1;

-- insert, support type cast from bool, hex/bit literal, char, float, and int
insert into t1 values (0, false);
insert into t1 values (1, true);
insert into t1 values (2, 0x2);
insert into t1 values (3, 0b11);
insert into t1 values (4, x'04');
insert into t1 values (5, b'101');
insert into t1 values (6, 'a');
insert into t1 values (6, 'ab');  -- error, data too long, bit_len('ab') = 16 > 10
insert into t1 values (7, 7.4999);  -- round(7.4999) = 7
insert into t1 values (8, 7.5);  -- round(7.5) = 8
insert into t1 values (9, 9);
insert into t1 values (10, 10);
insert into t1 values (10, 10);
insert into t1 values (1023, 0x3ff);
insert into t1 values (1024, 0x4ff);  -- error, data too long, bit_len(0x4ff) = 11 > 10

select * from t1;

-- update
update t1 set b = 6 where b = cast('a' as bit(10));
select * from t1;

-- filter
select * from t1 where b > 3 order by b desc;

-- aggregation
select sum(a), cast(b as unsigned) from t1 group by b having b > 3;

-- delete
delete from t1 where b >= 7 and b <= 10;
select * from t1;

-- functions
select cast(b as int) from t1;
select count(b) from t1;
select sum(b) from t1;
select min(b) from t1;
select max(b) from t1;
select avg(b) from t1;
select median(b) from t1;
select var_pop(b) from t1;
select stddev_pop(b) from t1;

-- add column with default value
ALTER TABLE t1 ADD c BIT(10) DEFAULT 0x1;
select * from t1;

-- add index
ALTER TABLE t1 ADD INDEX (c);
show create table t1;

-- add unique constraint
ALTER TABLE t1 ADD UNIQUE (b);
show create table t1;

-- add pk
ALTER TABLE t1 ADD PRIMARY KEY (b);
show create table t1;

-- drop index
ALTER TABLE t1 DROP INDEX c;
show create table t1;

-- drop column
ALTER TABLE t1 DROP COLUMN c;
show create table t1;

-- drop index
ALTER TABLE t1 DROP INDEX b;
show create table t1;

-- drop pk
ALTER TABLE t1 DROP PRIMARY KEY;
show create table t1;

-- modify column type
ALTER TABLE t1 MODIFY a bit(9);  -- error, data type length is too short
ALTER TABLE t1 MODIFY a bit(10);
show create table t1;
select * from t1;

-- modify column type as well as rename column name
ALTER TABLE t1 CHANGE a new_a int;
show create table t1;
select * from t1;
