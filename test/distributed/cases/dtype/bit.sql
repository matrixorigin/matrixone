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

-- prepared parameters preserve NULL across first execution and statement reuse
DROP TABLE IF EXISTS prepared_bit_null;
CREATE TABLE prepared_bit_null (id INT PRIMARY KEY, b1 BIT(1), b8 BIT(8), b64 BIT(64));
PREPARE prepared_bit_insert FROM 'INSERT INTO prepared_bit_null VALUES (?, ?, ?, ?)';
SET @id = 1, @b1 = NULL, @b8 = NULL, @b64 = NULL;
EXECUTE prepared_bit_insert USING @id, @b1, @b8, @b64;
SET @id = 2, @b1 = 1, @b8 = 165, @b64 = 9223372036854775808;
EXECUTE prepared_bit_insert USING @id, @b1, @b8, @b64;
SET @id = 3, @b1 = NULL, @b8 = NULL, @b64 = NULL;
EXECUTE prepared_bit_insert USING @id, @b1, @b8, @b64;
SELECT id, b1 IS NULL, b8 IS NULL, b64 IS NULL, HEX(b1), HEX(b8), HEX(b64)
FROM prepared_bit_null ORDER BY id;
DEALLOCATE PREPARE prepared_bit_insert;
DROP TABLE prepared_bit_null;

-- BIT(64) arithmetic must preserve the unsigned upper half when paired with
-- signed integer literals.
DROP TABLE IF EXISTS issue_28685_bit64;
CREATE TABLE issue_28685_bit64 (id INT PRIMARY KEY, b BIT(64));
INSERT INTO issue_28685_bit64 VALUES
    (1, 0),
    (2, 9223372036854775807),
    (3, 9223372036854775808),
    (4, 18446744073709551615),
    (5, NULL);
SELECT b * b * b * b * b * b * b * b + 0 AS bit8_derived
FROM (SELECT CAST(255 AS BIT(8)) AS b) AS bit8_source;
-- Decimal256 arithmetic keeps the exact square instead of overflowing the
-- Decimal128 domain selected by the older resolver.
SELECT (b + 0) * (b + 0) AS widened_value
FROM issue_28685_bit64 WHERE id = 4;
SELECT 1 AS after_overflow;
SELECT id,
       b + 0 AS plus_zero,
       0 + b AS reverse_plus,
       b - 1 AS minus_one,
       1 - b AS reverse_minus,
       b * 1 AS multiply_one,
       1 * b AS reverse_multiply,
       b % 2 AS mod_two
FROM issue_28685_bit64 ORDER BY id;

PREPARE issue_28685_bit64_stmt FROM
    'SELECT id, b + ? AS value FROM issue_28685_bit64 ORDER BY id';
SET @issue_28685_bit64_param = 0;
EXECUTE issue_28685_bit64_stmt USING @issue_28685_bit64_param;
SET @issue_28685_bit64_param = -1;
EXECUTE issue_28685_bit64_stmt USING @issue_28685_bit64_param;
SET @issue_28685_bit64_param = 0;
EXECUTE issue_28685_bit64_stmt USING @issue_28685_bit64_param;
DEALLOCATE PREPARE issue_28685_bit64_stmt;

DROP TABLE IF EXISTS issue_28685_bit64_ctas;
CREATE TABLE issue_28685_bit64_ctas AS
    SELECT id, b + 0 AS value FROM issue_28685_bit64;
SELECT column_name, data_type, numeric_precision, numeric_scale
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'issue_28685_bit64_ctas'
ORDER BY ordinal_position;
SELECT id, value FROM issue_28685_bit64_ctas ORDER BY id;
DROP TABLE issue_28685_bit64_ctas;
DROP TABLE issue_28685_bit64;
