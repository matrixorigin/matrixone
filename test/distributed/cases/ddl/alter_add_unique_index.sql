-- Attempt to create a composite unique key with VARCHAR and DATETIME, but with duplicates
DROP TABLE IF EXISTS t0;
CREATE TABLE t0(
    col1 VARCHAR(255), 
    col2 DATETIME, 
    col3 DECIMAL(10,2)
);

-- Inserting duplicate combination of col1 and col2
INSERT INTO t0 VALUES ('test', '2023-04-01 12:00:00', 123.45);
INSERT INTO t0 VALUES ('test', '2023-04-01 12:00:00', 678.90);
SELECT COUNT(*) FROM t0;

-- This next statement will fail because there is a duplicate value for (col1, col2)
ALTER TABLE t0 ADD UNIQUE KEY `unique_compound_key`(col1, col2) COMMENT 'Unique constraint for VARCHAR and DATETIME';

-- If the above statement would have succeeded, you would see information about the unique index
SHOW INDEX FROM t0;
SELECT COUNT(*) FROM t0;

DROP TABLE t0;

-- Attempt to create a composite unique key with DECIMAL and DATE, but with duplicates
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(
    col1 DECIMAL(5,2), 
    col2 DATETIME, 
    col3 int
);

INSERT INTO t1 VALUES (10.55, '2023-04-01', 100);
INSERT INTO t1 VALUES (10.55, '2023-04-01', 200);
SELECT COUNT(*) FROM t1;

-- This next statement will fail due to the duplicate values for (col1, col2)
ALTER TABLE t1 ADD UNIQUE KEY `unique_decimal_date_key`(col1, col2) COMMENT 'Unique constraint for DECIMAL and DATE';

-- If the ALTER TABLE statement succeeded, this would show the unique indexes
SHOW INDEX FROM t1;
SELECT COUNT(*) FROM t1;

DROP TABLE t1;

DROP TABLE IF EXISTS t3;
create table t3 (col1 bigint primary key,col2 char(25), col3 float, col4 char(50), key num_id(col4));
insert into t3 values (1,'',20.23,'5678'),(2,'13873458290',100.00,'23');
insert into t3 values (67834,'13456789872',20.23,'5678'),(56473,'',100.00,'5678');
ALTER TABLE t3 ADD UNIQUE KEY `unique_empty_char`(col2) COMMENT 'Unique constraint for empty char';
DROP TABLE t3;