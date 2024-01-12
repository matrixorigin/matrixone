-- test encode/decode function
SELECT encode('\xa7', 'hex');
SELECT decode('616263', 'hex');

SELECT encode('abc', 'hex'), decode('616263', 'hex');
SELECT encode('abc', 'base64'), decode('YWJj', 'base64');

SELECT decode('invalid', 'hex');
SELECT decode('invalid', 'base64');

SELECT encode('abc', 'fake');
SELECT decode('abc', 'fake');


-- test serial() and serial_full()
CREATE TABLE t1 (name varchar(255), age int);
INSERT INTO t1 (name, age) VALUES ('Abby', 24);
INSERT INTO t1 (age) VALUES (25);
INSERT INTO t1 (name, age) VALUES ('Carol', 23);
SELECT * FROM t1;
SELECT serial(name,age) from t1;
SELECT serial_full(name,age) from t1;


-- test serial_extract
SELECT serial_extract(serial(1,2), 0 as bigint);
SELECT serial_extract(serial(1,2), 1 as bigint);
SELECT serial_extract(serial(1,2), 2 as bigint); -- error
SELECT serial_extract(serial(1,"adam"), 1 as varchar(4));
SELECT serial_extract(serial(1,"adam"), 1 as varchar(255));
SELECT serial_extract(serial(1,cast("[1,2,3]" as vecf32(3))), 1 as vecf32(3));
SELECT serial_extract(serial(cast(2.45 as float),cast(3 as bigint)), 0 as float);
SELECT serial_extract(serial(cast(2.45 as float),cast(3 as bigint)), 1 as bigint);
SELECT serial_extract(serial(NULL, cast(1 as bigint)), 1 as bigint); -- serial NULL
SELECT serial_extract(serial_full(NULL, cast(1 as bigint)), 1 as bigint); -- serial_full
SELECT serial_extract(serial_full(NULL, 1), 1 as int); -- error
SELECT serial_extract(serial_full(NULL, "adam"), 1 as varchar(4));
-- a potential dangerous case. we don't validate the subtype of Varlena. Need to be careful!!!
SELECT serial_extract(serial_full(NULL, "adam"), 1 as vecf32(4));