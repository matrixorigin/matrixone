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
