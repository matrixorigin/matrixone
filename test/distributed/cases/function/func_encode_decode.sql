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
SELECT serial_extract(serial_full(NULL, cast(1 as bigint)), 0 as varchar(1)); -- serial_full (data type doesn't matter for NULL)
SELECT serial_extract(serial_full(NULL, 1), 1 as int); -- error
SELECT serial_extract(serial_full(NULL, "adam"), 1 as varchar(4));
-- a potential dangerous case. we don't validate the subtype of Varlena. Need to be careful!!!
SELECT serial_extract(serial_full(NULL, "adam"), 1 as vecf32(4));


-- test min
CREATE TABLE t2 (name varchar(255), age int);
INSERT INTO t2 (name, age) VALUES ('Abby', 24);
INSERT INTO t2 (name,age) VALUES ('Alex',23);
INSERT INTO t2 (name, age) VALUES ('Carol', 23);
INSERT INTO t2 (age) VALUES (25);
select name, age from t2 order by name asc,age asc;
SELECT min( serial(t2.name, t2.age)) from t2;
SELECT min( serial_full(t2.name,t2.age)) from t2;
select  serial_extract(min, 0 as varchar(255)),  serial_extract(min, 1 as int) from (SELECT min( serial_full(t2.name,t2.age)) as min from t2);
select age,name from t2 order by age asc,name asc;
SELECT min( serial(t2.age,t2.name)) from t2;
SELECT min( serial_full(t2.age,t2.name)) from t2;
select  serial_extract(min, 0 as int),  serial_extract(min, 1 as varchar(255)) from (SELECT min( serial_full(t2.age,t2.name)) as min from t2);

-- test max
select name, age from t2 order by name desc,age desc;
SELECT max( serial(t2.name, t2.age)) from t2;
SELECT max( serial_full(t2.name,t2.age)) from t2;
select  serial_extract(max, 0 as varchar(255)),  serial_extract(max, 1 as int) from (SELECT max( serial_full(t2.name,t2.age)) as max from t2);
select age,name from t2 order by age desc,name desc;
SELECT max( serial(t2.age,t2.name)) from t2;
SELECT max( serial_full(t2.age,t2.name)) from t2;
select  serial_extract(max, 0 as int),  serial_extract(max, 1 as varchar(255)) from (SELECT max( serial_full(t2.age,t2.name)) as max from t2);
