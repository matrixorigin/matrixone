-- test hex/unhex and to_base64/from_base64 function
SELECT hex('\xa7');
SELECT unhex('616263');

SELECT hex('abc'), unhex('616263');
SELECT to_base64('abc'), from_base64('YWJj');

SELECT unhex('invalid');
SELECT from_base64('invalid');

SELECT TO_BASE64('abc'), FROM_BASE64(TO_BASE64('abc'));
SELECT HEX('abc'),UNHEX(HEX('abc'));

select to_base64(''),to_base64(NULL);
select to_base64('@#%#$^jfe12');
select to_base64(123dokgr);
select to_base64(-123);
select to_base64(2003-09-06);
select to_base64('2003-09-06');
select to_base64('数据库');

select from_base64(''),from_base64(NULL);
select from_base64('@#%#$^jfe12');

select from_base64(123dokgr);

select from_base64(-123);

select from_base64(2003-09-06);
select from_base64('5pWw5o2u5bqT');
select from_base64('MjAwMy0wOS0wNg==');
create table test_base(c1 varchar(25));
insert into test_base values(to_base64('blue')),(to_base64('232525')),(to_base64('lijfe23253'));
select from_base64(c1) from test_base;

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

-- test function serial()、serial_full()、max() and min()
drop table if exists test01;
create table test01 (col1 bigint, col2 varchar(10), col3 char);
insert into test01 values (1392034, 'database', 'a');
insert into test01 values (23849242, 'abcdai', 'b');
insert into test01 values (-32934, 'mo', 'c');
insert into test01 values (null, null, null);
select * from test01;
select serial(col1, col2) from test01;
select serial(col1, col2, col3) from test01;
select serial_full(col1, col2, col3) from test01;
select serial_full(col1, col3) from test01;
select max(serial(col1, col2)) from test01;
select max(serial(col1, col2, col3)) from test01;
select min(serial_full(col1, col2, col3)) from test01;
select min(serial_full(col1, col3)) from test01;
drop table test01;

drop table if exists test02;
create table test02 (col1 int, col2 decimal, col3 char);
insert into test02 values (1,2,'a');
insert into test02 values (2,3,'b');
insert into test02 values (null, null, null);
select * from test02;
select serial_extract(max(serial(col1, col2, col3)), 1 as decimal) from test02;
select serial_extract(min(serial(col1, col2, col3)), 1 as decimal) from test02;
select serial_extract(max(serial_full(cast(col1 as decimal), cast(col2 as double))), 0 as decimal) from test02;
select serial_extract(min(serial_full(cast(col1 as decimal), cast(col2 as double))), 0 as decimal) from test02;
drop table test02;

drop table if exists vtab64;
create table vtab64(id int primary key auto_increment,`vecf64_3` vecf64(3),`vecf64_5` vecf64(5));
insert into vtab64(vecf64_3,vecf64_5) values("[0.8166459,NULL,0.4886152]",NULL);
insert into vtab64(vecf64_3,vecf64_5) values(NULL,NULL);
insert into vtab64(vecf64_3,vecf64_5) values("[0.8166459,0.66616553,0.4886152]",NULL);
insert into vtab64(vecf64_3,vecf64_5) values ("[8.5606893,6.7903588,821.977768]","[0.46323407,23.49801546,563.9229458,56.07673508,8732.9583881]");
insert into vtab64(vecf64_3,vecf64_5) values ("[0.9260021,0.26637346,0.06567037]","[0.45756745,65.2996871,321.623636,3.60082066,87.58445764]");
select * from vtab64;
select serial_extract(max(serial(id, `vecf64_3`, `vecf64_5`)), 1 as vecf64(3)) from vtab64;
select serial_extract(min(serial(id, `vecf64_3`, `vecf64_5`)), 1 as vecf64(3)) from vtab64;
select serial_extract(max(serial_full(cast(id as decimal), `vecf64_3`)), 0 as decimal) from vtab64;
select serial_extract(min(serial_full(cast(id as decimal), `vecf64_3`)), 1 as vecf64(3)) from vtab64;
drop table vtab64;
