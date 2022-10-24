
-- test different type union, union all

drop table if exists t1;
create table t1(
a int,
b varchar(100)
);

insert into t1 values(30, 'cccc');
insert into t1 values(20, 'bbbb');
insert into t1 values(10, 'aaaa');
insert into t1 values ();
select * from t1;


drop table if exists t2;
create table t2(
col1 date,
col2 datetime,
col3 timestamp
);

insert into t2 values ();
insert into t2 values('2022-01-01', '2022-01-01', '2022-01-01');
insert into t2 values('2022-01-01', '2022-01-01 00:00:00', '2022-01-01 00:00:00.000000');
insert into t2 values('2022-01-01', '2022-01-01 00:00:00.000000', '2022-01-01 23:59:59.999999');
select * from t2;

-- test int type union all date type

(select a from t1 union all select col1 from t2) order by a;

(select a from t1 union all select col1 from t2) order by col1;

select a from t1 union all select col1 from t2;

(select a from t1 order by a) union all select col1 from t2;
(select a from t1) union all (select col1 from t2 order by col1);
(select a from t1 order by a) union all (select col1 from t2 order by col1);

(select a from t1 union all select col1 from t2) order by col1;

-- test int type union datetime type

(select a from t1 union select col2 from t2) order by a;

(select a from t1 union select col2 from t2) order by col2;

select a from t1 union select col2 from t2;

(select a from t1 order by a) union select col2 from t2;
(select a from t1) union (select col2 from t2 order by col2);
(select a from t1 order by a) union (select col2 from t2 order by col2);

(select a from t1 union select col2 from t2) order by col2;

drop table t1;
drop table t2;


-- test different length type union
drop table if exists t3;
create table t3(
a tinyint
);

insert into t3 values (20),(10),(30),(-10);

drop table if exists t4;
create table t4(
col1 smallint,
col2 smallint unsigned,
col3 float,
col4 bool
);

insert into t4 values(100, 65535, 127.0, 1);
insert into t4 values(300, 0, 1.0, 0);
insert into t4 values(500, 100, 0.0, 0);
insert into t4 values(200, 35, 127.0, 1);
insert into t4 values(200, 35, 127.44, 1);

select a from t3 union select col1 from t4;

(select a from t3) union (select col2 from t4 order by col2);

select a from t3 union select col2 from t4;

select a from t3 union select col3 from t4;

-- @bvt:issue#4942
select a from t3 union select col4 from t4;
-- @bvt:issue

drop table t3;
drop table t4;

-- test int type and text type union varchar type and text type
drop table if exists t5;
create table t5(
a int,
b text
);

insert into t5 values (12, 'aa');
insert into t5 values (20, 'bb');
insert into t5 values (18, 'aa');
insert into t5 values (15, 'bb');

drop table if exists t6;
create table t6 (
col1 varchar(100),
col2 text
);

insert into t6 values ('aa', '11');
insert into t6 values ('bb', '22');
insert into t6 values ('cc', '33');
insert into t6 values ('dd', '44');

select a from t5 union select col1 from t6;

select a from t5 union select col2 from t6;
select b from t5 union select col1 from t6;
select b from t5 union select col2 from t6;


drop table t5;
drop table t6;

-- test subquery union, union all
drop table if exists t7;
CREATE TABLE t7 (
a int not null,
b char (10) not null
);

insert into t7 values(1,'a'),(2,'b'),(3,'c'),(3,'c');

select * from t7 union select * from t7 limit 2;

select * from (select * from t7 union select * from t7) a;

select * from (select * from t7 union all select * from t7) a;

select * from (select * from t7 union all select * from t7 limit 2) a;
select * from (select * from t7 union select * from t7 limit 2) a;

select * from (select * from t7 union select * from t7 where a > 1) a;
select * from (select * from t7 union all select * from t7 where a > 1) a;

select * from (select * from t7 union select * from t7 where a < 1) a;
select * from (select * from t7 union all select * from t7 where a < 1) a;

select * from (select * from t7 where a > 1 union select * from t7 where a < 1) a;
select * from (select * from t7 where a > 1 union all select * from t7 where a < 1) a;

select * from (select * from t7 where a >=1 union select * from t7 where a <= 1) a;
select * from (select * from t7 where a >=1 union all select * from t7 where a <= 1) a;

select * from (select * from t7 where a between 1 and 3 union select * from t7 where a <= 1) a;
select * from (select * from t7 where a between 1 and 3 union all select * from t7 where a <= 1) a;

select * from (select * from t7 where a between 1 and 3 union all select * from t7 where a between 3 and 1) a;
select * from (select * from t7 where a between 1 and 3 union all select * from t7 where a between 3 and 1) a;

drop table t7;


-- test union distinct, union all, union
create table t8(a int);
create table t9(a int);
create table t10(a int);
insert into t8 values(1),(1);
insert into t9 values(2),(2);
insert into t10 values(3),(3);

select * from t8 union distinct select * from t9 union all select * from t10;

select * from t8 union distinct select * from t9 union distinct select * from t10;

select * from (select * from t8 union distinct select * from t9 union all select * from t10) X;

-- @bvt:issue#4946
select * from t8 union select * from t9 intersect select * from t10;
select * from t8 union select * from t9 minus select * from t10;
(select * from t8 union select * from t9) intersect select * from t10;
(select * from t8 union select * from t9) minus select * from t10;
-- @bvt:issue
drop table t8;
drop table t9;
drop table t10;


-- test select ... union select case ... when ...;
SELECT 'case+union+test' UNION
SELECT CASE '1' WHEN '2' THEN 'BUG' ELSE 'nobug' END;

select 'case+union+tet' union
SELECT CASE '1' WHEN '1' THEN 'BUG' ELSE 'nobug' END;

SELECT 1, 2 UNION SELECT 'a', 'b';


-- test union and concat function
select 'a' union select concat('a', -4);
select 'a' union select concat('a', -4.5);
select 'a' union select concat('a', -(4 + 1));
select 'a' union select concat('a', 4 - 5);
select 'a' union select concat('a', -'3');
select 'a' union select concat('a', -concat('3',4));
select 'a' union select concat('a', -0);
select 'a' union select concat('a', -0.0);
select 'a' union select concat('a', -0.0000);

select concat((select x from (select 'a' as x) as t1 ),
(select y from (select 'b' as y) as t2 )) from (select 1 union select 2 )
as t3;

drop table if exists t11;
create table t11(f1 varchar(6));
insert into t11 values ("123456");
select concat(f1, 2) a from t11 union select 'x' a from t11;
drop table t11;


-- test union all, where
drop table if exists t12;
create table t12 (EVENT_ID int auto_increment primary key,  LOCATION char(20));
insert into t12 values (NULL,"Mic-4"),(NULL,"Mic-5"),(NULL,"Mic-6");
SELECT LOCATION FROM t12 WHERE EVENT_ID=2 UNION ALL  SELECT LOCATION FROM t12 WHERE EVENT_ID=3;
SELECT LOCATION FROM t12 WHERE EVENT_ID=2 UNION ALL  SELECT LOCATION FROM t12 WHERE EVENT_ID=3;
SELECT LOCATION FROM t12 WHERE EVENT_ID=2 UNION ALL  SELECT LOCATION FROM t12 WHERE EVENT_ID=3;
drop table t12;


-- test union prepare
drop table if exists t13;
create table t13 (a int primary key);
insert into t13 values (1);
select * from t13 where 3 in (select (1+1) union select 1);
select * from t13 where 3 in (select (1+2) union select 1);
prepare st_18492 from 'select * from t13 where 3 in (select (1+1) union select 1)';
execute st_18492;

prepare st_18493 from 'select * from t13 where 3 in (select (2+1) union select 1)';
execute st_18493;

deallocate prepare st_18492;
deallocate prepare st_18493;
drop table t13;

-- @bvt:issue#4635
select cast(a as DECIMAL(3,2))
 from (select 11.1233 as a
  UNION select 11.1234
  UNION select 12.1234
 ) t;
-- @bvt:issue


drop table if exists t14;
CREATE TABLE t14 (
  `pseudo` char(35) NOT NULL default '',
  `pseudo1` char(35) NOT NULL default '',
  `same` tinyint(1) unsigned NOT NULL default '1',
  PRIMARY KEY  (`pseudo1`),
  KEY `pseudo` (`pseudo`)
);

INSERT INTO t14 (pseudo,pseudo1,same) VALUES
('joce', 'testtt', 1),('joce', 'tsestset', 1),('dekad', 'joce', 1);

SELECT pseudo FROM t14 WHERE pseudo1='joce' UNION SELECT pseudo FROM t14 WHERE pseudo='joce';
SELECT pseudo1 FROM t14 WHERE pseudo1='joce' UNION SELECT pseudo1 FROM t14 WHERE pseudo='joce';
SELECT * FROM t14 WHERE pseudo1='joce' UNION SELECT * FROM t14 WHERE pseudo='joce' order by pseudo desc,pseudo1 desc;
SELECT pseudo1 FROM t14 WHERE pseudo='joce' UNION SELECT pseudo FROM t14 WHERE pseudo1='joce';
SELECT pseudo1 FROM t14 WHERE pseudo='joce' UNION ALL SELECT pseudo FROM t14 WHERE pseudo1='joce';

drop table t14;


-- test union,union all, join, left join, right join
drop table if exists t15;

CREATE TABLE t15 (
id int(3) unsigned default '0'
);

INSERT INTO t15 (id) VALUES("1");

drop table if exists t16;
CREATE TABLE t16 (
id int(3) unsigned default '0',
id_master int(5) default '0',
text15 varchar(5) default NULL,
text16 varchar(5) default NULL
);

INSERT INTO t16 (id, id_master, text15, text16) VALUES("1", "1", "foo1", "bar1");
INSERT INTO t16 (id, id_master, text15, text16) VALUES("2", "1", "foo2", "bar2");
INSERT INTO t16 (id, id_master, text15, text16) VALUES("3", "1", NULL, "bar3");
INSERT INTO t16 (id, id_master, text15, text16) VALUES("4", "1", "foo4", "bar4");

SELECT 1 AS id_master, 1 AS id, NULL AS text15, 'ABCDE' AS text16
UNION
SELECT id_master, t16.id, text15, text16 FROM t15 LEFT JOIN t16 ON t15.id = t16.id_master;

SELECT 1 AS id_master, 1 AS id, NULL AS text15, 'ABCDE' AS text16
UNION ALL
SELECT id_master, t16.id, text15, text16 FROM t15 LEFT JOIN t16 ON t15.id = t16.id_master;


SELECT 1 AS id_master, 1 AS id, 'ABCDE' AS text15, 'ABCDE' AS text16
UNION
SELECT id_master, t16.id, text15, text16 FROM t15 LEFT JOIN t16 ON t15.id = t16.id_master;

SELECT 1 AS id_master, 1 AS id, 'ABCDE' AS text15, 'ABCDE' AS text16
UNION
SELECT id_master, t16.id, text15, text16 FROM t15 right join  t16 ON t15.id = t16.id_master;

SELECT 1 AS id_master, 1 AS id, 'ABCDE' AS text15, 'ABCDE' AS text16
UNION
SELECT id_master, t16.id, text15, text16 FROM t15 JOIN t16 ON t15.id = t16.id_master;

SELECT 1 AS id_master, 1 AS id, 'ABCDE' AS text15, 'ABCDE' AS text16
UNION ALL
SELECT id_master, t16.id, text15, text16 FROM t15 LEFT JOIN t16 ON t15.id = t16.id_master;

SELECT 1 AS id_master, 1 AS id, 'ABCDE' AS text15, 'ABCDE' AS text16
UNION ALL
SELECT id_master, t16.id, text15, text16 FROM t15 right JOIN t16 ON t15.id = t16.id_master;

SELECT 1 AS id_master, 1 AS id, 'ABCDE' AS text15, 'ABCDE' AS text16
UNION ALL
SELECT id_master, t16.id, text15, text16 FROM t15  JOIN t16 ON t15.id = t16.id_master;

drop table t15;
drop table t16;


drop table if exists t17;
create table t17 (
RID int(11) not null default '0',
IID int(11) not null default '0',
nada varchar(50)  not null,
NAME varchar(50) not null,
PHONE varchar(50) not null);

insert into t17 ( RID,IID,nada,NAME,PHONE) values
(1, 1, 'main', 'a', '111'),
(2, 1, 'main', 'b', '222'),
(3, 1, 'main', 'c', '333'),
(4, 1, 'main', 'd', '444'),
(5, 1, 'main', 'e', '555'),
(6, 2, 'main', 'c', '333'),
(7, 2, 'main', 'd', '454'),
(8, 2, 'main', 'e', '555'),
(9, 2, 'main', 'f', '666'),
(10, 2, 'main', 'g', '777');

select A.NAME, A.PHONE, B.NAME, B.PHONE from t17 A
left join t17 B on A.NAME = B.NAME and B.IID = 2 where A.IID = 1 and (A.PHONE <> B.PHONE or B.NAME is null)
union
select A.NAME, A.PHONE, B.NAME, B.PHONE from t17 B left join t17 A on B.NAME = A.NAME and A.IID = 1
where B.IID = 2 and (A.PHONE <> B.PHONE or A.NAME is null);

select A.NAME, A.PHONE, B.NAME, B.PHONE from t17 A
left join t17 B on A.NAME = B.NAME and B.IID = 2 where A.IID = 1 and (A.PHONE <> B.PHONE or B.NAME is null)
union all
select A.NAME, A.PHONE, B.NAME, B.PHONE from t17 B left join t17 A on B.NAME = A.NAME and A.IID = 1
where B.IID = 2 and (A.PHONE <> B.PHONE or A.NAME is null);

drop table t17;