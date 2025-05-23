-- @skip:issue#16438
drop database if exists db1;
create database db1;
use db1;
-----------------------------------------------range partition------------------------------------------------------
drop table if exists employees;
CREATE TABLE employees (
      emp_no      INT             NOT NULL,
      birth_date  DATE            NOT NULL,
      first_name  VARCHAR(14)     NOT NULL,
      last_name   VARCHAR(16)     NOT NULL,
      gender      varchar(5)      NOT NULL,
      hire_date   DATE            NOT NULL,
      PRIMARY KEY (emp_no)
) PARTITION BY RANGE columns (emp_no)(
    partition p01 values less than (100001),
    partition p02 values less than (200001),
    partition p03 values less than (300001),
    partition p04 values less than (400001)
);

INSERT INTO employees VALUES
                          (9001,'1980-12-17', 'SMITH', 'CLERK', 'F', '2008-12-17'),
                          (9002,'1981-02-20', 'ALLEN', 'SALESMAN', 'F', '2008-02-20'),
                          (9003,'1981-02-22', 'WARD', 'SALESMAN', 'M', '2005-02-22'),
                          (9004,'1981-04-02', 'JONES', 'MANAGER', 'M', '2003-04-02'),
                          (9005,'1981-09-28', 'MARTIN', 'SALESMAN', 'F','2003-09-28'),
                          (9006,'1981-05-01', 'BLAKE', 'MANAGER', 'M', '2003-05-01'),
                          (9007,'1981-06-09', 'CLARK', 'MANAGER', 'F', '2005-06-09'),
                          (9008,'1987-07-13', 'SCOTT', 'ANALYST', 'F', '2001-07-13'),
                          (9009,'1981-11-17', 'KING', 'PRESIDENT', 'M','2001-11-17'),
                          (9010,'1981-09-08', 'TURNER', 'SALESMAN', 'M','2001-09-08'),
                          (9011,'1997-07-13', 'ADAMS', 'CLERK', 'F', '2003-07-13'),
                          (100001, '1953-09-02', 'Georgi', 'Facello', 'M', '1986-06-26'),
                          (100002, '1964-06-02', 'Bezalel', 'Simmel', 'F', '1985-11-21'),
                          (100003, '1959-12-03', 'Parto', 'Bamford', 'F', '1986-08-28'),
                          (100004, '1954-05-01', 'Chirstian', 'Koblick', 'F', '1986-12-01'),
                          (100005, '1955-01-21', 'Kyoichi', 'Maliniak', 'M', '1989-09-12'),
                          (100006, '1953-04-20', 'Anneke', 'Preusig', 'M', '1989-06-02'),
                          (100007, '1957-05-23', 'Tzvetan', 'Zielinski', 'F', '1989-02-10'),
                          (100008, '1958-02-19', 'Saniya', 'Kalloufi', 'M', '1994-09-15'),
                          (100009, '1952-04-19', 'Sumant', 'Peac', 'F', '1985-02-18'),
                          (100000, '1963-06-01', 'Duangkaew', 'Piveteau', 'F', '1989-08-24'),
                          (200001, '1953-11-07', 'Mary', 'Sluis', 'M' ,'1990-01-22'),
                          (200002, '1960-10-04', 'Patricio', 'Bridgland', 'F', '1992-12-18'),
                          (200003, '1963-06-07', 'Eberhardt', 'Terkki', 'M', '1985-10-20'),
                          (200004, '1956-02-12', 'Berni', 'Genin', 'F', '1987-03-11'),
                          (200005, '1959-08-19', 'Guoxiang', 'Nooteboom', 'F', '1987-07-02'),
                          (200006, '1961-05-02', 'Kazuhito', 'Cappelletti', 'F', '1995-01-27'),
                          (200007, '1958-07-06', 'Cristinel', 'Bouloucos', 'M', '1993-08-03'),
                          (200008, '1954-06-19', 'Kazuhide', 'Peha', 'M', '1987-04-03'),
                          (200009, '1953-01-23', 'Lillian', 'Haddadi', 'M', '1999-04-30'),
                          (200010, '1952-12-24', 'Mayuko', 'Warwick', 'M', '1991-01-26'),
                          (300001, '1960-02-20', 'Ramzi', 'Erde', 'F', '1988-02-10'),
                          (300002, '1952-07-08', 'Shahaf', 'Famili', 'F', '1995-08-22'),
                          (300003, '1953-09-29', 'Bojan', 'Montemayor', 'M', '1989-12-17'),
                          (300004, '1958-09-05', 'Suzette', 'Pettey', 'M', '1997-05-19'),
                          (300005, '1958-10-31', 'Prasadram', 'Heyers', 'F', '1987-08-17'),
                          (300006, '1953-04-03', 'Yongqiao', 'Berztiss', 'F', '1995-03-20'),
                          (300007, '1962-07-10', 'Divier', 'Reistad', 'M', '1989-07-07'),
                          (300008, '1963-11-26', 'Domenick', 'Tempesti', 'M', '1991-10-22'),
                          (300009, '1956-12-13', 'Otmar', 'Herbst', 'F', '1985-11-20'),
                          (300010, '1958-07-14', 'Elvis', 'Demeyer', 'M', '1994-02-17');

SELECT table_schema, table_name, partition_name, partition_ordinal_position, partition_method, partition_expression
FROM information_schema.PARTITIONS
WHERE table_schema = 'db1' AND table_name = 'employees';

-- 添加新分区
ALTER TABLE employees ADD PARTITION (PARTITION p05 VALUES LESS THAN (500001));

select table_schema, table_name, partition_name,partition_ordinal_position,partition_method,partition_expression
from information_schema.PARTITIONS
where table_schema = 'db1' and table_name = 'employees';

INSERT INTO employees VALUES
                          (400001, '1959-01-27', 'Karsten', 'Joslin', 'F',  '1991-09-01'),
                          (400002, '1960-08-09', 'Jeong', 'Reistad', 'M', '1990-06-20'),
                          (400003, '1956-11-14', 'Arif', 'Merlo', 'F', '1987-03-18'),
                          (400004, '1962-12-29', 'Bader', 'Swan', 'M', '1988-09-21'),
                          (400005, '1953-02-08', 'Alain', 'Chappelet', 'M', '1988-09-05'),
                          (400006, '1959-08-10', 'Adamantios', 'Portugali', 'F', '1992-01-03'),
                          (400007, '1963-07-22', 'Pradeep', 'Makrucki', 'M','1990-12-05'),
                          (400008, '1960-07-20', 'Huan', 'Lortz',  'M', '1989-09-20'),
                          (400009, '1959-10-01', 'Alejandro', 'Brender', 'F', '1988-01-19'),
                          (400010, '1959-09-13', 'Weiyi', 'Meriste',  'F', '1993-02-14');

select * from employees order by emp_no;

drop table if exists titles;
CREATE TABLE titles (
       emp_no      INT             NOT NULL,
       title       VARCHAR(50)     NOT NULL,
       from_date   DATE            NOT NULL,
       to_date     DATE,
       PRIMARY KEY (emp_no,title, from_date)
) PARTITION BY RANGE (to_days(from_date))
(
	partition p01 values less than (to_days('1985-12-31')),
	partition p02 values less than (to_days('1986-12-31')),
	partition p03 values less than (to_days('1987-12-31')),
	partition p04 values less than (to_days('1988-12-31')),
	partition p05 values less than (to_days('1989-12-31')),
	partition p06 values less than (to_days('1990-12-31')),
	partition p07 values less than (to_days('1991-12-31'))
);

select table_schema, table_name, partition_name,partition_ordinal_position,partition_method,partition_expression
from information_schema.PARTITIONS
where table_schema = 'db1' and table_name = 'titles';

INSERT INTO titles VALUES
                       (10001, 'Facello', '1985-09-02','1986-06-26'),
                       (10002, 'Simmel', '1985-06-02','1985-11-21'),
                       (10003, 'Bamford', '1986-12-03','1986-08-28'),
                       (10004, 'Koblick', '1986-05-01','1986-12-01'),
                       (10005, 'Maliniak', '1987-01-21','1989-09-12'),
                       (10006, 'Preusig', '1987-04-20','1989-06-02'),
                       (10007, 'Zielinski', '1987-05-23','1989-02-10'),
                       (10008, 'Kalloufi', '1988-02-19','1994-09-15'),
                       (10009, 'Peac', '1988-04-19','1985-02-18'),
                       (10010, 'Piveteau', '1989-06-01','1989-08-24'),
                       (10011, 'Sluis', '1989-11-07','1990-01-22'),
                       (10012, 'Bridgland', '1990-10-04','1992-12-18'),
                       (10013, 'Terkki', '1990-06-07','1985-10-20'),
                       (10014, 'Genin', '1991-02-12','1987-03-11'),
                       (10015, 'Nooteboom', '1991-08-19','1987-07-02'),
                       (10016, 'Cappelletti', '1991-05-02','1995-01-27');

ALTER TABLE titles ADD PARTITION (
	partition p08 values less than (to_days('1992-12-31')),
	partition p09 values less than (to_days('1993-12-31')),
	partition p10 values less than (to_days('1994-12-31')),
	partition p11 values less than (to_days('1995-12-31'))
);

select table_schema, table_name, partition_name,partition_ordinal_position,partition_method,partition_expression
from information_schema.PARTITIONS
where table_schema = 'db1' and table_name = 'titles';

INSERT INTO titles VALUES
                       (10017, 'Bouloucos', '1992-07-06','1993-08-03'),
                       (10018, 'Peha', '1992-06-19','1987-04-03'),
                       (10019, 'Haddadi', '1992-01-23','1999-04-30'),
                       (10020, 'Warwick', '1993-12-24','1991-01-26'),
                       (10021, 'Erde', '1993-02-20','1988-02-10'),
                       (10022, 'Famili', '1993-07-08','1995-08-22'),
                       (10023, 'Montemayor', '1994-09-29','1989-12-17'),
                       (10024, 'Pettey', '1994-09-05','1997-05-19'),
                       (10025, 'Heyers', '1994-10-31','1987-08-17'),
                       (10026, 'Berztiss', '1995-04-03','1995-03-20'),
                       (10027, 'Reistad', '1995-07-10','1989-07-07'),
                       (10028, 'Tempesti', '1995-11-26','1991-10-22'),
                       (10029, 'Herbst', '1995-12-13','1985-11-20');

select * from titles order by emp_no;

drop table if exists pt1;
CREATE TABLE pt1 (
      id INT,
      date_column DATE,
      value INT
) PARTITION BY RANGE (YEAR(date_column)) (
  PARTITION p1 VALUES LESS THAN (2010) COMMENT 'Before 2010',
  PARTITION p2 VALUES LESS THAN (2020) COMMENT '2010 - 2019',
  PARTITION p3 VALUES LESS THAN (2021) COMMENT '2020 - 2021'
);

select table_schema, table_name, partition_name,partition_ordinal_position,partition_method,partition_expression
from information_schema.PARTITIONS
where table_schema = 'db1' and table_name = 'pt1';

ALTER TABLE pt1 ADD PARTITION (PARTITION p4 VALUES LESS THAN (2022) comment '2021 - 2022');

select table_schema, table_name, partition_name,partition_ordinal_position,partition_method,partition_expression
from information_schema.PARTITIONS
where table_schema = 'db1' and table_name = 'pt1';

-------------------------容错测试--------------------------
drop table if exists pt2;
CREATE TABLE pt2 (
      id INT,
      date_column DATE,
      value INT
)
PARTITION BY RANGE (YEAR(date_column)) (
  PARTITION p1 VALUES LESS THAN (2010) COMMENT 'Before 2010',
  PARTITION p2 VALUES LESS THAN (2020) COMMENT '2010 - 2019',
  PARTITION p3 VALUES LESS THAN MAXVALUE COMMENT '2020 and Beyond'
);

INSERT INTO pt2 VALUES
        (4001, '2005-01-27', 12000),
        (4002, '2007-08-09', 2700),
        (4003, '2019-11-14', 25000),
        (4004, '2017-12-29', 49000);

select table_schema, table_name, partition_name,partition_ordinal_position,partition_method,partition_expression
from information_schema.PARTITIONS
where table_schema = 'db1' and table_name = 'pt2';

-- 添加新分区 报错
ALTER TABLE pt2 ADD PARTITION (PARTITION p4 VALUES LESS THAN (2021));
--ERROR 1493 (HY000): VALUES LESS THAN value must be strictly increasing for each partition

select table_schema, table_name, partition_name,partition_ordinal_position,partition_method,partition_expression
from information_schema.PARTITIONS
where table_schema = 'db1' and table_name = 'pt2';

-------------------------------------------------LIST分区--------------------------------------------------
drop table if exists client_firms;
CREATE TABLE client_firms (
     id   INT,
     name VARCHAR(35)
) PARTITION BY LIST (id) (
	PARTITION r0 VALUES IN (1, 5, 9, 13),
	PARTITION r1 VALUES IN (2, 6, 10, 14),
	PARTITION r2 VALUES IN (3, 7, 11, 15),
	PARTITION r3 VALUES IN (4, 8, 12, 16)
);

select table_schema, table_name, partition_name,partition_ordinal_position,partition_method,partition_expression
from information_schema.PARTITIONS
where table_schema = 'db1' and table_name = 'client_firms';

INSERT INTO client_firms VALUES
(1, 'LV'),
(2, 'oracle'),
(3, 'mysql'),
(4, 'matrixone'),
(5, 'Mercedes Benz'),
(6, 'BMW'),
(7, 'tesla'),
(8, 'spacex'),
(9, 'apple'),
(10, 'openAI'),
(11, 'IBM'),
(12, 'Microsoft'),
(13, 'ZARA'),
(14, 'Apache'),
(15, 'Dell'),
(16, 'HP');

select * from client_firms order by id;

-- 添加新分区
ALTER TABLE client_firms ADD PARTITION (PARTITION r4 VALUES IN (17, 18, 19));
INSERT INTO client_firms VALUES
 (17, 'BOSE'),
 (18, 'Samsung'),
 (19, 'ASML');

select * from client_firms order by id;
------------------------------------------------------
drop table if exists pt3;
CREATE TABLE pt3 (
     id INT,
     category VARCHAR(50),
     value INT
) PARTITION BY LIST COLUMNS(category) (
  PARTITION p1 VALUES IN ('A', 'B') COMMENT 'Category A and B',
  PARTITION p2 VALUES IN ('C', 'D') COMMENT 'Category C and D',
  PARTITION p3 VALUES IN ('E', 'F') COMMENT 'Category E and F'
);

select table_schema, table_name, partition_name,partition_ordinal_position,partition_method,partition_expression
from information_schema.PARTITIONS
where table_schema = 'db1' and table_name = 'pt3';

-- 添加新分区
ALTER TABLE pt3 ADD PARTITION (PARTITION p4 VALUES IN ('G', 'H') COMMENT 'Category G and H');
select table_schema, table_name, partition_name,partition_ordinal_position,partition_method,partition_expression
from information_schema.PARTITIONS
where table_schema = 'db1' and table_name = 'pt3';

drop table if exists pt4;
CREATE TABLE pt4 (
                     a INT,
                     b INT,
                     c date,
                     d decimal(7,2),
                     PRIMARY KEY(a, b)
) PARTITION BY LIST COLUMNS(a,b) (
PARTITION p0 VALUES IN( (0,0), (0,1), (0,2) ),
PARTITION p1 VALUES IN( (0,3), (1,0), (1,1) ),
PARTITION p2 VALUES IN( (1,2), (2,0), (2,1) ),
PARTITION p3 VALUES IN( (1,3), (2,2), (2,3) )
);

select table_schema, table_name, partition_name,partition_ordinal_position,partition_method,partition_expression
from information_schema.PARTITIONS
where table_schema = 'db1' and table_name = 'pt4';

insert into pt4 values(0, 2, '1980-12-17', 9000);
insert into pt4 values(1, 2, '1983-11-11', 800);
insert into pt4 values(2, 1, '1997-04-20', 1600);
insert into pt4 values(1, 1, '1991-02-02', 3400);
insert into pt4 values(1, 3, '1992-07-26', 5600);
insert into pt4 values(2, 0, '1993-02-12', 500);

select * from pt4 order by a, b;

ALTER TABLE pt4 ADD PARTITION (
PARTITION r4 VALUES IN ((3,0), (3,1)),
PARTITION r5 VALUES IN ((3,2), (3,3))
);

select table_schema, table_name, partition_name,partition_ordinal_position,partition_method,partition_expression
from information_schema.PARTITIONS
where table_schema = 'db1' and table_name = 'pt4';

insert into pt4 values(3, 0, '1991-02-02', 2400);
insert into pt4 values(3, 1, '2001-12-02', 3800);
insert into pt4 values(3, 2, '1013-01-30', 3400);
insert into pt4 values(3, 3, '1991-02-22', 1900);

select * from pt4 order by a, b;
-----------------------------------------KEY / hash分区容错测试-----------------------------------------------
drop table if exists pt4;
CREATE TABLE pt4 (
      col1 INT NOT NULL,
      col2 DATE NOT NULL,
      col3 INT PRIMARY KEY
) PARTITION BY KEY(col3)
PARTITIONS 4;

ALTER TABLE pt4 ADD PARTITION (PARTITION p5 VALUES IN (15, 17));
--ERROR 1480 (HY000): Only LIST PARTITIONING can use VALUES IN in partition definition

ALTER TABLE pt4 ADD PARTITION (PARTITION p5 VALUES LESS THAN (200));
--ERROR 1480 (HY000): Only RANGE PARTITIONING can use VALUES LESS THAN in partition definition

drop table if exists pt5;
CREATE TABLE pt5 (
      col1 INT,
      col2 CHAR(5),
      col3 DATE
) PARTITION BY LINEAR HASH( YEAR(col3)) PARTITIONS 6;

ALTER TABLE pt5 ADD PARTITION (PARTITION p5 VALUES IN (2016, 2017));
--ERROR 1480 (HY000): Only LIST PARTITIONING can use VALUES IN in partition definition

ALTER TABLE pt5 ADD PARTITION (PARTITION p5 VALUES LESS THAN (2020));
--ERROR 1480 (HY000): Only RANGE PARTITIONING can use VALUES LESS THAN in partition definition

drop database db1;