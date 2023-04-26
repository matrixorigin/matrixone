drop table if exists t1;
CREATE TABLE t1 (
col1 INT NOT NULL,
col2 DATE NOT NULL,
col3 INT NOT NULL UNIQUE
)
PARTITION BY KEY(col3)
PARTITIONS 4;
insert into t1 values
(1, '1980-12-17', 7369),
(2, '1981-02-20', 7499),
(3, '1981-02-22', 7521),
(4, '1981-04-02', 7566),
(5, '1981-09-28', 7654),
(6, '1981-05-01', 7698),
(7, '1981-06-09', 7782),
(8, '0087-07-13', 7788),
(9, '1981-11-17', 7839),
(10, '1981-09-08', 7844),
(11, '2007-07-13', 7876),
(12, '1981-12-03', 7900),
(13, '1987-07-13', 7980),
(14, '2001-11-17', 7981),
(15, '1951-11-08', 7982),
(16, '1927-10-13', 7983),
(17, '1671-12-09', 7984),
(18, '1981-11-06', 7985),
(19, '1771-12-06', 7986),
(20, '1985-10-06', 7987),
(21, '1771-10-06', 7988),
(22, '1981-10-05', 7989),
(23, '2001-12-04', 7990),
(24, '1999-08-01', 7991),
(25, '1951-11-08', 7992),
(26, '1927-10-13', 7993),
(27, '1971-12-09', 7994),
(28, '1981-12-09', 7995),
(29, '2001-11-17', 7996),
(30, '1981-12-09', 7997),
(31, '2001-11-17', 7998),
(32, '2001-11-17', 7999);
select * from `%!%p0%!%t1` order by col1;
select * from `%!%p1%!%t1` order by col1;
select * from `%!%p2%!%t1` order by col1;
select * from `%!%p3%!%t1` order by col1;
select * from t1 where col1 > 5 order by col3;
delete from t1 where col1 > 20;
select * from t1 order by col1;
select * from `%!%p0%!%t1` order by col1;
select * from `%!%p1%!%t1` order by col1;
select * from `%!%p2%!%t1` order by col1;
select * from `%!%p3%!%t1` order by col1;
drop table t1;


drop table if exists t2;
CREATE TABLE t2 (
col1 INT NOT NULL,
col2 INT NOT NULL,
col3 DATE NOT NULL,
UNIQUE KEY (col1, col3)
)
PARTITION BY HASH(col1 + col3)
PARTITIONS 6;

insert into t2 values
(1, 7369, '1980-12-17'),
(2, 7499, '1981-02-20'),
(3, 7521, '1981-02-22'),
(4, 7566, '1981-04-02'),
(5, 7654, '1981-09-28'),
(6, 7698, '1981-05-01'),
(7, 7782, '1981-06-09'),
(8, 7788, '0087-07-13'),
(9, 7839, '1981-11-17'),
(10, 7844, '1981-09-08'),
(11, 7876, '2007-07-13'),
(12, 7900, '1981-12-03'),
(13, 7980, '1987-07-13'),
(14, 7981, '2001-11-17'),
(15, 7982, '1951-11-08'),
(16, 7983, '1927-10-13'),
(17, 7984, '1671-12-09'),
(18, 7985, '1981-11-06'),
(19, 7986, '1771-12-06'),
(20, 7987, '1985-10-06'),
(21, 7988, '1771-10-06'),
(22, 7989, '1981-10-05'),
(23, 7990, '2001-12-04'),
(24, 7991, '1999-08-01'),
(25, 7992, '1951-11-08'),
(26, 7993, '1927-10-13'),
(27, 7994, '1971-12-09'),
(28, 7995, '1981-12-09'),
(29, 7996, '2001-11-17'),
(30, 7997, '1981-12-09'),
(31, 7998, '2001-11-17'),
(32, 7999, '2001-11-17'),
(33, 7980, '1987-07-13'),
(34, 7981, '2001-11-17'),
(35, 7982, '1951-11-08'),
(36, 7983, '1927-10-13'),
(37, 7984, '1671-12-09'),
(38, 7985, '1981-11-06'),
(39, 7986, '1771-12-06'),
(40, 7987, '1985-10-06');

SELECT * from t2 WHERE col1 BETWEEN 0 and 25 order by col1;
SELECT * from t2 WHERE col1 BETWEEN 26 and 30 order by col1;
select * from `%!%p0%!%t2` order by col1;
select * from `%!%p1%!%t2` order by col1;
select * from `%!%p2%!%t2` order by col1;
select * from `%!%p3%!%t2` order by col1;

delete from t2 where col1 > 30;
select * from t2 order by col1;
select * from `%!%p0%!%t2` order by col1;
select * from `%!%p1%!%t2` order by col1;
select * from `%!%p2%!%t2` order by col1;
select * from `%!%p3%!%t2` order by col1;
drop table t2;

drop table if exists t3;
CREATE TABLE t3 (
id int NOT NULL AUTO_INCREMENT,
key_num int NOT NULL DEFAULT '0',
hiredate date NOT NULL,
PRIMARY KEY (id),
KEY key_num (key_num)
)
PARTITION BY RANGE COLUMNS(id) (
PARTITION p0 VALUES LESS THAN(10),
PARTITION p1 VALUES LESS THAN(20),
PARTITION p2 VALUES LESS THAN(30),
PARTITION p3 VALUES LESS THAN(MAXVALUE)
);

insert into t3 values
(1, 7369, '1980-12-17'),
(2, 7499, '1981-02-20'),
(3, 7521, '1981-02-22'),
(4, 7566, '1981-04-02'),
(5, 7654, '1981-09-28'),
(6, 7698, '1981-05-01'),
(7, 7782, '1981-06-09'),
(8, 7788, '0087-07-13'),
(9, 7839, '1981-11-17'),
(10, 7844, '1981-09-08'),
(11, 7876, '2007-07-13'),
(12, 7900, '1981-12-03'),
(13, 7980, '1987-07-13'),
(14, 7981, '2001-11-17'),
(15, 7982, '1951-11-08'),
(16, 7983, '1927-10-13'),
(17, 7984, '1671-12-09'),
(18, 7985, '1981-11-06'),
(19, 7986, '1771-12-06'),
(20, 7987, '1985-10-06'),
(21, 7988, '1771-10-06'),
(22, 7989, '1981-10-05'),
(23, 7990, '2001-12-04'),
(24, 7991, '1999-08-01'),
(25, 7992, '1951-11-08'),
(26, 7993, '1927-10-13'),
(27, 7994, '1971-12-09'),
(28, 7995, '1981-12-09'),
(29, 7996, '2001-11-17'),
(30, 7997, '1981-12-09'),
(31, 7998, '2001-11-17'),
(32, 7999, '2001-11-17'),
(33, 7980, '1987-07-13'),
(34, 7981, '2001-11-17'),
(35, 7982, '1951-11-08'),
(36, 7983, '1927-10-13'),
(37, 7984, '1671-12-09'),
(38, 7985, '1981-11-06'),
(39, 7986, '1771-12-06'),
(40, 7987, '1985-10-06');

SELECT * from t3 WHERE id BETWEEN 0 and 9 order by id;
SELECT * from t3 WHERE id BETWEEN 20 and 29 order by id;
select * from `%!%p0%!%t3` order by id;
select * from `%!%p1%!%t3` order by id;
select * from `%!%p2%!%t3` order by id;
select * from `%!%p3%!%t3` order by id;

delete from t3 where id > 30;
select * from t3 order by id;
select * from `%!%p0%!%t3` order by id;
select * from `%!%p1%!%t3` order by id;
select * from `%!%p2%!%t3` order by id;
select * from `%!%p3%!%t3` order by id;
drop table t3;

drop table if exists titles;

CREATE TABLE titles (
emp_no      INT             NOT NULL,
title       VARCHAR(50)     NOT NULL,
from_date   DATE            NOT NULL,
to_date     DATE,
PRIMARY KEY (emp_no, title, from_date)
)
partition by range (to_days(from_date))
(
    partition p01 values less than (to_days('1985-12-31')),
    partition p02 values less than (to_days('1986-12-31')),
    partition p03 values less than (to_days('1987-12-31')),
    partition p04 values less than (to_days('1988-12-31')),
    partition p05 values less than (to_days('1989-12-31')),
    partition p06 values less than (to_days('1990-12-31')),
    partition p07 values less than (to_days('1991-12-31')),
    partition p08 values less than (to_days('1992-12-31')),
    partition p09 values less than (to_days('1993-12-31')),
    partition p10 values less than (to_days('1994-12-31')),
    partition p11 values less than (to_days('1995-12-31')),
    partition p12 values less than (to_days('1996-12-31')),
    partition p13 values less than (to_days('1997-12-31')),
    partition p14 values less than (to_days('1998-12-31')),
    partition p15 values less than (to_days('1999-12-31')),
    partition p16 values less than (to_days('2000-12-31')),
    partition p17 values less than (to_days('2001-12-31')),
    partition p18 values less than (to_days('2002-12-31')),
    partition p19 values less than (to_days('3000-12-31'))
);

INSERT INTO `titles` VALUES
(10001,'Senior Engineer','1986-06-26','9999-01-01'),
(10002,'Staff','1996-08-03','9999-01-01'),
(10003,'Senior Engineer','1995-12-03','9999-01-01'),
(10004,'Engineer','1986-12-01','1995-12-01'),
(10004,'Senior Engineer','1995-12-01','9999-01-01'),
(10005,'Senior Staff','1996-09-12','9999-01-01'),
(10005,'Staff','1989-09-12','1996-09-12'),
(10006,'Senior Engineer','1990-08-05','9999-01-01'),
(10007,'Senior Staff','1996-02-11','9999-01-01'),
(10007,'Staff','1989-02-10','1996-02-11'),
(10008,'Assistant Engineer','1998-03-11','2000-07-31'),
(10009,'Assistant Engineer','1985-02-18','1990-02-18'),
(10009,'Engineer','1990-02-18','1995-02-18'),
(10009,'Senior Engineer','1995-02-18','9999-01-01'),
(10010,'Engineer','1996-11-24','9999-01-01'),
(10011,'Staff','1990-01-22','1996-11-09'),
(10012,'Engineer','1992-12-18','2000-12-18'),
(10012,'Senior Engineer','2000-12-18','9999-01-01'),
(10013,'Senior Staff','1985-10-20','9999-01-01'),
(10014,'Engineer','1993-12-29','9999-01-01'),
(10015,'Senior Staff','1992-09-19','1993-08-22'),
(10016,'Staff','1998-02-11','9999-01-01'),
(10017,'Senior Staff','2000-08-03','9999-01-01'),
(10017,'Staff','1993-08-03','2000-08-03'),
(10018,'Engineer','1987-04-03','1995-04-03'),
(10018,'Senior Engineer','1995-04-03','9999-01-01'),
(10019,'Staff','1999-04-30','9999-01-01'),
(10020,'Engineer','1997-12-30','9999-01-01'),
(10021,'Technique Leader','1988-02-10','2002-07-15'),
(10022,'Engineer','1999-09-03','9999-01-01'),
(10023,'Engineer','1999-09-27','9999-01-01'),
(10024,'Assistant Engineer','1998-06-14','9999-01-01'),
(10025,'Technique Leader','1987-08-17','1997-10-15'),
(10026,'Engineer','1995-03-20','2001-03-19'),
(10026,'Senior Engineer','2001-03-19','9999-01-01'),
(10027,'Engineer','1995-04-02','2001-04-01'),
(10027,'Senior Engineer','2001-04-01','9999-01-01'),
(10028,'Engineer','1991-10-22','1998-04-06'),
(10029,'Engineer','1991-09-18','2000-09-17'),
(10029,'Senior Engineer','2000-09-17','9999-01-01'),
(10030,'Engineer','1994-02-17','2001-02-17'),
(10030,'Senior Engineer','2001-02-17','9999-01-01');

select * from titles order by emp_no;

select * from `%!%p01%!%titles` order by emp_no;
select * from `%!%p02%!%titles` order by emp_no;
select * from `%!%p03%!%titles` order by emp_no;
select * from `%!%p04%!%titles` order by emp_no;
select * from `%!%p05%!%titles` order by emp_no;
select * from `%!%p06%!%titles` order by emp_no;
select * from `%!%p07%!%titles` order by emp_no;
select * from `%!%p08%!%titles` order by emp_no;
select * from `%!%p09%!%titles` order by emp_no;
select * from `%!%p10%!%titles` order by emp_no;
select * from `%!%p11%!%titles` order by emp_no;
select * from `%!%p12%!%titles` order by emp_no;
select * from `%!%p13%!%titles` order by emp_no;
select * from `%!%p14%!%titles` order by emp_no;
select * from `%!%p15%!%titles` order by emp_no;
select * from `%!%p16%!%titles` order by emp_no;
select * from `%!%p17%!%titles` order by emp_no;
select * from `%!%p18%!%titles` order by emp_no;
select * from `%!%p19%!%titles` order by emp_no;

select to_days(from_date) from titles order by emp_no;

select * from titles where to_days(from_date) between to_days('1988-12-31') and to_days('1992-12-31');
delete from titles where to_days(from_date) between to_days('1988-12-31') and to_days('1992-12-31');
select * from titles order by emp_no;

drop table titles;