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