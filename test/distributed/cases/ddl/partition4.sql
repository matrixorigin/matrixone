drop table if exists t1;
CREATE TABLE t1 (
col1 INT NOT NULL AUTO_INCREMENT,
col2 DATE NOT NULL,
col3 INT PRIMARY KEY
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

select * from t1 order by col1;

truncate table t1;
select * from t1 order by col1;
select * from `%!%p0%!%t1` order by col1;
select * from `%!%p1%!%t1` order by col1;
select * from `%!%p2%!%t1` order by col1;
select * from `%!%p3%!%t1` order by col1;

drop table t1;
select * from t1;
select * from `%!%p0%!%t1`;
select * from `%!%p1%!%t1`;
select * from `%!%p2%!%t1`;
select * from `%!%p3%!%t1`;

-- Partition table fault tolerance test
create table insert_ignore_06 (
    sale_id INT AUTO_INCREMENT,
    product_id INT,
    sale_amount DECIMAL(10, 2),
    sale_date DATE,
    PRIMARY KEY (sale_id, sale_date)
) PARTITION BY RANGE (year(sale_date)) (
PARTITION p0 VALUES LESS THAN (1991),
PARTITION p1 VALUES LESS THAN (1992),
PARTITION p2 VALUES LESS THAN (1993),
PARTITION p3 VALUES LESS THAN (1994));

-- should report error
insert into insert_ignore_06 (product_id, sale_amount, sale_date) VALUES
(1, 1000.00, '1990-04-01'),
(2, 1500.00, '1992-05-01'),
(3, 500.00, '1995-06-01'),
(1, 2000.00, '1991-07-01');

select * from insert_ignore_06 order by sale_id;
-- should success
insert into insert_ignore_06 (product_id, sale_amount, sale_date) VALUES
(1, 1000.00, '1990-04-01'),
(2, 1500.00, '1992-05-01'),
(1, 2000.00, '1991-07-01');
select * from insert_ignore_06 order by sale_id;
drop table insert_ignore_06;