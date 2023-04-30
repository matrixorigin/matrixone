drop table if exists t1;
CREATE TABLE t1 (
col1 INT NOT NULL,
col2 DATE NOT NULL,
col3 INT NOT NULL UNIQUE
)
PARTITION BY KEY(col3)
PARTITIONS 4;
-- @bvt:issue#9102
insert into `%!%p0%!%t1` values (1, '1980-12-17', 7369),(2, '1981-02-20', 7499),(3, '1981-02-22', 7521),(4, '1981-04-02', 7566),(5, '1981-09-28', 7654);
insert into `%!%p1%!%t1` values (6, '1981-05-01', 7698),(7, '1981-06-09', 7782),(8, '0087-07-13', 7788),(9, '1981-11-17', 7839);
insert into `%!%p2%!%t1` values (10, '1981-09-08', 7844),(11, '2007-07-13', 7876),(12, '1981-12-03', 7900),(13, '1987-07-13', 7980);
insert into `%!%p3%!%t1` values (14, '2001-11-17', 7981),(15, '1951-11-08', 7982),(16, '1927-10-13', 7983),(17, '1971-12-09', 7984),(18, '1981-12-09', 7985);
select * from t1 where col1 > 5 order by col3;
-- @bvt:issue
drop table t1;