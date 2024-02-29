/* data prepare for sample from single column */
drop table if exists s_t1;
create table s_t1 (c1 int, c2 int, c3 int);
insert into s_t1 values (1, 1, 0), (1, 1, 1), (1, 1, 2), (1, 1, null);
insert into s_t1 values (2, 1, 0);
insert into s_t1 values (2, 2, 0), (2, 2, null);
insert into s_t1 values (3, 1, null), (3, 2, null), (3, 2, null);

/* expected failed case */
/* 1. only support one sample function in a sql */
select c1, sample(c2, 1 rows), sample(c3, 1 rows) from s_t1;
/* 2. cannot fixed sample function (non-scalar function) and aggregate function (scalar function) */
select c1, max(c2), sample(c3, 1 rows) from s_t1;
/* 3. sample(column list, N rows) requires 1 <= N <= 11000 */
select sample(c1, 0 rows) from s_t1;
select sample(c1, 11001 rows) from s_t1;
/* 4. sample(column list, K percent) requires 0.00 <= K <= 100.00, should watch that 99.994 was accepted but will treat as 99.99 */
select sample(c1, 101 percent) from s_t1;
select sample(c1, 100.01 percent) from s_t1;
/* 5. sample cannot be used in where clause */
select c1, c2, c3 from s_t1 where sample(c1, 1 rows) = 1;
select c1, c2, c3 from s_t1 where sample(c1, 1 percent) = 1;
/* 6. cannot sample the group by column */
select sample(c1, 1 rows) from s_t1 group by c1;
select sample(*, 1 rows) from s_t1 group by c1;

/* expected succeed case */
/* 1. sample 2 rows from table by column c1 */
select count(*) from (select sample(c1, 2 rows) from s_t1);
/* 2. sample 2 rows from table by column c2 */
select count(*) from (select sample(c2, 2 rows) from s_t1);
/* 3. sample 2 rows from table by column c2 group by col1 */
select count(*) from (select sample(c2, 2 rows) from s_t1 group by c1);
/* 4. sample 3 rows from table by column c3 where c1 = 2, expected to get only 2 rows because one row's c3 is null */
select sample(c3, 3 rows) from s_t1 where c1 = 2;
/* 5. sample 3 rows from table by column c3 where c1 = 1, expected to get 3 rows (0),(1),(2) */
select c1, c2, sample(c3, 3 rows) from s_t1 where c1 = 1 order by c3;
/* 6. sample 100 percent from table by column c3, expected to get all rows except null rows */
select sample(c3, 100 percent) from s_t1 order by c3;
/* 7. sample 0 percent from table by column c1, expected to get empty */
select sample(c1, 0 percent) from s_t1;
/* 8. some case I don't know how to describe it, in short, these cases should be OK */
select count(*) from (select c1, c2, sample(c2, 100 percent), c3 from s_t1);
select count(*) from (select c1, sample(c2, 100 percent), c2, c3 from s_t1);
/* 9. with limit */
select count(*) from (select sample(c2, 2 rows) from s_t1 limit 1);
select count(*) from (select sample(c2, 2 rows) from s_t1 group by c1 limit 2);
/* 10. with alias */
select sample(c3, 3 rows) as k from s_t1 where c1 = 2;
/* 11. sample from all invalid rows, should get only one invalid row */
select sample(c3, 1 rows) from s_t1 where c1 = 3;
select sample(c3, 2 rows) from s_t1 where c1 = 3;
select c1, sample(c3, 3 rows) from s_t1 where c1 = 3 group by c1;
select c1, sample(c3, 3 rows) from s_t1 group by c1 order by c1, c3;
/* 12. sample as and outer filter */
select * from (select c1, sample(c3, 3 rows) as k from s_t1 group by c1) where k < 2 order by c1, k;

/* data prepare for sample from multi columns */
drop table if exists s_t2;
create table s_t2 (cc1 int, cc2 int);
insert into s_t2 values (1, 1), (null, 2), (3, null), (null, null);

/* expected failed case */
/* 1. multi sample with alias */
select sample(cc1, cc2, 1 rows) as k from s_t2;

/* expected succeed case */
/* 1. sample 2 rows from table by column cc1, cc2, expected to get 3 rows because we should sample 2 not-null value for each column */
select count(*) from (select sample(cc1, cc2, 2 rows) from s_t2);
/* 2. sample 100 percent from table by column cc1, cc2, expected to get all rows except (null, null) */
select sample(cc1, cc2, 100 percent) from s_t2 order by cc1 asc;
/* 3. sample 0 percent from table by column cc1, cc2, expected to get empty */
select sample(cc1, cc2, 0 percent) from s_t2;
/* 4. should support the sample * from table */
select sample(*, 100 percent) from s_t2 order by cc1 asc;
select sample(*, 0 percent) from s_t2;
select sample(*, 2 rows) from s_t2 order by cc1 asc;

/* data prepare for expression sample */
drop table if exists s_t3;
create table s_t3 (c1 int, c2 int);
insert into s_t3 values (1, 3), (2, 5), (3, 6), (4, 7), (5, 8);

/* expected failed case */
/* 1. sample(aggregate function, N rows) should failed */
select sample(max(c1), 1 rows) from s_t3;

/* expected succeed case */
/* 1. expression (not only simple column) should be OK */
select c1, sample(c1+1, 100 percent) from s_t3 order by c1;

-- test `sample(expression, n rows, unit)` syntax.
-- it's same as sample(n rows) but will avoid centroids skewed.
select count(*) from (select sample(c1, 2 rows, 'row') from s_t3);
select count(*) from (select sample(c1, 2 rows, 'block') from s_t3);
select c1, sample(c2, 1 rows, 'row') from s_t3 group by c1 order by c1;