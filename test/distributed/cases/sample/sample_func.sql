/* data prepare for sample from single column */
drop table if exists s_t1;
create table s_t1 (c1 int, c2 int, c3 int);
insert into s_t1 values (1, 1, 0), (1, 1, 1), (1, 1, 2), (1, 1, null);
insert into s_t1 values (2, 1, 0);
insert into s_t1 values (2, 2, 0), (2, 2, null);
insert into s_t1 values (3, 1, null), (3, 2, null), (3, 2, null);

/* expected error case */
/* 1. only support one sample function in a sql */
select c1, sample(c2, 1 rows), sample(c3, 1 rows) from s_t1;
/* 2. cannot fixed sample function (non-scalar function) and aggregate function (scalar function) */
select c1, max(c2), sample(c3, 1 rows) from s_t1;
/* 3. sample(column list, N rows) requires 1 <= N <= 1000 */
select sample(c1, 0 rows) from s_t1;
select sample(c1, 1001 rows) from s_t1;
/* 4. sample(column list, K percent) requires 0.00 <= K <= 100.00, should watch that 99.994 was accepted but will treat as 99.99 */
select sample(c1, 101 percent) from s_t1;
select sample(c1, 100.01 percent) from s_t1;
/* 5. sample cannot be used in where clause */
select c1, c2, c3 from s_t1 where sample(c1, 1 rows) = 1;
select c1, c2, c3 from s_t1 where sample(c1, 1 percent) = 1;

/* expected success case */
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