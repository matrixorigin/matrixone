drop database if exists test;
create database test;
use test;

create table t1(a int, b int, c int, d int, e int, index(b), index compIdx(c, d), unique index(e));

-- test 1: add normal col
insert into t1 select *,*,*,*,* from generate_series(1, 100*100*10)g;
delete from t1 where a in (1, 1111, 11111);
select * from t1 where a in (1, 1111, 11111);

select count(*) from t1;
alter table t1 add column f int;
select count(*) from t1;

delete from t1 where a in (2, 2222, 22222);
select * from t1 where a in (2, 2222, 22222);

-- index table scan
select a from t1 where b = 9;
select a from t1 where c = 9000 and d = 9000;
select a from t1 where e = 27000;


-- test2: drop normal column;
alter table t1 drop column f;
-- index table scan
select a from t1 where b = 9;
select a from t1 where c = 9000 and d = 9000;
select a from t1 where e = 27000;


-- test3: modify normal column to pk
alter table t1 modify column a int primary key;
-- index table scan
select a from t1 where b = 9;
select a from t1 where c = 9000 and d = 9000;
select a from t1 where e = 27000;

-- test4: modify pk column to normal column
alter table t1 drop primary key;
-- index table scan
select a from t1 where b = 9;
select a from t1 where c = 9000 and d = 9000;
select a from t1 where e = 27000;

-- test5: add primary key column
alter table t1 add primary key(a);
-- index table scan
select a from t1 where b = 9;
select a from t1 where c = 9000 and d = 9000;
select a from t1 where e = 27000;
alter table t1 drop primary key;


-- test6: modify index column type
alter table t1 modify b double;
-- index table scan
select a from t1 where b = 9;
select a from t1 where c = 9000 and d = 9000;
select a from t1 where e = 27000;

-- test7: rename index column
alter table t1 rename column b to b_2;
-- index table scan
select a from t1 where b_2 = 9;
select a from t1 where c = 9000 and d = 9000;
select a from t1 where e = 27000;

-- index scan
delete from t1 where a mod 19 = 0;
select * from t1 where b_2 in (1, 10, 19, 38, 100);
select * from t1 where c  in (1, 10, 19, 38, 100) and d in (1, 10, 19, 38, 100);
select * from t1 where e in (1, 10, 19, 38, 100);

update t1 set a = a + 1 where c in (8,88,888,8888,88888);
select * from t1 where c in (8,88,888,8888,88888);
alter table t1 add column f int;
update t1 set a = a + 1 where c in (8,88,888,8888,88888);
select * from t1 where c in (8,88,888,8888,88888);

set experimental_fulltext_index=1;
set experimental_ivf_index = 1;
set experimental_hnsw_index = 1;
create table t2 (a bigint primary key auto_increment, b vecf32(3), c vecf32(3), d text, index ivfIdx using ivfflat(b) lists=5 op_type "vector_l2_ops", index hnswIdx using hnsw(c) op_type 'vector_l2_ops', fulltext(d));
insert into t2(b,c,d) select CONCAT('[',FLOOR(RAND() * 9),',', FLOOR(RAND() * 9), ',', FLOOR(RAND() * 9),']'), CONCAT('[',FLOOR(RAND() * 9),',', FLOOR(RAND() * 9), ',', FLOOR(RAND() * 9),']'), * from generate_series(1, 1000)g;
insert into t2(b,c,d) values("[1,1,1]","[1,1,1]","111"),("[9,9,9]","[9,9,9]","999");

alter table t2 add column f int;
select floor(max(l2_distance(b, "[1,1,1]"))) from t2;
select floor(max(l2_distance(c, "[1,1,1]"))) from t2;
select a,d from t2 where match(d) against('234' in boolean mode);

set experimental_fulltext_index=0;
set experimental_ivf_index = 0;
set experimental_hnsw_index = 0;

drop database test;