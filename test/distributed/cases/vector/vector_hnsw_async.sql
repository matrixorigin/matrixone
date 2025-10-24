
SET experimental_hnsw_index = 1;

drop database if exists hnsw_cdc;
create database if not exists hnsw_cdc;
use hnsw_cdc;

create table t1(a bigint primary key, b vecf32(3),c int,key c_k(c));

-- empty data
create index idx01 using hnsw on t1(b) op_type "vector_l2_ops" M 20 EF_CONSTRUCTION 100 EF_SEARCH 100 ASYNC;

-- select sleep(10);

insert into t1 values (0, "[1,2,3]", 1);
-- select hnsw_cdc_update('hnsw_cdc', 't1', 3, '{"start":"", "end":"", "cdc":[{"t":"U", "pk":0, "v":[1,2,3]}]}');

UPDATE t1 set b = '[4,5,6]' where a = 0;
-- select hnsw_cdc_update('hnsw_cdc', 't1', 3, '{"start":"", "end":"", "cdc":[{"t":"U", "pk":0, "v":[4,5,6]}]}');

insert into t1 values (1, "[2,3,4]", 1);
-- select hnsw_cdc_update('hnsw_cdc', 't1', 3, '{"start":"", "end":"", "cdc":[{"t":"I", "pk":1, "v":[2,3,4]}]}');

DELETE FROM t1 WHERE a=1;
-- select hnsw_cdc_update('hnsw_cdc', 't1', 3, '{"start":"", "end":"", "cdc":[{"t":"D", "pk":0}]}');

select sleep(10);

-- test with multi-cn is tricky.  since model is cached in memory, model may not be updated after CDC sync'd.  The only way to test is to all INSERT/DELETE/UPDATE before SELECT.
-- already update to [4,5,6], result is [4,5,6]
select * from t1 order by  L2_DISTANCE(b,"[1,2,3]") ASC LIMIT 10;

-- should return a=0
select * from t1 order by  L2_DISTANCE(b,"[4,5,6]") ASC LIMIT 10;

-- a=1 deleted. result is [4,5,6]
select * from t1 order by  L2_DISTANCE(b,"[2,3,4]") ASC LIMIT 10;

drop table t1;

-- t2
create table t2(a bigint primary key, b vecf32(128));
create index idx2 using hnsw on t2(b) op_type "vector_l2_ops" M 20 EF_CONSTRUCTION 100 EF_SEARCH 100 ASYNC;
-- select sleep(10);

load data infile {'filepath'='$resources/vector/sift128_base_10k.csv.gz', 'compression'='gzip'} into table t2 fields terminated by ':' parallel 'true';

select count(*) from t2;

select sleep(10);

select * from t2 order by L2_DISTANCE(b, "[14, 2, 0, 0, 0, 2, 42, 55, 9, 1, 0, 0, 18, 100, 77, 32, 89, 1, 0, 0, 19, 85, 15, 68, 52, 4, 0, 0, 0, 0, 2, 28, 34, 13, 5, 12, 49, 40, 39, 37, 24, 2, 0, 0, 34, 83, 88, 28, 119, 20, 0, 0, 41, 39, 13, 62, 119, 16, 2, 0, 0, 0, 10, 42, 9, 46, 82, 79, 64, 19, 2, 5, 10, 35, 26, 53, 84, 32, 34, 9, 119, 119, 21, 3, 3, 11, 17, 14, 119, 25, 8, 5, 0, 0, 11, 22, 23, 17, 42, 49, 17, 12, 5, 5, 12, 78, 119, 90, 27, 0, 4, 2, 48, 92, 112, 85, 15, 0, 2, 7, 50, 36, 15, 11, 1, 0, 0, 7]") ASC LIMIT 1;

select * from t2 order by L2_DISTANCE(b, "[0, 16, 35, 5, 32, 31, 14, 10, 11, 78, 55, 10, 45, 83, 11, 6, 14, 57, 102, 75, 20, 8, 3, 5, 67, 17, 19, 26, 5, 0, 1, 22, 60, 26, 7, 1, 18, 22, 84, 53, 85, 119, 119, 4, 24, 18, 7, 7, 1, 81, 106, 102, 72, 30, 6, 0, 9, 1, 9, 119, 72, 1, 4, 33, 119, 29, 6, 1, 0, 1, 14, 52, 119, 30, 3, 0, 0, 55, 92, 111, 2, 5, 4, 9, 22, 89, 96, 14, 1, 0, 1, 82, 59, 16, 20, 5, 25, 14, 11, 4, 0, 0, 1, 26, 47, 23, 4, 0, 0, 4, 38, 83, 30, 14, 9, 4, 9, 17, 23, 41, 0, 0, 2, 8, 19, 25, 23, 1]") ASC LIMIT 1;


-- delete whole table won't work for now.
-- delete from t2
-- select sleep(10)

drop table t2;

-- end t2

-- t3
create table t3(a bigint primary key, b vecf32(128));

load data infile {'filepath'='$resources/vector/sift128_base_10k.csv.gz', 'compression'='gzip'} into table t3 fields terminated by ':' parallel 'true';

select count(*) from t3;

create index idx3 using hnsw on t3(b) op_type "vector_l2_ops" M 20 EF_CONSTRUCTION 100 EF_SEARCH 100 ASYNC;

select sleep(10);

load data infile {'filepath'='$resources/vector/sift128_base_10k_2.csv.gz', 'compression'='gzip'} into table t3 fields terminated by ':' parallel 'true';

select count(*) from t3;

select sleep(10);

select * from t3 order by L2_DISTANCE(b, "[14, 2, 0, 0, 0, 2, 42, 55, 9, 1, 0, 0, 18, 100, 77, 32, 89, 1, 0, 0, 19, 85, 15, 68, 52, 4, 0, 0, 0, 0, 2, 28, 34, 13, 5, 12, 49, 40, 39, 37, 24, 2, 0, 0, 34, 83, 88, 28, 119, 20, 0, 0, 41, 39, 13, 62, 119, 16, 2, 0, 0, 0, 10, 42, 9, 46, 82, 79, 64, 19, 2, 5, 10, 35, 26, 53, 84, 32, 34, 9, 119, 119, 21, 3, 3, 11, 17, 14, 119, 25, 8, 5, 0, 0, 11, 22, 23, 17, 42, 49, 17, 12, 5, 5, 12, 78, 119, 90, 27, 0, 4, 2, 48, 92, 112, 85, 15, 0, 2, 7, 50, 36, 15, 11, 1, 0, 0, 7]") ASC LIMIT 1;

select * from t3 order by L2_DISTANCE(b, "[0, 16, 35, 5, 32, 31, 14, 10, 11, 78, 55, 10, 45, 83, 11, 6, 14, 57, 102, 75, 20, 8, 3, 5, 67, 17, 19, 26, 5, 0, 1, 22, 60, 26, 7, 1, 18, 22, 84, 53, 85, 119, 119, 4, 24, 18, 7, 7, 1, 81, 106, 102, 72, 30, 6, 0, 9, 1, 9, 119, 72, 1, 4, 33, 119, 29, 6, 1, 0, 1, 14, 52, 119, 30, 3, 0, 0, 55, 92, 111, 2, 5, 4, 9, 22, 89, 96, 14, 1, 0, 1, 82, 59, 16, 20, 5, 25, 14, 11, 4, 0, 0, 1, 26, 47, 23, 4, 0, 0, 4, 38, 83, 30, 14, 9, 4, 9, 17, 23, 41, 0, 0, 2, 8, 19, 25, 23, 1]") ASC LIMIT 1;


select * from t3 order by L2_DISTANCE(b, "[59, 0, 0, 1, 1, 1, 5, 100, 41, 0, 0, 4, 57, 34, 31, 115, 4, 0, 0, 12, 30, 33, 43, 85, 21, 0, 0, 14, 25, 9, 10, 60, 99, 11, 0, 0, 0, 0, 10, 55, 68, 1, 0, 3, 115, 65, 42, 115, 32, 3, 0, 4, 13, 21, 104, 115, 81, 15, 15, 23, 9, 2, 21, 75, 43, 20, 1, 0, 10, 2, 2, 20, 52, 35, 32, 61, 79, 8, 7, 41, 50, 106, 96, 20, 8, 2, 11, 39, 115, 48, 53, 11, 3, 0, 2, 43, 35, 11, 0, 1, 13, 7, 0, 1, 115, 58, 54, 29, 1, 2, 0, 3, 32, 115, 99, 34, 1, 0, 0, 0, 35, 15, 52, 44, 9, 0, 0, 18]") ASC LIMIT 1;

select * from t3 order by L2_DISTANCE(b, "[0, 0, 0, 0, 0, 101, 82, 4, 2, 0, 0, 0, 3, 133, 133, 8, 46, 1, 2, 13, 15, 29, 87, 50, 22, 1, 0, 16, 25, 6, 18, 49, 5, 2, 0, 2, 3, 59, 70, 19, 18, 2, 0, 11, 42, 37, 30, 13, 133, 13, 4, 53, 28, 3, 8, 42, 77, 6, 11, 103, 36, 0, 0, 32, 7, 15, 59, 27, 2, 0, 2, 5, 14, 5, 55, 52, 51, 3, 2, 5, 133, 21, 10, 38, 26, 1, 0, 64, 71, 3, 10, 118, 53, 5, 6, 28, 33, 26, 73, 15, 0, 0, 0, 22, 13, 15, 133, 133, 4, 0, 0, 15, 107, 62, 46, 91, 9, 1, 7, 16, 28, 4, 0, 27, 33, 4, 15, 25]") ASC LIMIT 1;

drop table t3;

-- end t3

drop database hnsw_cdc;

