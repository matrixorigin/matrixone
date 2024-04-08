-- pre
drop database if exists vecdb3;
create database vecdb3;
use vecdb3;
SET GLOBAL experimental_ivf_index = 1;


-- 0.a  Verify Experimental Flag
drop table if exists t6;
create table t6(a int primary key,b vecf32(4), c varchar(3) );
insert into t6 values(1, "[1,0,0,0]" , "1");
insert into t6 values(2, "[2,0,0,0]", "2");
insert into t6 values(3, "[3,0,0,0]", "3");
insert into t6 values(4, "[1,1,0,0]", "4");
insert into t6 values(5, "[2,2,0,0]", "5");
insert into t6 values(6, "[3,3,0,0]", "6");
SET GLOBAL experimental_ivf_index = 0;
create index idx6 using ivfflat on t6(b) lists=2 op_type "vector_l2_ops";
SET GLOBAL experimental_ivf_index = 1;
create index idx6 using ivfflat on t6(b) lists=2 op_type "vector_l2_ops";
select a, b from t6 order by l2_distance(b, "[1,0,0,0]") limit 4;

-- 1.a KNN with pk and embedding alone
drop table if exists t1;
create table t1(a int primary key,b vecf32(4), c varchar(3) );
insert into t1 values(1, "[1,0,0,0]" , "1");
insert into t1 values(2, "[2,0,0,0]", "2");
insert into t1 values(3, "[3,0,0,0]", "3");
insert into t1 values(4, "[1,1,0,0]", "4");
insert into t1 values(5, "[2,2,0,0]", "5");
insert into t1 values(6, "[3,3,0,0]", "6");
insert into t1 values(7, "[1,1,1,0]", "7");
insert into t1 values(8, "[2,2,2,0]", "8");
insert into t1 values(9, "[3,3,3,0]", "9");
select a, b from t1 order by l2_distance(normalize_l2(b), normalize_l2("[1,1,1,0]")) limit 3;

create index idx1 using ivfflat on t1(b) lists=3 op_type "vector_l2_ops";
select a, b from t1 order by l2_distance(b, "[1,1,1,0]") limit 4;

insert into t1 values(10, "[4,4,4,0]", "10");
select a, b from t1 order by l2_distance(b, "[1,1,1,0]") limit 4;

-- 1.b KNN with pk and embedding and non-indexed columns
select a,b,c from t1 order by l2_distance(b, "[1,1,1,0]") limit 4;


insert into t1 values(11, "[1,1,1,1]", "11");
insert into t1 values(12, "[2,2,2,2]", "12");
insert into t1 values(13, "[3,3,3,3]", "13");

alter table t1 alter reindex idx1 ivfflat lists=4;
select a, b from t1 order by l2_distance(b, "[1,0,0,0]") limit 4;
select a, b from t1 order by l2_distance(b, "[1,1,0,0]") limit 4;
select a, b from t1 order by l2_distance(b, "[1,1,1,0]") limit 4;
select a, b from t1 order by l2_distance(b, "[1,1,1,1]") limit 4;


-- 2.a KNN with embedding alone (composite PK)
drop table if exists t3;
create table t2(a int,b vecf32(4), c varchar(3), d int, primary key(a,d));
insert into t2 values(1, "[1,0,0,0]" , "1", 1);
insert into t2 values(2, "[2,0,0,0]", "2", 2);
insert into t2 values(3, "[3,0,0,0]", "3", 3);
insert into t2 values(4, "[1,1,0,0]", "4", 4);
insert into t2 values(5, "[2,2,0,0]", "5", 5);
insert into t2 values(6, "[3,3,0,0]", "6", 6);
insert into t2 values(7, "[1,1,1,0]", "7", 7);
insert into t2 values(8, "[2,2,2,0]", "8", 8);
insert into t2 values(9, "[3,3,3,0]", "9", 9);
select a, b from t2 order by l2_distance(normalize_l2(b), normalize_l2("[1,1,1,0]")) limit 3;

create index idx2 using ivfflat on t2(b) lists=3 op_type "vector_l2_ops";
select b from t2 order by l2_distance(b, "[1,1,1,0]") limit 4; -- speed path (should output only 3 rows)
select a, b from t2 order by l2_distance(b, "[1,1,1,0]") limit 4; -- slow path

insert into t2 values(10, "[4,4,4,0]", "10", 10);
select b from t2 order by l2_distance(b, "[1,1,1,0]") limit 4; -- speed path
select a, b from t2 order by l2_distance(b, "[1,1,1,0]") limit 4; -- slow path

-- 2.b KNN with embedding and non-indexed columns (composite PK)
select a,b,c from t2 order by l2_distance(b, "[1,1,1,0]") limit 4;

insert into t2 values(11, "[1,1,1,1]", "11", 11);
insert into t2 values(12, "[2,2,2,2]", "12", 12);
insert into t2 values(13, "[3,3,3,3]", "13", 13);

alter table t2 alter reindex idx2 ivfflat lists=4;
select a, b from t2 order by l2_distance(b, "[1,0,0,0]") limit 4;
select a, b from t2 order by l2_distance(b, "[1,1,0,0]") limit 4;
select a, b from t2 order by l2_distance(b, "[1,1,1,0]") limit 4;
select a, b from t2 order by l2_distance(b, "[1,1,1,1]") limit 4;

-- 3.a KNN with no PK and embedding alone
drop table if exists t3;
create table t3(a int,b vecf32(4), c varchar(3));
insert into t3 values(1, "[1,0,0,0]" , "1");
insert into t3 values(2, "[2,0,0,0]", "2");
insert into t3 values(3, "[3,0,0,0]", "3");
insert into t3 values(4, "[1,1,0,0]", "4");
insert into t3 values(5, "[2,2,0,0]", "5");
insert into t3 values(6, "[3,3,0,0]", "6");
insert into t3 values(7, "[1,1,1,0]", "7");
insert into t3 values(8, "[2,2,2,0]", "8");
insert into t3 values(9, "[3,3,3,0]", "9");
select a, b from t3 order by l2_distance(normalize_l2(b), normalize_l2("[1,1,1,0]")) limit 3;

create index idx3 using ivfflat on t3(b) lists=3 op_type "vector_l2_ops";
select a, b from t3 order by l2_distance(b, "[1,1,1,0]") limit 4;

insert into t3 values(10, "[4,4,4,0]", "10");
select a, b from t3 order by l2_distance(b, "[1,1,1,0]") limit 4;


-- 3.b KNN with no PK and embedding and non-indexed columns
select a,b,c from t3 order by l2_distance(b, "[1,1,1,0]") limit 4;

insert into t3 values(11, "[1,1,1,1]", "11");
insert into t3 values(12, "[2,2,2,2]", "12");
insert into t3 values(13, "[3,3,3,3]", "13");

alter table t3 alter reindex idx3 ivfflat lists=4;
select a, b from t3 order by l2_distance(b, "[1,0,0,0]") limit 4;
select a, b from t3 order by l2_distance(b, "[1,1,0,0]") limit 4;
select a, b from t3 order by l2_distance(b, "[1,1,1,0]") limit 4;
select a, b from t3 order by l2_distance(b, "[1,1,1,1]") limit 4;

-- 4.a NULL embedding
drop table if exists t4;
create table t4(a int primary key,b vecf32(4), c varchar(3) );
insert into t4 values(1, "[1,0,0,0]" , "1");
insert into t4 values(2, "[2,0,0,0]", "2");
insert into t4 values(3, "[3,0,0,0]", "3");
insert into t4 values(4, "[1,1,0,0]", "4");
insert into t4 values(5, "[2,2,0,0]", "5");
insert into t4 values(6, "[3,3,0,0]", "6");
insert into t4 values(7, "[1,1,1,0]", "7");
insert into t4 values(8, "[2,2,2,0]", "8");
insert into t4 values(9, NULL, "9");
select a, b from t4 order by l2_distance(normalize_l2(b), normalize_l2("[1,1,1,0]")) limit 3;

create index idx4 using ivfflat on t4(b) lists=3 op_type "vector_l2_ops";
select a, b from t4 order by l2_distance(b, "[1,1,1,0]") limit 4;

insert into t4 values(10, "[4,4,4,0]", "10");
select a, b from t4 order by l2_distance(b, "[1,1,1,0]") limit 4;

-- 5.a Zero Vector
drop table if exists t5;
create table t5(a int primary key,b vecf32(4), c varchar(3) );
insert into t5 values(1, "[1,0,0,0]" , "1");
insert into t5 values(2, "[2,0,0,0]", "2");
insert into t5 values(3, "[3,0,0,0]", "3");
insert into t5 values(4, "[1,1,0,0]", "4");
insert into t5 values(5, "[2,2,0,0]", "5");
insert into t5 values(6, "[3,3,0,0]", "6");
insert into t5 values(7, "[1,1,1,0]", "7");
insert into t5 values(8, "[2,2,2,0]", "8");
insert into t5 values(9, "[3,3,3,3]", "9");
insert into t5 values(10, "[0,0,0,0]", "10");
select a, b from t5 order by l2_distance(normalize_l2(b), normalize_l2("[1,1,1,0]")) limit 3;

create index idx5 using ivfflat on t5(b) lists=3 op_type "vector_l2_ops";
--mysql> select * from `__mo_index_secondary_018ebf8c-c9d6-7c4e-b4c3-b6df681e9017`;
--+--------------------------------+---------------------------+--------------------+------------------------------+
--| __mo_index_centroid_fk_version | __mo_index_centroid_fk_id | __mo_index_pri_col | __mo_index_centroid_fk_entry |
--+--------------------------------+---------------------------+--------------------+------------------------------+
--|                              0 |                         1 |                  1 | [1, 0, 0, 0]                 |
--|                              0 |                         1 |                  2 | [2, 0, 0, 0]                 |
--|                              0 |                         1 |                  3 | [3, 0, 0, 0]                 |
--|                              0 |                         3 |                  4 | [1, 1, 0, 0]                 |
--|                              0 |                         3 |                  5 | [2, 2, 0, 0]                 |
--|                              0 |                         3 |                  6 | [3, 3, 0, 0]                 |
--|                              0 |                         3 |                  7 | [1, 1, 1, 0]                 |
--|                              0 |                         3 |                  8 | [2, 2, 2, 0]                 |
--|                              0 |                         3 |                  9 | [3, 3, 3, 3]                 |
--|                              0 |                         2 |                 10 | [0, 0, 0, 0]                 |
--+--------------------------------+---------------------------+--------------------+------------------------------+
select a, b from t5 order by l2_distance(b, "[1,1,1,0]") limit 7;

insert into t5 values(11, "[4,4,4,0]", "11");
select a, b from t5 order by l2_distance(b, "[1,1,1,0]") limit 7;


-- post
SET GLOBAL experimental_ivf_index = 0;
drop database vecdb3;

