-- pre
drop database if exists vecdb3;
create database vecdb3;
use vecdb3;

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
select a, b from t1 order by l2_distance(b, "[1,1,1,0]") limit 3;

insert into t1 values(10, "[4,4,4,0]", "10");
select a, b from t1 order by l2_distance(b, "[1,1,1,0]") limit 4;

-- 1.b KNN with pk and embedding and non-indexed columns
select a,b,c from t1 order by l2_distance(b, "[1,1,1,0]") limit 4;


insert into t1 values(11, "[1,1,1,1]", "11");
insert into t1 values(12, "[2,2,2,2]", "12");
insert into t1 values(13, "[3,3,3,3]", "13");

alter table t1 alter reindex idx1 ivfflat lists=4;
select a, b from t1 order by l2_distance(b, "[1,1,1,0]") limit 4;
select a, b from t1 order by l2_distance(b, "[1,1,0,0]") limit 3;
select a, b from t1 order by l2_distance(b, "[1,0,0,0]") limit 3; -- problem
select a, b from t1 order by l2_distance(b, "[1,1,1,1]") limit 3; -- problem

-- post
drop database vecdb3;

