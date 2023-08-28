-- pre
drop database if exists vecdb;
create database vecdb;
use vecdb;
drop table if exists vec_table;

-- standard
create table vec_table(a int, b vecf32(3), c vecf64(3));
insert into vec_table values(1, "[1,2,3]", "[4,5,6]");
select * from vec_table;

-- binary operators
select b+b from vec_table;
select b-b from vec_table;
select b*b from vec_table;
select b/b from vec_table;
select * from vec_table where b> "[1,2,3]";
select * from vec_table where b< "[1,2,3]";
select * from vec_table where b>= "[1,2,3]";
select * from vec_table where b<= "[1,2,3]";
select * from vec_table where b!= "[1,2,3]";
select * from vec_table where b= "[1,2,3]";
select * from vec_table where b= cast("[1,2,3]" as vecf32(3));
select b + "[1,2,3]" from vec_table;
select b + "[1,2]" from vec_table;
select b + "[1,2,3,4]" from vec_table;


-- cast
select cast("[1,2,3]" as vecf32(3));
select b + "[1,2,3]" from vec_table;
select b + sqrt(b) from vec_table;
select b + c from vec_table;

-- vector ops
select abs(b) from vec_table;
select abs(cast("[-1,-2,3]" as vecf32(3)));
select sqrt(b) from vec_table;
select summation(b) from vec_table;
select l1_norm(b) from vec_table;
select l2_norm(b) from vec_table;
select vector_dims(b) from vec_table;
select inner_product(b,"[1,2,3]") from vec_table;
select cosine_similarity(b,"[1,2,3]") from vec_table;

-- top K
select * FROM vec_table ORDER BY cosine_similarity(b, '[3,1,2]') LIMIT 5;

-- throw error cases
select b + "[1,2,3" from vec_table;
select b + "1,2,3" from vec_table;
create table t2(a int, b vecf32(3) primary key);
create unique index t3 on vec_table(b);
create table t3(a int, b vecf32(65537));

-- throw error for Nan/Inf
select sqrt(cast("[1,2,-3]" as vecf32(3)));
select b/(cast("[1,2,0]" as vecf32(3))) from vec_table;

-- agg
select count(b) from vec_table;

-- insert test (zero pad, more dim error)
create table t4(a int, b vecf32(5), c vecf64(5));
insert into t4 values(1, "[1,2,3,4,5]", "[1,2,3,4,5]");
insert into t4 values(1, "[1,2]", "[1,2]");
insert into t4 values(1, "[1,2,3,4,5,6]", "[1,2,3,4,5,6]");
select * from t4;

-- insert, flush and select
insert into vec_table values(2, "[0,2,3]", "[4,4,6]");
insert into vec_table values(3, "[1,3,3]", "[4,1,6]");
-- @separator:table
select mo_ctl('dn', 'flush', 'vecdb.vec_table');
select * from vec_table where b> "[1,2,3]";
select * from vec_table where b!= "[1,2,3]";
select * from vec_table where b= "[1,2,3]";

-- post
drop database vecdb;