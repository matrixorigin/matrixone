-- pre
drop database if exists vecdb;
create database vecdb;
use vecdb;
drop table if exists vec_table;

-- standard
create table vec_table(a int, b vecf32(3), c vecf64(3));
desc vec_table;
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
select l2_distance(b,"[1,2,3]") from vec_table;
select cosine_distance(b,"[1,2,3]") from vec_table;
select normalize_l2(b) from vec_table;

-- top K
select * FROM vec_table ORDER BY cosine_similarity(b, '[3,1,2]') LIMIT 5;
select * FROM vec_table ORDER BY l2_distance(b, '[3,1,2]') LIMIT 5;
select * FROM vec_table ORDER BY inner_product(b, '[3,1,2]') LIMIT 5;


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

-- insert test (more dim error)
create table t4(a int, b vecf32(5), c vecf64(5));
insert into t4 values(1, "[1,2,3,4,5]", "[1,2,3,4,5]");
insert into t4 values(1, "[1,2]", "[1,2]");
insert into t4 values(1, "[1,2,3,4,5,6]", "[1,2,3,4,5,6]");
select * from t4;

-- insert vector as binary
create table t5(a int, b vecf32(3));
insert into t5 values(1, unhex('7e98b23e9e10383b2f41133f'));
insert into t5 values(2, unhex('0363733ff13e0b3f7aa39d3e'));
insert into t5 values(3, unhex('be1ac03e485d083ef6bc723f'));

insert into t5 values(4, "[0,2,3]");

insert into t5 values(5, unhex('05486c3f3ee2863e713d503dd58e8e3e7b88743f')); -- this is float32[5]
insert into t5 values(6, unhex('9be2123fcf92de3e')); -- this is float32[2]

select * from t5;
select * from t5 where t5.b > "[0,0,0]";

-- output vector as binary (the output is little endian hex encoding)
select hex(b) from t5;

-- insert nulls
create table t6(a int, b vecf32(3));
insert into t6 values(1, null);
insert into t6 (a,b) values (1, '[1,2,3]'), (2, '[4,5,6]'), (3, '[2,1,1]'), (4, '[7,8,9]'), (5, '[0,0,0]'), (6, '[3,1,2]');
select * from t6;
update t6 set b = NULL;
select * from t6;

-- vector precision/scale check
create table t7(a int, b vecf32(3), c vecf32(5));
insert into t7 values(1, NULL,NULL);
insert into t7 values(2, "[0.8166459, 0.66616553, 0.4886152]", NULL);
insert into t7 values(3, "[0.1726299, 3.2908857, 30.433094]","[0.45052445, 2.1984527, 9.579752, 123.48039, 4635.894]");
insert into t7 values(4, "[8.560689, 6.790359, 821.9778]", "[0.46323407, 23.498016, 563.923, 56.076736, 8732.958]");
select * from t7;
select a, b + b, c + c from t7;
select a, b * b, c * c from t7;
select l2_norm(b), l2_norm(c) from t7;


-- insert, flush and select
insert into vec_table values(2, "[0,2,3]", "[4,4,6]");
insert into vec_table values(3, "[1,3,3]", "[4,1,6]");
-- @separator:table
select mo_ctl('dn', 'flush', 'vecdb.vec_table');
-- @separator:table
select mo_ctl('dn', 'flush', 'vecdb.t6');
select * from vec_table where b> "[1,2,3]";
select * from vec_table where b!= "[1,2,3]";
select * from vec_table where b= "[1,2,3]";

-- create table with PK or UK or No Key (https://github.com/matrixorigin/matrixone/issues/13038)
create table vec_table1(a int, b vecf32(3), c vecf64(3));
insert into vec_table1 values(1, "[1,2,3]", "[4,5,6]");
select * from vec_table1;
create table vec_table2(a int primary key, b vecf32(3), c vecf64(3));
insert into vec_table2 values(1, "[1,2,3]", "[4,5,6]");
select * from vec_table2;
create table vec_table3(a int unique key, b vecf32(3), c vecf64(3));
insert into vec_table3 values(1, "[1,2,3]", "[4,5,6]");
select * from vec_table3;

-- Scalar Null check
select summation(null);
select l1_norm(null);
select l2_norm(null);
select vector_dims(null);
select inner_product(null, "[1,2,3]");
select cosine_similarity(null, "[1,2,3]");
select l2_distance(null, "[1,2,3]");
select cosine_distance(null, "[1,2,3]");
select normalize_l2(null);
select cast(null as vecf32(3));
select cast(null as vecf64(3));

-- Precision issue for Cosine Similarity/Distance
create table t8(a int, b vecf32(3), c vecf32(5));
INSERT INTO `t8` VALUES (1,NULL,NULL);
INSERT INTO `t8` VALUES(2,'[0.8166459, 0.66616553, 0.4886152]',NULL);
INSERT INTO `t8` VALUES(3,'[0.1726299, 3.2908857, 30.433094]','[0.45052445, 2.1984527, 9.579752, 123.48039, 4635.894]');
INSERT INTO `t8` VALUES(4,'[8.560689, 6.790359, 821.9778]','[0.46323407, 23.498016, 563.923, 56.076736, 8732.958]');
select cosine_similarity(b,b), cosine_similarity(c,c) from t8;

create table t9(a int, b vecf64(3), c vecf64(5));
INSERT INTO `t9` VALUES (1,NULL,NULL);
INSERT INTO `t9` VALUES (2,'[0.8166459, 0.66616553, 0.4886152]',NULL);
INSERT INTO `t9` VALUES (3,'[8.5606893, 6.7903588, 821.977768]','[0.46323407, 23.49801546, 563.9229458, 56.07673508, 8732.9583881]');
INSERT INTO `t9` VALUES (4,'[0.9260021, 0.26637346, 0.06567037]','[0.45756745, 65.2996871, 321.623636, 3.60082066, 87.58445764]');
select cosine_similarity(b,b), cosine_similarity(c,c) from t9;

-- Sub Vector
create table t10(a int, b vecf32(3), c vecf64(3));
insert into t10 values(1, "[1,2.4,3]", "[4.1,5,6]");
insert into t10 values(2, "[3,4,5]", "[6,7.3,8]");
insert into t10 values(3, "[5,6,7]", "[8,9,10]");
select subvector(b,1) from t10;
select subvector(b,2) from t10;
select subvector(b,3) from t10;
select subvector(b,4) from t10;
select subvector(b,-1) from t10;
select subvector(b,-2) from t10;
select subvector(b,-3) from t10;
select subvector(b, 1, 1) from t10;
select subvector(b, 1, 2) from t10;
select subvector(b, 1, 3) from t10;
select subvector(b, 1, 4) from t10;
select subvector(b, -1, 1) from t10;
select subvector(b, -2, 1) from t10;
select subvector(b, -3, 1) from t10;
SELECT SUBVECTOR("[1,2,3]", 2);
SELECT SUBVECTOR("[1,2,3]",2,1);

-- Arithmetic Operators between Vector and Scalar
select b + 2 from t10;
select b - 2 from t10;
select b * 2 from t10;
select b / 2 from t10;
select 2 + b from t10;
select 2 - b from t10;
select 2 * b from t10;
select 2 / b from t10;
select b + 2.0 from t10;
select b - 2.0 from t10;
select b * 2.0 from t10;
select b / 2.0 from t10;
select 2.0 + b from t10;
select 2.0 - b from t10;
select 2.0 * b from t10;
select 2.0 / b from t10;
select cast("[1,2,3]" as vecf32(3)) + 2;
select cast("[1,2,3]" as vecf32(3)) - 2;
select cast("[1,2,3]" as vecf32(3)) * 2;
select cast("[1,2,3]" as vecf32(3)) / 2;
select 2 + cast("[1,2,3]" as vecf32(3));
select 2 - cast("[1,2,3]" as vecf32(3));
select 2 * cast("[1,2,3]" as vecf32(3));
select 2 / cast("[1,2,3]" as vecf32(3));
select cast("[1,2,3]" as vecf32(3)) + 2.0;
select cast("[1,2,3]" as vecf32(3)) - 2.0;
select cast("[1,2,3]" as vecf32(3)) * 2.0;
select cast("[1,2,3]" as vecf32(3)) / 2.0;
select 2.0 + cast("[1,2,3]" as vecf32(3));
select 2.0 - cast("[1,2,3]" as vecf32(3));
select 2.0 * cast("[1,2,3]" as vecf32(3));
select 2.0 / cast("[1,2,3]" as vecf32(3));
select cast("[1,2,3]" as vecf32(3)) / 0 ;
select 5 + (-1*cast("[1,2,3]" as vecf32(3)));

-- Distinct SQL
create table t11(a vecf32(2));
insert into t11 values('[1,0]');
insert into t11 values('[1,2]');
select distinct a from t11;
select distinct a,a from t11;

-- TinyInt + Vector
drop table if exists t1;
create table t1(c1 int,c2 vecf32(5),c3 tinyint unsigned,c4 bigint,c5 decimal(4,2),c6 float,c7 double);
insert into t1 values(10 ,"[1, 0, 1, 6, 6]",3,10,7.1,0.36,2.10);
insert into t1 values(60,"[6, 0, 8, 10,129]",2,5,3.26,4.89,1.26);
insert into t1 values(20,"[ 9, 18, 1, 4, 132]",6,1,9.36,6.9,5.6);
select c2+c3 from t1;

-- Except
select * from t8 except select * from t9;

-- post
drop database vecdb;