-- pre
drop database if exists vecdb2;
create database vecdb2;
use vecdb2;

-- create table
drop table if exists t1;
create table t1(a int primary key,b vecf32(3), c vecf64(5));
insert into t1 values(1, "[1,2,3]" , "[1,2,3,4,5");
insert into t1 values(2, "[1,2,4]", "[1,2,4,4,5]");
insert into t1 values(3, "[1,2.4,4]", "[1,2.4,4,4,5]");
insert into t1 values(4, "[1,2,5]", "[1,2,5,4,5]");
insert into t1 values(5, "[1,3,5]", "[1,3,5,4,5]");
insert into t1 values(6, "[100,44,50]", "[100,44,50,60,70]");
insert into t1 values(7, "[120,50,70]", "[120,50,70,80,90]");
insert into t1 values(8, "[130,40,90]", "[130,40,90,100,110]");

-- 1. kmeans on vecf32 column
select a,b,normalize_l2(b) from t1;
select cluster_centers(b spherical_kmeans '2,vector_l2_ops') from t1;
select cluster_centers(b spherical_kmeans '2,vector_ip_ops') from t1;
select cluster_centers(b spherical_kmeans '2,vector_cosine_ops') from t1;
SELECT value FROM  (SELECT cluster_centers(b spherical_kmeans '2,vector_cosine_ops') AS centers FROM t1) AS subquery CROSS JOIN  UNNEST(subquery.centers) AS u;

-- 2. kmeans on vecf64 column
select a,c,normalize_l2(c) from t1;
select cluster_centers(c spherical_kmeans '2,vector_l2_ops') from t1;
select cluster_centers(c spherical_kmeans '2,vector_ip_ops') from t1;
select cluster_centers(c spherical_kmeans '2,vector_cosine_ops') from t1;
SELECT value FROM  (SELECT cluster_centers(c spherical_kmeans '2,vector_cosine_ops') AS centers FROM t1) AS subquery CROSS JOIN  UNNEST(subquery.centers) AS u;



-- 3. Create Secondary Index with IVFFLAT.
drop table if exists tbl;
create table tbl(id int primary key, embedding vecf32(3));
insert into tbl values(1, "[1,2,3]");
insert into tbl values(2, "[1,2,4]");
insert into tbl values(3, "[1,2.4,4]");
insert into tbl values(4, "[1,2,5]");
insert into tbl values(5, "[1,3,5]");
insert into tbl values(6, "[100,44,50]");
insert into tbl values(7, "[120,50,70]");
insert into tbl values(8, "[130,40,90]");
create index idx1 using IVFFLAT on tbl(embedding) lists = 2 op_type 'vector_l2_ops';

show index from tbl;
show create table tbl;
select name, type, column_name, algo, algo_table_type,algo_params from mo_catalog.mo_indexes where name="idx1";

alter table tbl alter reindex idx1 ivfflat lists=3;
show index from tbl;
show create table tbl;
select name, type, column_name, algo, algo_table_type,algo_params from mo_catalog.mo_indexes where name="idx1";

-- post
drop database vecdb2;