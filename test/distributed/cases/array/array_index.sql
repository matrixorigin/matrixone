-- pre
drop database if exists vecdb;
create database vecdb;
use vecdb;

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
select cluster_centers(b spherical_kmeans '2,L2') from t1;
select cluster_centers(b spherical_kmeans '2,IP') from t1;
select cluster_centers(b spherical_kmeans '2,COSINE') from t1;
SELECT value FROM  (SELECT cluster_centers(b spherical_kmeans '2,L2') AS centers FROM t1) AS subquery CROSS JOIN  UNNEST(subquery.centers) AS u;

-- 2. kmeans on vecf64 column
select a,c,normalize_l2(c) from t1;
select cluster_centers(c spherical_kmeans '2,L2') from t1;
select cluster_centers(c spherical_kmeans '2,IP') from t1;
select cluster_centers(c spherical_kmeans '2,COSINE') from t1;
SELECT value FROM  (SELECT cluster_centers(c spherical_kmeans '2,L2') AS centers FROM t1) AS subquery CROSS JOIN  UNNEST(subquery.centers) AS u;


-- post
drop database vecdb;