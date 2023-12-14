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

-- 4. Reindex Secondary Index with IVFFLAT.
alter table tbl alter reindex idx1 ivfflat lists=3;
show index from tbl;
show create table tbl;
select name, type, column_name, algo, algo_table_type,algo_params from mo_catalog.mo_indexes where name="idx1";

-- 5. Alter table add column with IVFFLAT.
alter table tbl add c vecf32(3);
show index from tbl;
show create table tbl;
select name, type, column_name, algo, algo_table_type,algo_params from mo_catalog.mo_indexes where name="idx1";

-- 6.  Create IVF index before table has data (we create the 3 hidden tables alone without populating them)
drop table if exists tbl;
create table tbl(a int primary key,b vecf32(3), c vecf64(5));
create index idx2 using IVFFLAT on tbl(b) lists = 2 op_type 'vector_l2_ops';
show index from tbl;
show create table tbl;
select name, type, column_name, algo, algo_table_type,algo_params from mo_catalog.mo_indexes where name="idx2";

-- 7. [FAILURE] Create IVF index on non-vector types, multiple columns, lists=-ve, unknown op_type
drop table if exists tbl;
create table tbl(a int primary key,b vecf32(3), c vecf32(3));
insert into tbl values(1, "[1,2,3]","[1,2,3]");
insert into tbl values(2, "[1,2,4]","[1,2,4]");
insert into tbl values(3, "[1,2.4,4]","[1,2.4,4]");
insert into tbl values(4, "[1,2,5]","[1,2,5]");
create index idx3 using IVFFLAT on tbl(a) lists = 2 op_type 'vector_l2_ops';
create index idx4 using IVFFLAT on tbl(b,c) lists = 2 op_type 'vector_l2_ops';
create index idx5 using IVFFLAT on tbl(b) lists = -1;
create index idx6 using IVFFLAT on tbl(b) op_type 'vector_l1_ops';

-- 8. [Default] Create IVF index without any params
drop table if exists tbl;
create table tbl(a int primary key,b vecf32(3), c vecf32(3));
insert into tbl values(1, "[1,2,3]","[1,2,3]");
insert into tbl values(2, "[1,2,4]","[1,2,4]");
insert into tbl values(3, "[1,2.4,4]","[1,2.4,4]");
insert into tbl values(4, "[1,2,5]","[1,2,5]");
create index idx7 using IVFFLAT on tbl(b);
show index from tbl;
show create table tbl;
select name, type, column_name, algo, algo_table_type,algo_params from mo_catalog.mo_indexes where name="idx7";

-- 9. Null & duplicate rows
drop table if exists tbl;
create table tbl(a int primary key, b vecf32(3));
insert into tbl values(1, "[1,2,3]");
insert into tbl values(2, "[1,2,4]");
insert into tbl values(3, "[1,2.4,4]");
insert into tbl values(4, "[1,2,5]");
insert into tbl values(5, "[1,3,5]");
insert into tbl values(6, "[100,44,50]");
insert into tbl values(7, "[100,44,50]");
insert into tbl values(8, "[130,40,90]");
insert into tbl values(9, null);
insert into tbl values(10, null);
create index idx8 using IVFFLAT on tbl(b) lists = 2 op_type 'vector_l2_ops';
show index from tbl;
show create table tbl;
select name, type, column_name, algo, algo_table_type,algo_params from mo_catalog.mo_indexes where name="idx8";

-- 10. Create IVF index within Create Table (this will create empty index hidden tables)
drop table if exists tbl;
create table tbl(a int primary key, b vecf32(3), index idx9 using ivfflat (b));

-- 11. Delete column having IVFFLAT index
drop table if exists tbl;
create table tbl(a int primary key, b vecf32(3), index idx10 using ivfflat (b));
insert into tbl values(1, "[1,2,3]");
insert into tbl values(2, "[1,2,4]");
show index from tbl;
show create table tbl;
select name, type, column_name, algo, algo_table_type,algo_params from mo_catalog.mo_indexes where name="idx10";
alter table tbl drop column b;
show index from tbl;
show create table tbl;
select name, type, column_name, algo, algo_table_type,algo_params from mo_catalog.mo_indexes where name="idx10";

-- 12. Drop IVFFLAT index
drop table if exists tbl;
create table tbl(a int primary key, b vecf32(3), index idx11 using ivfflat (b));
insert into tbl values(1, "[1,2,3]");
insert into tbl values(2, "[1,2,4]");
show index from tbl;
show create table tbl;
select name, type, column_name, algo, algo_table_type,algo_params from mo_catalog.mo_indexes where name="idx11";
alter table tbl drop index idx11;
show index from tbl;
show create table tbl;
select name, type, column_name, algo, algo_table_type,algo_params from mo_catalog.mo_indexes where name="idx11";

-- post
drop database vecdb2;