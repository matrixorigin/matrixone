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
select cluster_centers(b kmeans '2,vector_l2_ops') from t1;
select cluster_centers(b kmeans '2,vector_ip_ops') from t1;
select cluster_centers(b kmeans '2,vector_cosine_ops') from t1;
SELECT value FROM  (SELECT cluster_centers(b kmeans '2,vector_l2_ops,kmeansplusplus,false') AS centers FROM t1) AS subquery CROSS JOIN  UNNEST(subquery.centers) AS u;

-- 1.b spherical kmeans on vecf32 column
SELECT value FROM  (SELECT cluster_centers(b kmeans '2,vector_l2_ops,kmeansplusplus,true') AS centers FROM t1) AS subquery CROSS JOIN  UNNEST(subquery.centers) AS u;

-- 2. kmeans on vecf64 column
select a,c,normalize_l2(c) from t1;
select cluster_centers(c kmeans '2,vector_l2_ops') from t1;
select cluster_centers(c kmeans '2,vector_ip_ops') from t1;
select cluster_centers(c kmeans '2,vector_cosine_ops') from t1;
SELECT value FROM  (SELECT cluster_centers(c kmeans '2,vector_l2_ops') AS centers FROM t1) AS subquery CROSS JOIN  UNNEST(subquery.centers) AS u;

-- 2.b spherical kmeans on vecf64 column
SELECT value FROM  (SELECT cluster_centers(c kmeans '2,vector_l2_ops,kmeansplusplus,true') AS centers FROM t1) AS subquery CROSS JOIN  UNNEST(subquery.centers) AS u;

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
create index idx6 using IVFFLAT on tbl(b) lists = 1 op_type 'vector_l1_ops';

-- 8. [Default] Create IVF index without any params -- will fail since we don't have lists argument.
drop table if exists tbl;
create table tbl(a int primary key,b vecf32(3), c vecf32(3));
insert into tbl values(1, "[1,2,3]","[1,2,3]");
insert into tbl values(2, "[1,2,4]","[1,2,4]");
insert into tbl values(3, "[1,2.4,4]","[1,2.4,4]");
insert into tbl values(4, "[1,2,5]","[1,2,5]");
create index idx7 using IVFFLAT on tbl(b);

-- 9. duplicate rows
drop table if exists tbl;
create table tbl(a int primary key, b vecf32(3));
insert into tbl values(1, "[1,2,3]");
insert into tbl values(2, "[1,2,4]");
insert into tbl values(3, "[1,2.4,4]");
insert into tbl values(4, "[1,2,5]");
insert into tbl values(5, "[1,3,5]");
insert into tbl values(6, "[100,44,50]"); -- dup
insert into tbl values(7, "[100,44,50]");
insert into tbl values(8, "[130,40,90]");
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

-- 13. Insert into index table (with one PK)
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
create index idx12 using ivfflat on tbl(embedding) lists=2 op_type "vector_l2_ops";
show index from tbl;
show create table tbl;
insert into tbl values(15, "[1,3,5]"); -- inserted to centroid 1 of version 0
insert into tbl values(18, "[130,40,90]"); -- inserted to centroid 2 of version 0
alter table tbl alter reindex idx12 ivfflat lists=2;
insert into tbl values(25, "[2,4,5]"); -- inserted to cluster 1 of version 1
insert into tbl values(28, "[131,41,91]"); -- inserted to cluster 2 of version 1

-- 14. Insert into index table (with CP key)
drop table if exists tbl;
create table tbl(id int, age int, embedding vecf32(3), primary key(id, age));
insert into tbl values(1, 10, "[1,2,3]");
insert into tbl values(2, 20, "[1,2,4]");
insert into tbl values(3, 30, "[1,2.4,4]");
insert into tbl values(4, 40, "[1,2,5]");
insert into tbl values(5, 50, "[1,3,5]");
insert into tbl values(6, 60, "[100,44,50]");
insert into tbl values(7, 70, "[120,50,70]");
insert into tbl values(8, 80, "[130,40,90]");
create index idx13 using ivfflat on tbl(embedding) lists=2 op_type "vector_l2_ops";
show index from tbl;
show create table tbl;
insert into tbl values(15, 90, "[1,3,5]"); -- inserted to centroid 1 of version 0
insert into tbl values(18, 100, "[130,40,90]"); -- inserted to centroid 2 of version 0
alter table tbl alter reindex idx13 ivfflat lists=2;
insert into tbl values(25, 110, "[2,4,5]"); -- inserted to cluster 1 of version 1
insert into tbl values(28, 120, "[131,41,91]"); -- inserted to cluster 2 of version 1


-- 15. Insert into index table (with No PK so fake_pk is used)
drop table if exists tbl;
create table tbl(id int, embedding vecf32(3));
insert into tbl values(1, "[1,2,3]");
insert into tbl values(2, "[1,2,4]");
insert into tbl values(3, "[1,2.4,4]");
insert into tbl values(4, "[1,2,5]");
insert into tbl values(5, "[1,3,5]");
insert into tbl values(6, "[100,44,50]");
insert into tbl values(7, "[120,50,70]");
insert into tbl values(8, "[130,40,90]");
create index idx14 using ivfflat on tbl(embedding) lists=2 op_type "vector_l2_ops";
show index from tbl;
show create table tbl;
insert into tbl values(15, "[1,3,5]"); -- inserted to centroid 1 of version 0
insert into tbl values(18, "[130,40,90]"); -- inserted to centroid 2 of version 0
alter table tbl alter reindex idx14 ivfflat lists=2;
insert into tbl values(25, "[2,4,5]"); -- inserted to cluster 1 of version 1
insert into tbl values(28, "[131,41,91]"); -- inserted to cluster 2 of version 1

-- 16. Delete embedding from original table
-- 17. Delete PK from original table
---18. Perform both 16 & 17 after alter reindex.
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
create index idx15 using ivfflat on tbl(embedding) lists=2 op_type "vector_l2_ops";
insert into tbl values(9, "[130,40,90]");
delete from tbl where id=9; -- delete 9
delete from tbl where embedding="[130,40,90]"; -- delete 8
alter table tbl alter reindex idx15 ivfflat lists=2;
insert into tbl values(10, "[130,40,90]");
delete from tbl where id=6; -- removes both (0,6) and (1,6) entries
delete from tbl where embedding="[1,3,5]"; -- removes both (0,5) and (1,5) entries
delete from tbl where id=10; -- removes (1,10)

-- 19. Delete from without condition
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
create index idx16 using ivfflat on tbl(embedding) lists=2 op_type "vector_l2_ops";
delete from tbl;

-- 20. Truncate tbl
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
create index idx16 using ivfflat on tbl(embedding) lists=2 op_type "vector_l2_ops";
truncate table tbl;

-- 21. Update embedding from original table
-- 22. Update PK from original table
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
create index idx16 using ivfflat on tbl(embedding) lists=2 op_type "vector_l2_ops";
update tbl set embedding="[1,2,3]" where id=8; -- update 8 to cluster 1 from cluster 2
update tbl set id=9 where id=8; -- update 8 to 9
alter table tbl alter reindex idx16 ivfflat lists=2;
update tbl set embedding="[1,2,3]" where id=7; -- update 7 to cluster 1 from cluster 2 for the latest versions
update tbl set id=10 where id=7; -- update 7 to 10

-- 23. Update & Delete with CP key
drop table if exists tbl;
create table tbl(id varchar(20), age varchar(20), embedding vecf32(3), primary key(id, age));
insert into tbl values("1", "10", "[1,2,3]");
insert into tbl values("2", "20", "[1,2,4]");
insert into tbl values("3", "30", "[1,2.4,4]");
insert into tbl values("4", "40", "[1,2,5]");
insert into tbl values("5", "50", "[1,3,5]");
insert into tbl values("6", "60", "[100,44,50]");
insert into tbl values("7", "70", "[120,50,70]");
insert into tbl values("8", "80", "[130,40,90]");
create index idx17 using ivfflat on tbl(embedding) lists=2 op_type "vector_l2_ops";
update tbl set embedding="[1,2,3]" where id="8";
update tbl set embedding="[1,2,3]" where id="7" and age = "70";
update tbl set id="70" where id="7";
alter table tbl alter reindex idx17 ivfflat lists=2;
update tbl set embedding="[1,2,3]" where id="6";
update tbl set id="60" where id="6";

-- 24. Update & Delete with No PK so fake_pk is used
drop table if exists tbl;
create table tbl(id int, embedding vecf32(3));
insert into tbl values(1, "[1,2,3]");
insert into tbl values(2, "[1,2,4]");
insert into tbl values(3, "[1,2.4,4]");
insert into tbl values(4, "[1,2,5]");
insert into tbl values(5, "[1,3,5]");
insert into tbl values(6, "[100,44,50]");
insert into tbl values(7, "[120,50,70]");
insert into tbl values(8, "[130,40,90]");
create index idx17 using ivfflat on tbl(embedding) lists=2 op_type "vector_l2_ops";
update tbl set embedding="[1,2,3]" where id="8";
delete from tbl where id="8";

-- 25. create table with index def
drop table if exists tbl;
create table tbl(id int primary key, embedding vecf32(3), key idx18 using ivfflat (embedding) lists=2 op_type "vector_l2_ops");
insert into tbl values(1, "[1,2,3]");
insert into tbl values(2, "[1,2,4]");
insert into tbl values(3, "[1,2.4,4]");
insert into tbl values(4, "[1,2,5]");
insert into tbl values(5, "[1,3,5]");
insert into tbl values(6, "[100,44,50]");
insert into tbl values(7, "[120,50,70]");
insert into tbl values(8, "[130,40,90]");

-- 26. Update table add new column
alter table tbl add column id2 VARCHAR(20);
--mysql> select * from `__mo_index_secondary_8ff07b6e-a483-11ee-b461-723e89f7b974`;
-- alter table --> create table --> insert into select * --> reindex
--+--------------------------------+---------------------------+--------------------+
--| __mo_index_centroid_fk_version | __mo_index_centroid_fk_id | __mo_index_pri_col |
--+--------------------------------+---------------------------+--------------------+
--|                              0 |                         1 |                  1 |
--|                              0 |                         1 |                  2 |
--|                              0 |                         1 |                  3 |
--|                              0 |                         1 |                  4 |
--|                              0 |                         1 |                  5 |
--|                              0 |                         1 |                  6 |
--|                              0 |                         1 |                  7 |
--|                              0 |                         1 |                  8 |
--|                              1 |                         1 |                  1 |
--|                              1 |                         1 |                  2 |
--|                              1 |                         1 |                  3 |
--|                              1 |                         1 |                  4 |
--|                              1 |                         1 |                  5 |
--|                              1 |                         2 |                  6 |
--|                              1 |                         2 |                  7 |
--|                              1 |                         2 |                  8 |
--+--------------------------------+---------------------------+--------------------+

update tbl set id2 = id;

-- 27. Insert into table select (internally uses window row_number)
drop table if exists tbl1;
create table tbl1(id int primary key, data vecf32(3));
insert into tbl1 values(1, "[1,2,3]");
insert into tbl1 values(2, "[1,2,4]");
insert into tbl1 values(3, "[1,2.4,4]");
insert into tbl1 values(4, "[1,2,5]");
insert into tbl1 values(5, "[1,3,5]");
insert into tbl1 values(6, "[100,44,50]");
insert into tbl1 values(7, "[120,50,70]");
insert into tbl1 values(8, "[130,40,90]");
create index idx19 using ivfflat on tbl1(data) lists=2 op_type "vector_l2_ops";
insert into tbl1 values(9, "[130,40,90]");

drop table if exists tbl2;
create table tbl2(id int primary key, data vecf32(3), key idx20 using ivfflat (data) lists=2 op_type "vector_l2_ops");
insert into tbl2 select * from tbl1;
--mysql> select * from `__mo_index_secondary_0b0c1e94-a483-11ee-b45f-723e89f7b974`;
-- assigned all the rows to 1 giant null centroid
--+--------------------------------+---------------------------+--------------------+
--| __mo_index_centroid_fk_version | __mo_index_centroid_fk_id | __mo_index_pri_col |
--+--------------------------------+---------------------------+--------------------+
--|                              0 |                         1 |                  1 |
--|                              0 |                         1 |                  2 |
--|                              0 |                         1 |                  3 |
--|                              0 |                         1 |                  4 |
--|                              0 |                         1 |                  5 |
--|                              0 |                         1 |                  6 |
--|                              0 |                         1 |                  7 |
--|                              0 |                         1 |                  8 |
--|                              0 |                         1 |                  9 |
--+--------------------------------+---------------------------+--------------------+


-- 28. Create Index with no rows
drop table if exists tbl1;
create table tbl1(id int primary key, data vecf32(3));
create index idx19 using ivfflat on tbl1(data) lists=2 op_type "vector_l2_ops";
insert into tbl1 values(1, "[1,2,3]");
insert into tbl1 values(2, "[1,2,4]");
insert into tbl1 values(3, "[1,2.4,4]");
insert into tbl1 values(4, "[1,2,5]");
insert into tbl1 values(5, "[1,3,5]");
insert into tbl1 values(6, "[100,44,50]");
insert into tbl1 values(7, "[120,50,70]");
insert into tbl1 values(8, "[130,40,90]");
--mysql> select * from `__mo_index_secondary_0b0c1e94-a483-11ee-b45f-723e89f7b974`;
-- assigned all the rows to 1 giant null centroid
--+--------------------------------+---------------------------+--------------------+
--| __mo_index_centroid_fk_version | __mo_index_centroid_fk_id | __mo_index_pri_col |
--+--------------------------------+---------------------------+--------------------+
--|                              0 |                         1 |                  1 |
--|                              0 |                         1 |                  2 |
--|                              0 |                         1 |                  3 |
--|                              0 |                         1 |                  4 |
--|                              0 |                         1 |                  5 |
--|                              0 |                         1 |                  6 |
--|                              0 |                         1 |                  7 |
--|                              0 |                         1 |                  8 |
--|                              0 |                         1 |                  9 |
--+--------------------------------+---------------------------+--------------------+
--9 rows in set (0.00 sec)

-- 29. Handle Null embeddings
drop table if exists tbl;
create table tbl(id int primary key, data vecf32(3));
insert into tbl values(1, NULL);
insert into tbl values(2, NULL);
insert into tbl values(3, NULL);
insert into tbl values(4, "[1,2,5]");
insert into tbl values(5, "[1,3,5]");
create index idx20 using ivfflat on tbl(data) lists=2 op_type "vector_l2_ops";
insert into tbl values(6, NULL);
insert into tbl values(7, "[130,40,90]");
--mysql> select * from `__mo_index_secondary_56ab082e-a483-11ee-b461-723e89f7b974`;
-- null are randomly assigned to clusters
--+--------------------------------+---------------------------+--------------------+
--| __mo_index_centroid_fk_version | __mo_index_centroid_fk_id | __mo_index_pri_col |
--+--------------------------------+---------------------------+--------------------+
--|                              0 |                         1 |                  1 |
--|                              0 |                         1 |                  2 |
--|                              0 |                         1 |                  3 |
--|                              0 |                         1 |                  4 |
--|                              0 |                         2 |                  5 |
--|                              0 |                         1 |                  6 |
--|                              0 |                         2 |                  7 |
--+--------------------------------+---------------------------+--------------------+


-- 30. create index with totalCnt < k
drop table if exists tbl;
create table tbl(id int primary key, data vecf32(3));
insert into tbl values(1, "[1,2,3]");
insert into tbl values(2, "[1,2,4]");
create index idx21 using ivfflat on tbl(data) lists=3 op_type "vector_l2_ops";
--mysql> select * from `__mo_index_secondary_ca68dc64-a483-11ee-b461-723e89f7b974`;
--+-----------------------------+------------------------+---------------------+
--| __mo_index_centroid_version | __mo_index_centroid_id | __mo_index_centroid |
--+-----------------------------+------------------------+---------------------+
--|                           0 |                      1 | NULL                |
--+-----------------------------+------------------------+---------------------+


-- 31. create index with totalCnt = 0
drop table if exists tbl;
create table tbl(id int primary key, data vecf32(3));
create index idx22 using ivfflat on tbl(data) lists=3 op_type "vector_l2_ops";
insert into tbl values(1, "[1,2,3]");
insert into tbl values(2, "[1,2,4]");
--mysql> select * from `__mo_index_secondary_ef84b932-a483-11ee-b462-723e89f7b974`;
--+-----------------------------+------------------------+---------------------+
--| __mo_index_centroid_version | __mo_index_centroid_id | __mo_index_centroid |
--+-----------------------------+------------------------+---------------------+
--|                           0 |                      1 | NULL                |
--+-----------------------------+------------------------+---------------------+

-- 32. Truncate table (does not remove centroids)
drop table if exists tbl;
create table tbl(id int, embedding vecf32(3));
insert into tbl values(1, "[1,2,3]");
insert into tbl values(2, "[1,2,4]");
insert into tbl values(3, "[1,2.4,4]");
insert into tbl values(4, "[1,2,5]");
insert into tbl values(5, "[1,3,5]");
insert into tbl values(6, "[100,44,50]");
insert into tbl values(7, "[120,50,70]");
insert into tbl values(8, "[130,40,90]");
create index idx23 using ivfflat on tbl(embedding) lists=2 op_type "vector_l2_ops";
alter table tbl alter reindex idx23 ivfflat lists=2;
truncate table tbl;
--mysql> select * from `__mo_index_secondary_c18d6262-a56a-11ee-8301-723e89f7b974`;
--+-----------------------------+------------------------+-----------------------------+
--| __mo_index_centroid_version | __mo_index_centroid_id | __mo_index_centroid         |
--+-----------------------------+------------------------+-----------------------------+
--|                           0 |                      1 | [1, 2.28, 4.2]              |
--|                           0 |                      2 | [116.666664, 44.666668, 70] |
--|                           1 |                      1 | [1, 2.28, 4.2]              |
--|                           1 |                      2 | [116.666664, 44.666668, 70] |
--+-----------------------------+------------------------+-----------------------------+
insert into tbl values(1, "[1,2,3]");
insert into tbl values(2, "[1,2,4]");
insert into tbl values(3, "[1,2.4,4]");
insert into tbl values(4, "[1,2,5]");
insert into tbl values(5, "[1,3,5]");
insert into tbl values(6, "[100,44,50]");
insert into tbl values(7, "[120,50,70]");
insert into tbl values(8, "[130,40,90]");

-- 33. Delete from tbl (without condition)
drop table if exists tbl;
create table tbl(id int, embedding vecf32(3));
insert into tbl values(1, "[1,2,3]");
insert into tbl values(2, "[1,2,4]");
insert into tbl values(3, "[1,2.4,4]");
insert into tbl values(4, "[1,2,5]");
insert into tbl values(5, "[1,3,5]");
insert into tbl values(6, "[100,44,50]");
insert into tbl values(7, "[120,50,70]");
insert into tbl values(8, "[130,40,90]");
create index idx23 using ivfflat on tbl(embedding) lists=2 op_type "vector_l2_ops";
alter table tbl alter reindex idx23 ivfflat lists=2;
delete from tbl;
--mysql> select * from `__mo_index_secondary_9aaa720c-a56a-11ee-8301-723e89f7b974`;
--+-----------------------------+------------------------+-----------------------------+
--| __mo_index_centroid_version | __mo_index_centroid_id | __mo_index_centroid         |
--+-----------------------------+------------------------+-----------------------------+
--|                           0 |                      1 | [1, 2.28, 4.2]              |
--|                           0 |                      2 | [116.666664, 44.666668, 70] |
--|                           1 |                      1 | [1, 2.28, 4.2]              |
--|                           1 |                      2 | [116.666664, 44.666668, 70] |
--+-----------------------------+------------------------+-----------------------------+
insert into tbl values(1, "[1,2,3]");
insert into tbl values(2, "[1,2,4]");
insert into tbl values(3, "[1,2.4,4]");
insert into tbl values(4, "[1,2,5]");
insert into tbl values(5, "[1,3,5]");
insert into tbl values(6, "[100,44,50]");
insert into tbl values(7, "[120,50,70]");
insert into tbl values(8, "[130,40,90]");


--- 34. 2 Vector Index on same column
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
create index idx16 using ivfflat on tbl(embedding) lists=2 op_type "vector_l2_ops";
create index idx17 using ivfflat on tbl(embedding) lists=2 op_type "vector_l2_ops";
insert into tbl values(9, "[130,40,90]");

-- 35. KNN using Vector Index (Single PK)
--QUERY PLAN
--Project
--  ->  Sort
--        Sort Key: l2_distance(tbl.embedding, cast('[120,51,70]' AS VECF32)) ASC
--        Limit: 3
--        ->  Join
--              Join Type: INNER   hashOnPK
--              Join Cond: (cast(#[0,0] AS BIGINT) = tbl.id)
--              ->  Join
--                    Join Type: INNER
--                    Join Cond: (#[0,0] = #[1,0])
--                    ->  Project
--                          ->  Join
--                                Join Type: SINGLE
--                                Join Cond: (__mo_index_secondary_018db491-b45f-7ea7-a779-a350abdc16fe.__mo_index_centroid_fk_version = #[1,0])
--                                ->  Table Scan on vecdb3.__mo_index_secondary_018db491-b45f-7ea7-a779-a350abdc16fe
--                                ->  Project
--                                      ->  Filter
--                                            Filter Cond: (__mo_index_secondary_018db491-b45f-724d-9dea-3bbf4d8104bf.__mo_index_key = 'version')
--                                            ->  Table Scan on vecdb3.__mo_index_secondary_018db491-b45f-724d-9dea-3bbf4d8104bf
--                    ->  Sort
--                          Sort Key: #[0,1] ASC
--                          Limit: CASE WHEN (@probe_limit IS NULL) THEN 1 ELSE cast(@probe_limit AS BIGINT) END
--                          ->  Project
--                                ->  Join
--                                      Join Type: SINGLE
--                                      Join Cond: (__mo_index_secondary_018db491-b45f-729f-8873-3a419dc30842.__mo_index_centroid_version = #[1,0])
--                                      ->  Table Scan on vecdb3.__mo_index_secondary_018db491-b45f-729f-8873-3a419dc30842
--                                      ->  Project
--                                            ->  Filter
--                                                  Filter Cond: (__mo_index_secondary_018db491-b45f-724d-9dea-3bbf4d8104bf.__mo_index_key = 'version')
--                                                  ->  Table Scan on vecdb3.__mo_index_secondary_018db491-b45f-724d-9dea-3bbf4d8104bf
--              ->  Table Scan on vecdb3.tbl
drop table if exists tbl;
SET @PROBE_LIMIT=1;
create table tbl(id int PRIMARY KEY, embedding vecf32(3));
insert into tbl values(1, "[1,2,3]");
insert into tbl values(2, "[1,2,4]");
insert into tbl values(3, "[1,2.4,4]");
insert into tbl values(4, "[1,2,5]");
insert into tbl values(5, "[1,3,5]");
insert into tbl values(6, "[100,44,50]");
insert into tbl values(7, "[120,50,70]");
insert into tbl values(8, "[130,40,90]");
SELECT id, embedding FROM tbl ORDER BY l2_distance(embedding,'[120,51,70]') ASC LIMIT 3;
create index idx using ivfflat on tbl(embedding) lists=2 op_type "vector_l2_ops";
SELECT id, embedding FROM tbl ORDER BY l2_distance(embedding,'[120,51,70]') ASC LIMIT 3;

-- 36. KNN using Vector Index (No PK)
drop table if exists tbl;
create table tbl(id int, embedding vecf32(3));
insert into tbl values(1, "[1,2,3]");
insert into tbl values(2, "[1,2,4]");
insert into tbl values(3, "[1,2.4,4]");
insert into tbl values(4, "[1,2,5]");
insert into tbl values(5, "[1,3,5]");
insert into tbl values(6, "[100,44,50]");
insert into tbl values(7, "[120,50,70]");
insert into tbl values(8, "[130,40,90]");
SELECT id, embedding FROM tbl ORDER BY l2_distance(embedding,'[120,51,70]') ASC LIMIT 3;
create index idx using ivfflat on tbl(embedding) lists=2 op_type "vector_l2_ops";
SELECT id, embedding FROM tbl ORDER BY l2_distance(embedding,'[120,51,70]') ASC LIMIT 3;

-- 37. KNN using Vector Index (2 PK)
drop table if exists tbl;
create table tbl(id int, id2 int, embedding vecf32(3), primary key(id, id2));
insert into tbl values(1, 0,"[1,2,3]");
insert into tbl values(2, 0, "[1,2,4]");
insert into tbl values(3, 0,"[1,2.4,4]");
insert into tbl values(4, 0, "[1,2,5]");
insert into tbl values(5, 0, "[1,3,5]");
insert into tbl values(6, 0, "[100,44,50]");
insert into tbl values(7, 0, "[120,50,70]");
insert into tbl values(8, 0, "[130,40,90]");
SELECT id,id2, embedding FROM tbl ORDER BY l2_distance(embedding,'[120,51,70]') ASC LIMIT 3;
create index idx using ivfflat on tbl(embedding) lists=2 op_type "vector_l2_ops";
SELECT id,id2, embedding FROM tbl ORDER BY l2_distance(embedding,'[120,51,70]') ASC LIMIT 3;
SET @test_limit = 3;
SELECT id from tbl LIMIT @test_limit;

-- 38. Alter Reindex on Empty Table
create table vector_index_05(a int primary key, b vecf32(3),c vecf32(4));
create index idx01 using ivfflat on vector_index_05(c) lists=4 op_type "vector_l2_ops";
show create table vector_index_05;
alter table vector_index_05 alter reindex idx01 ivfflat lists=5;
show create table vector_index_05;

-- 39. Alter Reindex Verify List update on `Show Create Table`
create table vector_index_04(a int primary key, b vecf32(3),c vecf32(4));
insert into vector_index_04 values(1,"[56,23,6]","[0.25,0.14,0.88,0.0001]"),(2,"[77,45,3]","[1.25,5.25,8.699,4.25]"),(3,"[8,56,3]","[9.66,5.22,1.22,7.02]");
create index idx01 using ivfflat on vector_index_04(c) lists=5 op_type "vector_l2_ops";
insert into vector_index_04 values(4,"[156,213,61]","[10.25,0.14,0.88,10.0001]"),(5,"[177,425,30]","[11.25,51.25,80.699,44.25]"),(6,"[80,56,3]","[90.686,5.212,19.22,7.02]");
show create table vector_index_04;
alter table vector_index_04 alter reindex idx01 ivfflat lists=8;
show create table vector_index_04;

-- 40. Add Index and Alter table add column
create table vector_index_08(a int primary key, b vecf32(128),c int,key c_k(c));
create index idx01 using ivfflat on vector_index_08(b) lists=3 op_type "vector_l2_ops";
alter table vector_index_08 add column d vecf32(3) not null after c;

-- 41. Create Index with no lists argument. However lists=0 will fail.
create table vector_index_07(a int primary key, b vecf32(128),c int,key c_k(c));
create index idx01 using ivfflat on vector_index_07(b);
create index idx02 using ivfflat on vector_index_07(b) lists=0;
alter table vector_index_07 reindex idx01 ivfflat lists=0;
alter table vector_index_07 reindex idx01 ivfflat;

-- 42. Create Index on Table with Less than List's row count. Then call alter reindex.
create table vector_index_09(a int primary key, b vecf32(128),c int,key c_k(c));
insert into vector_index_09 values(9774 ,NULL,3),(9775,NULL,10);
insert into vector_index_09(a,c) values(9777,4),(9778,9);
create index idx01 using ivfflat on vector_index_09(b) lists=3 op_type "vector_l2_ops";
select * from vector_index_09 order by L2_DISTANCE(b,"[1, 0, 1, 6, 6, 17, 47]");
insert into vector_index_09 values(97741 ,"[1, 0, 1, 6, 6, 17, 47, 39, 2, 0, 1, 25, 27, 10, 56, 130, 18, 5, 2, 6, 15, 2, 19, 130, 42, 28, 1, 1, 2, 1, 0, 5, 0, 2, 4, 4, 31, 34, 44, 35, 9, 3, 8, 11, 33, 12, 61, 130, 130, 17, 0, 1, 6, 2, 9, 130, 111, 36, 0, 0, 11, 9, 1, 12, 2, 100, 130, 28, 7, 2, 6, 7, 9, 27, 130, 83, 5, 0, 1, 18, 130, 130, 84, 9, 0, 0, 2, 24, 111, 24, 0, 1, 37, 24, 2, 10, 12, 62, 33, 3, 0, 0, 0, 1, 3, 16, 106, 28, 0, 0, 0, 0, 17, 46, 85, 10, 0, 0, 1, 4, 11, 4, 2, 2, 9, 14, 8, 8]",3),(97751,"[0, 1, 1, 3, 0, 3, 46, 20, 1, 4, 17, 9, 1, 17, 108, 15, 0, 3, 37, 17, 6, 15, 116, 16, 6, 1, 4, 7, 7, 7, 9, 6, 0, 8, 10, 4, 26, 129, 27, 9, 0, 0, 5, 2, 11, 129, 129, 12, 103, 4, 0, 0, 2, 31, 129, 129, 94, 4, 0, 0, 0, 3, 13, 42, 0, 15, 38, 2, 70, 129, 1, 0, 5, 10, 40, 12, 74, 129, 6, 1, 129, 39, 6, 1, 2, 22, 9, 33, 122, 13, 0, 0, 0, 0, 5, 23, 4, 11, 9, 12, 45, 38, 1, 0, 0, 4, 36, 38, 57, 32, 0, 0, 82, 22, 9, 5, 13, 11, 3, 94, 35, 3, 0, 0, 0, 1, 16, 97]",5),(9776,"[10, 3, 8, 5, 48, 26, 5, 16, 17, 0, 0, 2, 132, 53, 1, 16, 112, 6, 0, 0, 7, 2, 1, 48, 48, 15, 18, 31, 3, 0, 0, 9, 6, 10, 19, 27, 50, 46, 17, 9, 18, 1, 4, 48, 132, 23, 3, 5, 132, 9, 4, 3, 11, 0, 2, 46, 84, 12, 10, 10, 1, 0, 12, 76, 26, 22, 16, 26, 35, 15, 3, 16, 15, 1, 51, 132, 125, 8, 1, 2, 132, 51, 67, 91, 8, 0, 0, 30, 126, 39, 32, 38, 4, 0, 1, 12, 24, 2, 2, 2, 4, 7, 2, 19, 93, 19, 70, 92, 2, 3, 1, 21, 36, 58, 132, 94, 0, 0, 0, 0, 21, 25, 57, 48, 1, 0, 0, 1]",3);
insert into vector_index_09 values(97771, "[16, 15, 0, 0, 5, 46, 5, 5, 4, 0, 0, 0, 28, 118, 12, 5, 75, 44, 5, 0, 6, 32, 6, 49, 41, 74, 9, 1, 0, 0, 0, 9, 1, 9, 16, 41, 71, 80, 3, 0, 0, 4, 3, 5, 51, 106, 11, 3, 112, 28, 13, 1, 4, 8, 3, 104, 118, 14, 1, 1, 0, 0, 0, 88, 3, 27, 46, 118, 108, 49, 2, 0, 1, 46, 118, 118, 27, 12, 0, 0, 33, 118, 118, 8, 0, 0, 0, 4, 118, 95, 40, 0, 0, 0, 1, 11, 27, 38, 12, 12, 18, 29, 3, 2, 13, 30, 94, 78, 30, 19, 9, 3, 31, 45, 70, 42, 15, 1, 3, 12, 14, 22, 16, 2, 3, 17, 24, 13]",4),(97781,"[41, 0, 0, 7, 1, 1, 20, 67, 9, 0, 0, 0, 0, 31, 120, 61, 25, 0, 0, 0, 0, 10, 120, 90, 32, 0, 0, 1, 13, 11, 22, 50, 4, 0, 2, 93, 40, 15, 37, 18, 12, 2, 2, 19, 8, 44, 120, 25, 120, 5, 0, 0, 0, 2, 48, 97, 102, 14, 3, 3, 11, 9, 34, 41, 0, 0, 4, 120, 56, 3, 4, 5, 6, 15, 37, 116, 28, 0, 0, 3, 120, 120, 24, 6, 2, 0, 1, 28, 53, 90, 51, 11, 11, 2, 12, 14, 8, 6, 4, 30, 9, 1, 4, 22, 25, 79, 120, 66, 5, 0, 0, 6, 42, 120, 91, 43, 15, 2, 4, 39, 12, 9, 9, 12, 15, 5, 24, 36]",4);
alter table vector_index_09 alter reindex idx01 ivfflat lists=2;

-- 43. Invalid vector inside L2_distance Query.
drop table if exists vector_index_07;
create table vector_index_07(a int primary key, b vecf32(128),c int,key c_k(c));
create index idx01 using ivfflat on vector_index_07(b) lists=5 op_type "vector_l2_ops";
insert into vector_index_07 values(9774 ,"[1, 0, 1, 6, 6, 17, 47, 39, 2, 0, 1, 25, 27, 10, 56, 130, 18, 5, 2, 6, 15, 2, 19, 130, 42, 28, 1, 1, 2, 1, 0, 5, 0, 2, 4, 4, 31, 34, 44, 35, 9, 3, 8, 11, 33, 12, 61, 130, 130, 17, 0, 1, 6, 2, 9, 130, 111, 36, 0, 0, 11, 9, 1, 12, 2, 100, 130, 28, 7, 2, 6, 7, 9, 27, 130, 83, 5, 0, 1, 18, 130, 130, 84, 9, 0, 0, 2, 24, 111, 24, 0, 1, 37, 24, 2, 10, 12, 62, 33, 3, 0, 0, 0, 1, 3, 16, 106, 28, 0, 0, 0, 0, 17, 46, 85, 10, 0, 0, 1, 4, 11, 4, 2, 2, 9, 14, 8, 8]",3),(9775,"[0, 1, 1, 3, 0, 3, 46, 20, 1, 4, 17, 9, 1, 17, 108, 15, 0, 3, 37, 17, 6, 15, 116, 16, 6, 1, 4, 7, 7, 7, 9, 6, 0, 8, 10, 4, 26, 129, 27, 9, 0, 0, 5, 2, 11, 129, 129, 12, 103, 4, 0, 0, 2, 31, 129, 129, 94, 4, 0, 0, 0, 3, 13, 42, 0, 15, 38, 2, 70, 129, 1, 0, 5, 10, 40, 12, 74, 129, 6, 1, 129, 39, 6, 1, 2, 22, 9, 33, 122, 13, 0, 0, 0, 0, 5, 23, 4, 11, 9, 12, 45, 38, 1, 0, 0, 4, 36, 38, 57, 32, 0, 0, 82, 22, 9, 5, 13, 11, 3, 94, 35, 3, 0, 0, 0, 1, 16, 97]",5),(9776,"[10, 3, 8, 5, 48, 26, 5, 16, 17, 0, 0, 2, 132, 53, 1, 16, 112, 6, 0, 0, 7, 2, 1, 48, 48, 15, 18, 31, 3, 0, 0, 9, 6, 10, 19, 27, 50, 46, 17, 9, 18, 1, 4, 48, 132, 23, 3, 5, 132, 9, 4, 3, 11, 0, 2, 46, 84, 12, 10, 10, 1, 0, 12, 76, 26, 22, 16, 26, 35, 15, 3, 16, 15, 1, 51, 132, 125, 8, 1, 2, 132, 51, 67, 91, 8, 0, 0, 30, 126, 39, 32, 38, 4, 0, 1, 12, 24, 2, 2, 2, 4, 7, 2, 19, 93, 19, 70, 92, 2, 3, 1, 21, 36, 58, 132, 94, 0, 0, 0, 0, 21, 25, 57, 48, 1, 0, 0, 1]",3);
insert into vector_index_07 values(9777, " [16, 15, 0, 0, 5, 46, 5, 5, 4, 0, 0, 0, 28, 118, 12, 5, 75, 44, 5, 0, 6, 32, 6, 49, 41, 74, 9, 1, 0, 0, 0, 9, 1, 9, 16, 41, 71, 80, 3, 0, 0, 4, 3, 5, 51, 106, 11, 3, 112, 28, 13, 1, 4, 8, 3, 104, 118, 14, 1, 1, 0, 0, 0, 88, 3, 27, 46, 118, 108, 49, 2, 0, 1, 46, 118, 118, 27, 12, 0, 0, 33, 118, 118, 8, 0, 0, 0, 4, 118, 95, 40, 0, 0, 0, 1, 11, 27, 38, 12, 12, 18, 29, 3, 2, 13, 30, 94, 78, 30, 19, 9, 3, 31, 45, 70, 42, 15, 1, 3, 12, 14, 22, 16, 2, 3, 17, 24, 13]",4),(9778,"[41, 0, 0, 7, 1, 1, 20, 67, 9, 0, 0, 0, 0, 31, 120, 61, 25, 0, 0, 0, 0, 10, 120, 90, 32, 0, 0, 1, 13, 11, 22, 50, 4, 0, 2, 93, 40, 15, 37, 18, 12, 2, 2, 19, 8, 44, 120, 25, 120, 5, 0, 0, 0, 2, 48, 97, 102, 14, 3, 3, 11, 9, 34, 41, 0, 0, 4, 120, 56, 3, 4, 5, 6, 15, 37, 116, 28, 0, 0, 3, 120, 120, 24, 6, 2, 0, 1, 28, 53, 90, 51, 11, 11, 2, 12, 14, 8, 6, 4, 30, 9, 1, 4, 22, 25, 79, 120, 66, 5, 0, 0, 6, 42, 120, 91, 43, 15, 2, 4, 39, 12, 9, 9, 12, 15, 5, 24, 36]",4);
select * from vector_index_07 order by  L2_DISTANCE(b, "abc") ASC LIMIT 2;

-- 44. Auto Increment PK type
drop table if exists vector_index_08;
create table vector_index_08(a int auto_increment primary key, b vecf32(128),c int,key c_k(c));
insert into vector_index_08 values(9774 ,"[1, 0, 1, 6, 6, 17, 47, 39, 2, 0, 1, 25, 27, 10, 56, 130, 18, 5, 2, 6, 15, 2, 19, 130, 42, 28, 1, 1, 2, 1, 0, 5, 0, 2, 4, 4, 31, 34, 44, 35, 9, 3, 8, 11, 33, 12, 61, 130, 130, 17, 0, 1, 6, 2, 9, 130, 111, 36, 0, 0, 11, 9, 1, 12, 2, 100, 130, 28, 7, 2, 6, 7, 9, 27, 130, 83, 5, 0, 1, 18, 130, 130, 84, 9, 0, 0, 2, 24, 111, 24, 0, 1, 37, 24, 2, 10, 12, 62, 33, 3, 0, 0, 0, 1, 3, 16, 106, 28, 0, 0, 0, 0, 17, 46, 85, 10, 0, 0, 1, 4, 11, 4, 2, 2, 9, 14, 8, 8]",3),(9775,"[0, 1, 1, 3, 0, 3, 46, 20, 1, 4, 17, 9, 1, 17, 108, 15, 0, 3, 37, 17, 6, 15, 116, 16, 6, 1, 4, 7, 7, 7, 9, 6, 0, 8, 10, 4, 26, 129, 27, 9, 0, 0, 5, 2, 11, 129, 129, 12, 103, 4, 0, 0, 2, 31, 129, 129, 94, 4, 0, 0, 0, 3, 13, 42, 0, 15, 38, 2, 70, 129, 1, 0, 5, 10, 40, 12, 74, 129, 6, 1, 129, 39, 6, 1, 2, 22, 9, 33, 122, 13, 0, 0, 0, 0, 5, 23, 4, 11, 9, 12, 45, 38, 1, 0, 0, 4, 36, 38, 57, 32, 0, 0, 82, 22, 9, 5, 13, 11, 3, 94, 35, 3, 0, 0, 0, 1, 16, 97]",5),(9776,"[10, 3, 8, 5, 48, 26, 5, 16, 17, 0, 0, 2, 132, 53, 1, 16, 112, 6, 0, 0, 7, 2, 1, 48, 48, 15, 18, 31, 3, 0, 0, 9, 6, 10, 19, 27, 50, 46, 17, 9, 18, 1, 4, 48, 132, 23, 3, 5, 132, 9, 4, 3, 11, 0, 2, 46, 84, 12, 10, 10, 1, 0, 12, 76, 26, 22, 16, 26, 35, 15, 3, 16, 15, 1, 51, 132, 125, 8, 1, 2, 132, 51, 67, 91, 8, 0, 0, 30, 126, 39, 32, 38, 4, 0, 1, 12, 24, 2, 2, 2, 4, 7, 2, 19, 93, 19, 70, 92, 2, 3, 1, 21, 36, 58, 132, 94, 0, 0, 0, 0, 21, 25, 57, 48, 1, 0, 0, 1]",3);
insert into vector_index_08 values(9777, "[16, 15, 0, 0, 5, 46, 5, 5, 4, 0, 0, 0, 28, 118, 12, 5, 75, 44, 5, 0, 6, 32, 6, 49, 41, 74, 9, 1, 0, 0, 0, 9, 1, 9, 16, 41, 71, 80, 3, 0, 0, 4, 3, 5, 51, 106, 11, 3, 112, 28, 13, 1, 4, 8, 3, 104, 118, 14, 1, 1, 0, 0, 0, 88, 3, 27, 46, 118, 108, 49, 2, 0, 1, 46, 118, 118, 27, 12, 0, 0, 33, 118, 118, 8, 0, 0, 0, 4, 118, 95, 40, 0, 0, 0, 1, 11, 27, 38, 12, 12, 18, 29, 3, 2, 13, 30, 94, 78, 30, 19, 9, 3, 31, 45, 70, 42, 15, 1, 3, 12, 14, 22, 16, 2, 3, 17, 24, 13]",4),(9778,"[41, 0, 0, 7, 1, 1, 20, 67, 9, 0, 0, 0, 0, 31, 120, 61, 25, 0, 0, 0, 0, 10, 120, 90, 32, 0, 0, 1, 13, 11, 22, 50, 4, 0, 2, 93, 40, 15, 37, 18, 12, 2, 2, 19, 8, 44, 120, 25, 120, 5, 0, 0, 0, 2, 48, 97, 102, 14, 3, 3, 11, 9, 34, 41, 0, 0, 4, 120, 56, 3, 4, 5, 6, 15, 37, 116, 28, 0, 0, 3, 120, 120, 24, 6, 2, 0, 1, 28, 53, 90, 51, 11, 11, 2, 12, 14, 8, 6, 4, 30, 9, 1, 4, 22, 25, 79, 120, 66, 5, 0, 0, 6, 42, 120, 91, 43, 15, 2, 4, 39, 12, 9, 9, 12, 15, 5, 24, 36]",4);
create index idx01 using ivfflat on vector_index_08(b) lists=3 op_type "vector_l2_ops";
select * from  vector_index_08 ;
update vector_index_08 set b="[16, 15, 0, 0, 5, 46, 5, 5, 4, 0, 0, 0, 28, 118, 12, 5, 75, 44, 5, 0, 6, 32, 6, 49, 41, 74, 9, 1, 0, 0, 0, 9, 1, 9, 16, 41, 71, 80, 3, 0, 0, 4, 3, 5, 51, 106, 11, 3, 112, 28, 13, 1, 4, 8, 3, 104, 118, 14, 1, 1, 0, 0, 0, 88, 3, 27, 46, 118, 108, 49, 2, 0, 1, 46, 118, 118, 27, 12, 0, 0, 33, 118, 118, 8, 0, 0, 0, 4, 118, 95, 40, 0, 0, 0, 1, 11, 27, 38, 12, 12, 18, 29, 3, 2, 13, 30, 94, 78, 30, 19, 9, 3, 31, 45, 70, 42, 15, 1, 3, 12, 14, 22, 16, 2, 3, 17, 24, 13]" where a=9774;
select * from  vector_index_08 where a=9774;
delete from vector_index_08 where a=9777;
select * from  vector_index_08 where a=9777;
truncate table vector_index_08;
select * from  vector_index_08;
insert into vector_index_08 values(9774 ,"[1, 0, 1, 6, 6, 17, 47, 39, 2, 0, 1, 25, 27, 10, 56, 130, 18, 5, 2, 6, 15, 2, 19, 130, 42, 28, 1, 1, 2, 1, 0, 5, 0, 2, 4, 4, 31, 34, 44, 35, 9, 3, 8, 11, 33, 12, 61, 130, 130, 17, 0, 1, 6, 2, 9, 130, 111, 36, 0, 0, 11, 9, 1, 12, 2, 100, 130, 28, 7, 2, 6, 7, 9, 27, 130, 83, 5, 0, 1, 18, 130, 130, 84, 9, 0, 0, 2, 24, 111, 24, 0, 1, 37, 24, 2, 10, 12, 62, 33, 3, 0, 0, 0, 1, 3, 16, 106, 28, 0, 0, 0, 0, 17, 46, 85, 10, 0, 0, 1, 4, 11, 4, 2, 2, 9, 14, 8, 8]",3),(9775,"[0, 1, 1, 3, 0, 3, 46, 20, 1, 4, 17, 9, 1, 17, 108, 15, 0, 3, 37, 17, 6, 15, 116, 16, 6, 1, 4, 7, 7, 7, 9, 6, 0, 8, 10, 4, 26, 129, 27, 9, 0, 0, 5, 2, 11, 129, 129, 12, 103, 4, 0, 0, 2, 31, 129, 129, 94, 4, 0, 0, 0, 3, 13, 42, 0, 15, 38, 2, 70, 129, 1, 0, 5, 10, 40, 12, 74, 129, 6, 1, 129, 39, 6, 1, 2, 22, 9, 33, 122, 13, 0, 0, 0, 0, 5, 23, 4, 11, 9, 12, 45, 38, 1, 0, 0, 4, 36, 38, 57, 32, 0, 0, 82, 22, 9, 5, 13, 11, 3, 94, 35, 3, 0, 0, 0, 1, 16, 97]",5),(9776,"[10, 3, 8, 5, 48, 26, 5, 16, 17, 0, 0, 2, 132, 53, 1, 16, 112, 6, 0, 0, 7, 2, 1, 48, 48, 15, 18, 31, 3, 0, 0, 9, 6, 10, 19, 27, 50, 46, 17, 9, 18, 1, 4, 48, 132, 23, 3, 5, 132, 9, 4, 3, 11, 0, 2, 46, 84, 12, 10, 10, 1, 0, 12, 76, 26, 22, 16, 26, 35, 15, 3, 16, 15, 1, 51, 132, 125, 8, 1, 2, 132, 51, 67, 91, 8, 0, 0, 30, 126, 39, 32, 38, 4, 0, 1, 12, 24, 2, 2, 2, 4, 7, 2, 19, 93, 19, 70, 92, 2, 3, 1, 21, 36, 58, 132, 94, 0, 0, 0, 0, 21, 25, 57, 48, 1, 0, 0, 1]",3);
alter table vector_index_08 add column d vecf32(3) not null after c;
select * from vector_index_08;

-- 45. Auto Incr PK type vs non-auto Incr PK type
drop table if exists vector_index_08;
create table vector_index_08(a int primary key, b vecf32(3),c int);
insert into vector_index_08 values(1 ,"[1, 2, 3]",11);
insert into vector_index_08 values(2 ,"[1, 2, 3]",12);
insert into vector_index_08 values(3 ,"[1, 2, 3]",13);
create index idx01 using ivfflat on vector_index_08(b)  lists = 2 op_type 'vector_l2_ops';
alter table vector_index_08 add column d vecf32(3) not null after c;
select * from vector_index_08;

drop table if exists vector_index_08;
create table vector_index_08(a int auto_increment primary key, b vecf32(3),c int);
insert into vector_index_08 values(1 ,"[1, 2, 3]",11);
insert into vector_index_08 values(2 ,"[1, 2, 3]",12);
insert into vector_index_08 values(3 ,"[1, 2, 3]",13);
create index idx01 using ivfflat on vector_index_08(b)  lists = 2 op_type 'vector_l2_ops';
alter table vector_index_08 add column d vecf32(3) not null after c;
select * from vector_index_08;

-- post
drop database vecdb2;