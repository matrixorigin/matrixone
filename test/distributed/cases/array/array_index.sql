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

-- post
drop database vecdb2;