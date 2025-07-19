DROP DATABASE IF EXISTS db1;
create database db1;
use db1;

drop table if exists t1;
create table t1(id int PRIMARY KEY,name VARCHAR(255),age int);
insert into t1 values(1,"Abby", 24);
insert into t1 values(2,"Bob", 25);
insert into t1 values(3,"Carol", 23);
insert into t1 values(4,"Dora", 29);
create unique index idx1 on t1(name);
select * from t1;
show index from t1;

select name, type, column_name from mo_catalog.mo_indexes where name = 'idx1' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 't1');
select relkind from mo_catalog.mo_tables where relname in (select distinct index_table_name from mo_catalog.mo_indexes where name = 'idx1' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 't1'));

create index idx2 on t1(name, age);
select * from t1;
show index from t1;
select name, type, column_name from mo_catalog.mo_indexes where name = 'idx2' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 't1');
select relkind from mo_catalog.mo_tables where relname in (select distinct index_table_name from mo_catalog.mo_indexes where name = 'idx2' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 't1'));

alter table t1 add column sal int default 2000;
select name, type, column_name from mo_catalog.mo_indexes where name = 'idx1' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 't1');
select relkind from mo_catalog.mo_tables where relname in (select distinct index_table_name from mo_catalog.mo_indexes where name = 'idx1' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 't1'));

select name, type, column_name from mo_catalog.mo_indexes where name = 'idx2' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 't1');
select relkind from mo_catalog.mo_tables where relname in (select distinct index_table_name from mo_catalog.mo_indexes where name = 'idx2' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 't1'));


SET experimental_ivf_index = 1;
-- 10. Create IVF index within Create Table (this will create empty index hidden tables)
drop table if exists t2;
create table t2(a int primary key, b vecf32(3), index idx9 using ivfflat (b));

select * from t2;
show index from t2;
select name, type, column_name from mo_catalog.mo_indexes where name = 'idx9' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 't2');
select relkind from mo_catalog.mo_tables where relname in (select distinct index_table_name from mo_catalog.mo_indexes where name = 'idx9' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 't2'));

-- 11. Delete column having IVFFLAT index
drop table if exists t3;
create table t3(a int primary key, b vecf32(3), index idx10 using ivfflat (b));
insert into t3 values(1, "[1,2,3]");
insert into t3 values(2, "[1,2,4]");

show index from t3;
show create table t3;
select name, type, column_name from mo_catalog.mo_indexes where name = 'idx10' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 't3');
select relkind from mo_catalog.mo_tables where relname in (select distinct index_table_name from mo_catalog.mo_indexes where name = 'idx10' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 't3'));

alter table t3 drop column b;
show index from t3;
show create table t3;
select name, type, column_name from mo_catalog.mo_indexes where name = 'idx10' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 't3');
select relkind from mo_catalog.mo_tables where relname in (select distinct index_table_name from mo_catalog.mo_indexes where name = 'idx10' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 't3'));

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
select name, type, column_name, algo, algo_table_type,algo_params from mo_catalog.mo_indexes where name = 'idx1' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 'tbl');
select relkind from mo_catalog.mo_tables where relname in (select distinct index_table_name from mo_catalog.mo_indexes where name = 'idx1' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 'tbl'));

-- 4. Reindex Secondary Index with IVFFLAT.
alter table tbl alter reindex idx1 ivfflat lists=3;
show index from tbl;
show create table tbl;
select name, type, column_name, algo, algo_table_type,algo_params from mo_catalog.mo_indexes where name = 'idx1' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 'tbl');
select relkind from mo_catalog.mo_tables where relname in (select distinct index_table_name from mo_catalog.mo_indexes where name = 'idx1' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 'tbl'));

-- 5. Alter table add column with IVFFLAT.
alter table tbl add c vecf32(3);
show index from tbl;
show create table tbl;
select name, type, column_name, algo, algo_table_type,algo_params from mo_catalog.mo_indexes where name = 'idx1' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 'tbl');
select relkind from mo_catalog.mo_tables where relname in (select distinct index_table_name from mo_catalog.mo_indexes where name = 'idx1' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 'tbl'));

-- 6.  Create IVF index before table has data (we create the 3 hidden tables alone without populating them)
drop table if exists tbl;
create table tbl(a int primary key,b vecf32(3), c vecf64(5));
create index idx2 using IVFFLAT on tbl(b) lists = 2 op_type 'vector_l2_ops';

show index from tbl;
show create table tbl;

select name, type, column_name, algo, algo_table_type,algo_params from mo_catalog.mo_indexes where name = 'idx2' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 'tbl');
select relkind from mo_catalog.mo_tables where relname in (select distinct index_table_name from mo_catalog.mo_indexes where name = 'idx2' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 'tbl'));

-------------------------------------------------------------------------------------------------------------------
set experimental_fulltext_index=1;
set ft_relevancy_algorithm="TF-IDF";

create table src1 (id bigint primary key, body varchar, title text);
insert into src1 values (0, 'color is red', 't1'), (1, 'car is yellow', 'crazy car'), (2, 'sky is blue', 'no limit'), (3, 'blue is not red', 'colorful'),
                        (4, '遠東兒童中文是針對6到9歲的小朋友精心設計的中文學習教材，共三冊，目前已出版一、二冊。', '遠東兒童中文'),
                        (5, '每冊均採用近百張全幅彩圖及照片，生動活潑、自然真實，加深兒童學習印象，洋溢學習樂趣。', '遠東兒童中文'),
                        (6, '各個單元主題內容涵蓋中華文化及生活應用的介紹。本套教材含課本、教學指引、生字卡、學生作業本與CD，中英對照，精美大字版。本系列有繁體字及簡體字兩種版本印行。', '中文短篇小說'),
                        (7, '59個簡單的英文和中文短篇小說', '適合初學者'),
                        (8, NULL, 'NOT INCLUDED'),
                        (9, 'NOT INCLUDED BODY', NULL),
                        (10, NULL, NULL);

create fulltext index ftidx on src1 (body, title);
show index from src1;
select name, type, column_name from mo_catalog.mo_indexes where name = 'ftidx' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 'src1');
select relkind from mo_catalog.mo_tables where relname in (select distinct index_table_name from mo_catalog.mo_indexes where name = 'ftidx' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 'src1'));

-- add index for body column
alter table src1 add fulltext index ftidx2 (body);
show index from src1;
select name, type, column_name from mo_catalog.mo_indexes where name = 'ftidx2' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 'src1');
select relkind from mo_catalog.mo_tables where relname in (select distinct index_table_name from mo_catalog.mo_indexes where name = 'ftidx2' and table_id in (select rel_id from mo_catalog.mo_tables where relname = 'src1'));

select * from src1 where match(body, title) against('red');
select *, match(body, title) against('is red' in natural language mode) as score from src1;
select * from src1 where match(body, title) against('教學指引');
select * from src1 where match(body, title) against('彩圖' in natural language mode);
select * from src1 where match(body, title) against('遠東' in natural language mode);
select * from src1 where match(body, title) against('版一、二冊' in natural language mode);
drop table src1;

create table src2 (id bigint primary key, body varchar, title text, FULLTEXT(title, body));
insert into src2 values (0, 'color is red', 't1'), (1, 'car is yellow', 'crazy car'), (2, 'sky is blue', 'no limit'), (3, 'blue is not red', 'colorful'),
                        (4, '遠東兒童中文是針對6到9歲的小朋友精心設計的中文學習教材，共三冊，目前已出版一、二冊。', '遠東兒童中文'),
                        (5, '每冊均採用近百張全幅彩圖及照片，生動活潑、自然真實，加深兒童學習印象，洋溢學習樂趣。', '遠東兒童中文'),
                        (6, '各個單元主題內容涵蓋中華文化及生活應用的介紹。本套教材含課本、教學指引、生字卡、學生作業本與CD，中英對照，精美大字版。本系列有繁體字及簡體字兩種版本印行。', '中文短篇小說
'),
                        (7, '59個簡單的英文和中文短篇小說', '適合初學者'),
                        (8, NULL, 'NOT INCLUDED'),
                        (9, 'NOT INCLUDED BODY', NULL),
                        (10, NULL, NULL);


show index from src2;
select name, type, column_name from mo_catalog.mo_indexes where table_id in (select rel_id from mo_catalog.mo_tables where relname = 'src2');
select relkind from mo_catalog.mo_tables where relname in (select distinct index_table_name from mo_catalog.mo_indexes where table_id in (select rel_id from mo_catalog.mo_tables where relname = 'src2') and name != 'PRIMARY');

select * from src2 where match(body, title) against('red');
select *, match(body, title) against('is red' in natural language mode) as score from src2;
select * from src2 where match(body, title) against('教學指引');
select * from src2 where match(body, title) against('彩圖' in natural language mode);
select * from src2 where match(body, title) against('遠東' in natural language mode);
select * from src2 where match(body, title) against('版一、二冊' in natural language mode);
select *, match(body, title) against('遠東兒童中文' in natural language mode) as score from src2;
drop table src2;

DROP DATABASE IF EXISTS db1;