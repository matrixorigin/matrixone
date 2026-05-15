-- gojieba parser BVT.
-- Mirrors the default simple-tokenizer coverage in fulltext.sql / fulltext_bm25.sql,
-- but builds and queries the index through gojieba (Chinese word segmentation).

set ft_relevancy_algorithm="TF-IDF";

create table src (id bigint primary key, body varchar, title text);

insert into src values (0, 'color is red', 't1'), (1, 'car is yellow', 'crazy car'), (2, 'sky is blue', 'no limit'), (3, 'blue is not red', 'colorful'),
(4, '遠東兒童中文是針對6到9歲的小朋友精心設計的中文學習教材，共三冊，目前已出版一、二冊。', '遠東兒童中文'),
(5, '每冊均採用近百張全幅彩圖及照片，生動活潑、自然真實，加深兒童學習印象，洋溢學習樂趣。', '遠東兒童中文'),
(6, '各個單元主題內容涵蓋中華文化及生活應用的介紹。本套教材含課本、教學指引、生字卡、學生作業本與CD，中英對照，精美大字版。本系列有繁體字及簡體字兩種版本印行。', '中文短篇小說'),
(7, '59個簡單的英文和中文短篇小說', '適合初學者'),
(8, NULL, 'NOT INCLUDED'),
(9, 'NOT INCLUDED BODY', NULL),
(10, NULL, NULL);

create fulltext index ftidx on src (body, title) with parser gojieba;

-- check fulltext_match with index error
create fulltext index ftidx02 on src (body, title) with parser gojieba;
select * from src where match(body) against('red');
select * from src where match(body,title) against('+]]]');

select match(body) against('red') from src;

-- add index for body column
alter table src add fulltext index ftidx2 (body) with parser gojieba;
create fulltext index ftidx03 on src (body) with parser gojieba;
create fulltext index ftidx03 on src (body, title) with parser gojieba;

-- match in WHERE clause
select * from src where match(body, title) against('red');

select *, match(body, title) against('is red' in natural language mode) as score from src;

select * from src where match(body, title) against('教學指引');

select * from src where match(body, title) against('彩圖' in natural language mode);

select * from src where match(body, title) against('遠東' in natural language mode);

select * from src where match(body, title) against('版一、二冊' in natural language mode);

select *, match(body, title) against('遠東兒童中文' in natural language mode) as score from src;

select *, match(body) against('遠東兒童中文' in natural language mode) as score from src;

-- boolean mode
select * from src where match(body, title) against('+red blue' in boolean mode);

select * from src where match(body, title) against('re*' in boolean mode);

select * from src where match(body, title) against('+red -blue' in boolean mode);

select * from src where match(body, title) against('+red +blue' in boolean mode);

select * from src where match(body, title) against('+red ~blue' in boolean mode);

select * from src where match(body, title) against('+red -(<blue >is)' in boolean mode);

select * from src where match(body, title) against('+red +(<blue >is)' in boolean mode);

select * from src where match(body, title) against('"is not red"' in boolean mode);

select * from src where match(body, title) against('"red"' in boolean mode);

-- boolean mode in Chinese
select * from src where match(body, title) against('+教學指引 +短篇小說' in boolean mode);
select * from src where match(body, title) against('+教學指引 -短篇小說' in boolean mode);

-- phrase exact match.  double space cannot be matched and empty result
select * from src where match(body, title) against('"is  not red"' in boolean mode);

-- phrase exact match. all words match but not exact match
select * from src where match(body, title) against('"blue is red"' in boolean mode);

-- match in projection
select src.*, match(body, title) against('blue') from src;

-- match with Aggregate
select count(*) from src where match(title, body) against('red');

-- duplicate fulltext_match and compute once
-- @separator:table
explain select match(body, title) against('red') from src where match(body, title) against('red');

-- truncate
delete from src;

-- check empty index
select * from src where match(body) against('red');

select * from src where match(body, title) against('red');

-- insert data again

insert into src values (0, 'color is red', 't1'), (1, 'car is yellow', 'crazy car'), (2, 'sky is blue', 'no limit'), (3, 'blue is not red', 'colorful'),
(4, '遠東兒童中文是針對6到9歲的小朋友精心設計的中文學習教材，共三冊，目前已出版一、二冊。', '遠東兒童中文'),
(5, '每冊均採用近百張全幅彩圖及照片，生動活潑、自然真實，加深兒童學習印象，洋溢學習樂趣。', '遠東兒童中文'),
(6, '各個單元主題內容涵蓋中華文化及生活應用的介紹。本套教材含課本、教學指引、生字卡、學生作業本與CD，中英對照，精美大字版。本系列有繁體字及簡體字兩種版本印行。', '中文短篇小說'),
(7, '59個簡單的英文和中文短篇小說', '適合初學者'),
(8, NULL, 'NOT INCLUDED'),
(9, 'NOT INCLUDED BODY', NULL),
(10, NULL, NULL);

select * from src where match(body) against('red');

select match(body) against('red') from src;

-- match in WHERE clause
select * from src where match(body, title) against('red');

select *, match(body, title) against('is red' in natural language mode) as score from src;

select * from src where match(body, title) against('教學指引');

select * from src where match(body, title) against('彩圖' in natural language mode);

select * from src where match(body, title) against('遠東' in natural language mode);

select * from src where match(body, title) against('版一、二冊' in natural language mode);

select *, match(body, title) against('遠東兒童中文' in natural language mode) as score from src;

select *, match(body) against('遠東兒童中文' in natural language mode) as score from src;

-- boolean mode
select * from src where match(body, title) against('+red blue' in boolean mode);

select * from src where match(body, title) against('re*' in boolean mode);

select * from src where match(body, title) against('+red -blue' in boolean mode);

select * from src where match(body, title) against('+red +blue' in boolean mode);

select * from src where match(body, title) against('+red ~blue' in boolean mode);

select * from src where match(body, title) against('+red -(<blue >is)' in boolean mode);

select * from src where match(body, title) against('+red +(<blue >is)' in boolean mode);

select * from src where match(body, title) against('"is not red"' in boolean mode);

-- match in projection
select src.*, match(body, title) against('blue') from src;

-- match with Aggregate
select count(*) from src where match(title, body) against('red');

-- duplicate fulltext_match and compute once
-- @separator:table
explain select match(body, title) against('red') from src where match(body, title) against('red');

desc src;
show create table src;

drop table src;

-- composite primary key
create table src2 (id1 varchar, id2 bigint, body char(128), title text, primary key (id1, id2));

insert into src2 values ('id0', 0, 'red', 't1'), ('id1', 1, 'yellow', 't2'), ('id2', 2, 'blue', 't3'), ('id3', 3, 'blue red', 't4');

create fulltext index ftidx2 on src2 (body, title) with parser gojieba;
select * from src2 where match(body, title) against('red');
select src2.*, match(body, title) against('blue') from src2;

update src2 set body = 'orange' where id1='id0';

select * from src2 where match(body, title) against('red');

delete from src2 where id1='id3';

select * from src2 where match(body, title) against('t4');

insert into src2 values ('id4', 4, 'light brown', 't5');

select * from src2 where match(body, title) against('t5');

show create table src2;

drop table src2;

-- update/insert/delete with inline FULLTEXT clause
create table src (id bigint primary key, body varchar, title text, FULLTEXT(title, body) WITH PARSER gojieba);
insert into src values (0, 'color is red', 't1'), (1, 'car is yellow', 'crazy car'), (2, 'sky is blue', 'no limit'), (3, 'blue is not red', 'colorful'),
(4, '遠東兒童中文是針對6到9歲的小朋友精心設計的中文學習教材，共三冊，目前已出版一、二冊。', '遠東兒童中文'),
(5, '每冊均採用近百張全幅彩圖及照片，生動活潑、自然真實，加深兒童學習印象，洋溢學習樂趣。', '遠東兒童中文'),
(6, '各個單元主題內容涵蓋中華文化及生活應用的介紹。本套教材含課本、教學指引、生字卡、學生作業本與CD，中英對照，精美大字版。本系列有繁體字及簡體字兩種版本印行。', '中文短篇小說'),
(7, '59個簡單的英文和中文短篇小說', '適合初學者'),
(8, NULL, 'NOT INCLUDED'),
(9, 'NOT INCLUDED BODY', NULL),
(10, NULL, NULL);

-- check error
select *, match(body) against('遠東兒童中文' in natural language mode) as score from src;
select *, match(title, body) against('+Windows +(<"Photo" >defender)' in boolean mode) as score from src;
select *, match(title, body) against('+CC_BY +(<-1.0 >-SA-1.0)' in boolean mode) as score from src;

-- match in WHERE clause
select * from src where match(body, title) against('red');

select *, match(body, title) against('is red' in natural language mode) as score from src;

select * from src where match(body, title) against('教學指引');

select * from src where match(body, title) against('彩圖' in natural language mode);

select * from src where match(body, title) against('遠東' in natural language mode);

select * from src where match(body, title) against('版一、二冊' in natural language mode);

select *, match(body, title) against('遠東兒童中文' in natural language mode) as score from src;

-- boolean mode
select * from src where match(body, title) against('+red blue' in boolean mode);

select * from src where match(body, title) against('re*' in boolean mode);

select * from src where match(body, title) against('+red -blue' in boolean mode);

select * from src where match(body, title) against('+red +blue' in boolean mode);

select * from src where match(body, title) against('+red ~blue' in boolean mode);

select * from src where match(body, title) against('+red -(<blue >is)' in boolean mode);

select * from src where match(body, title) against('+red +(<blue >is)' in boolean mode);

select * from src where match(body, title) against('"is not red"' in boolean mode);

-- match in projection
select src.*, match(body, title) against('blue') from src;

-- match with Aggregate
select count(*) from src where match(title, body) against('red');

-- duplicate fulltext_match and compute once
-- @separator:table
explain select match(body, title) against('red') from src where match(body, title) against('red');

-- update
update src set body='color is brown' where id=0;

select * from src where match(body, title) against('brown');

-- delete
delete from src where id = 0;
select * from src where match(body, title) against('brown');

-- insert with NULL columns
insert into src (id, body) values (11, 'color is brown');
select * from src where match(body, title) against('brown');
update src set title='a good title' where id=11;
select * from src where match(body, title) against('brown');

-- truncate
delete from src;
select count(*) from src;

show create table src;

drop table src;

-- composite primary key with inline FULLTEXT
create table src2 (id1 varchar, id2 bigint, body char(128), title text, primary key (id1, id2), FULLTEXT(body, title) WITH PARSER gojieba);
insert into src2 values ('id0', 0, 'red', 't1'), ('id1', 1, 'yellow', 't2'), ('id2', 2, 'blue', 't3'), ('id3', 3, 'blue red', 't4');

select * from src2 where match(body, title) against('red');
select src2.*, match(body, title) against('blue') from src2;

show create table src2;

drop table src2;

-- big index build
drop table if exists t1;
create table t1(a int primary key, b varchar(200));
insert into t1 select result, "test create big fulltext index" from generate_series(3840001) g;
create fulltext index ftidx on t1 (b) with parser gojieba;
select count(*) from t1 where match(b) against ('test create' in natural language mode);
drop table t1;

-- #20149
CREATE TABLE IF NOT EXISTS nation (
  `n_nationkey`  INT,
  `n_name`       CHAR(25),
  `n_regionkey`  INT,
  `n_comment`    VARCHAR(152),
  `n_dummy`      VARCHAR(10),
  PRIMARY KEY (`n_nationkey`));

insert into nation values (0, 'china', 1, 'china beijing', 'dummy'), (1, 'korea', 2, 'korea noodle', 'dummmy');

create fulltext index ftidx on nation(n_comment) with parser gojieba;

select * from nation where match(n_comment) against('china');

delete from nation where n_nationkey = 0;

select * from nation where match(n_comment) against('china');

drop table nation;

-- pushdown limit
drop table if exists t1;
create table t1(a int primary key, b varchar(200));
insert into t1 select result, "pushdown limit is fast" from generate_series(30001) g;
create fulltext index ftidx on t1 (b) with parser gojieba;
-- no pushdown limit
select count(*) from t1 where match(b) against ('+pushdown +limit' in boolean mode);
-- pushdown limit
select count(*) from t1 where match(b) against ('+pushdown +limit' in boolean mode) limit 1;

drop table t1;

-- error drop column with multiple index parts
drop table if exists articles;
create table articles (id int auto_increment not null primary key, title varchar, body text);
create fulltext index ftidx on articles (title, body) with parser gojieba;
alter table articles drop column title;

-- drop column success
drop table if exists articles;
create table articles (id int auto_increment not null primary key, title varchar, body text);
create fulltext index ftidx on articles (title) with parser gojieba;
alter table articles drop column title;

-- #21678 simplified-Chinese coverage with jieba word segmentation
drop table if exists src;
create table src (id bigint primary key, body varchar, FULLTEXT(body) WITH PARSER gojieba);
insert into src values (0, 'SGB11型号的检验报告在对素材文件进行搜索时'), (1, '读书会 提效 社群 案例 运营 因为现在生产'),
(2, '使用全文索引会肥胖的原因都是因为摄入脂肪多导致的吗测试背景说明'),
(3, '索引肥胖的原因都是因为摄入fat多导致的吗说明');

select id from src where match(body) against('+SGB11型号的检验报告' IN BOOLEAN MODE);

select id from src where match(body) against('肥胖的原因都是因为摄入脂肪多导致的吗' IN NATURAL LANGUAGE MODE);

select id from src where match(body) against('+读书会 +提效 +社群 +案例 +运营' IN BOOLEAN MODE);

select id from src where match(body) against('肥胖的原因都是因为摄入fat多导致的吗' IN NATURAL LANGUAGE MODE);

drop table src;

-- mixed English / Chinese / json columns: only english_text and chinese_text
-- get a gojieba index here; json_data is left without an index.
CREATE TABLE example_table (id INT PRIMARY KEY,english_text TEXT, chinese_text TEXT,json_data JSON);
INSERT INTO example_table (id, english_text, chinese_text, json_data) VALUES(1, 'Hello, world!', '你好世界', '{"name": "Alice", "age": 30}'),(2, 'This is a test.', '这是一个测试', '{"name": "Bob", "age": 25}'),(3, 'Full-text search is powerful.', '全文搜索很强大', '{"name": "Charlie", "age": 35}');
CREATE FULLTEXT INDEX idx_english_text ON example_table (english_text) WITH PARSER gojieba;
CREATE FULLTEXT INDEX idx_chinese_text ON example_table (chinese_text) WITH PARSER gojieba;
(with t as (SELECT * FROM example_table WHERE MATCH(english_text) AGAINST('+test' IN BOOLEAN MODE)) select * from t) union all (with t as (SELECT * FROM example_table WHERE MATCH(english_text) AGAINST('+test' IN BOOLEAN MODE)) select * from t) ;
SELECT id, chinese_text FROM example_table WHERE MATCH(chinese_text) AGAINST('全文搜索' IN NATURAL LANGUAGE MODE);
SELECT id, chinese_text FROM example_table WHERE MATCH(chinese_text) AGAINST('+你好' IN BOOLEAN MODE);
drop table example_table;

-- BM25 path
set ft_relevancy_algorithm="BM25";

create table src (id bigint primary key, body varchar, title text);

insert into src values (0, 'color is red', 't1'), (1, 'car is yellow', 'crazy car'), (2, 'sky is blue', 'no limit'), (3, 'blue is not red', 'colorful'),
(4, '遠東兒童中文是針對6到9歲的小朋友精心設計的中文學習教材，共三冊，目前已出版一、二冊。', '遠東兒童中文'),
(5, '每冊均採用近百張全幅彩圖及照片，生動活潑、自然真實，加深兒童學習印象，洋溢學習樂趣。', '遠東兒童中文'),
(6, '各個單元主題內容涵蓋中華文化及生活應用的介紹。本套教材含課本、教學指引、生字卡、學生作業本與CD，中英對照，精美大字版。本系列有繁體字及簡體字兩種版本印行。', '中文短篇小說'),
(7, '59個簡單的英文和中文短篇小說', '適合初學者'),
(8, NULL, 'NOT INCLUDED'),
(9, 'NOT INCLUDED BODY', NULL),
(10, NULL, NULL);

create fulltext index ftidx on src (body, title) with parser gojieba;

select * from src where match(body, title) against('red');
select *, match(body, title) against('is red' in natural language mode) as score from src;
select * from src where match(body, title) against('教學指引');
select *, match(body, title) against('遠東兒童中文' in natural language mode) as score from src;

-- boolean mode
select * from src where match(body, title) against('+red blue' in boolean mode);
select * from src where match(body, title) against('+red -blue' in boolean mode);
select * from src where match(body, title) against('+red +blue' in boolean mode);
select * from src where match(body, title) against('+教學指引 +短篇小說' in boolean mode);

drop table src;

set ft_relevancy_algorithm="TF-IDF";

-- ============================================================
-- Chinese phrase queries in BOOLEAN MODE.
-- The phrase body must be re-tokenized through gojieba so it matches the
-- per-word rows the index stores. Pre-fix, an unspaced Chinese phrase
-- collapsed to a single TEXT pattern and never matched any row.
-- COUNT is used so the result is independent of internal join order.
-- ============================================================
drop table if exists srcph;
create table srcph (id int primary key, body text, fulltext (body) with parser gojieba);
insert into srcph values
(1, '我来到北京清华大学'),
(2, '北京清华大学是一所大学'),
(3, '我来到上海'),
(4, '清华大学很有名');

-- Two-token phrase "我来到" (jieba: 我, 来到) — matches docs 1 and 3.
select count(*) from srcph where match(body) against('"我来到"' in boolean mode);

-- Three-token phrase "我来到北京" (jieba: 我, 来到, 北京) — only doc 1.
select count(*) from srcph where match(body) against('"我来到北京"' in boolean mode);

-- Single-token phrase "清华大学" — matches docs 1, 2, 4.
select count(*) from srcph where match(body) against('"清华大学"' in boolean mode);

-- Phrase that doesn't appear as a contiguous jieba-token run anywhere.
select count(*) from srcph where match(body) against('"上海清华大学"' in boolean mode);

drop table srcph;

-- ============================================================
-- fulltext_index_tokenize(...) with parser=gojieba.
-- Direct table function call: the second arg `23` is the pkType code for
-- bigint (matrixone types.T_int64). Verifies the build-time path produces
-- jieba-segmented words plus the trailing __DocLen marker (5 rows total
-- for "我来到北京清华大学": 我, 来到, 北京, 清华大学, __DocLen).
-- ============================================================
select count(*) from (
select cast(column_0 as bigint) as id, column_1 as body
from (values row(1, '我来到北京清华大学'))
) as src
cross apply fulltext_index_tokenize('{"parser":"gojieba"}', 23, id, body) as f;

