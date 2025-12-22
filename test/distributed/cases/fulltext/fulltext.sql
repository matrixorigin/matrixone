-- Deprecated: experimental_fulltext_index is always enabled, kept for compatibility
-- Test compatibility: setting the variable should not error
SET experimental_fulltext_index = 1;
SET experimental_fulltext_index = 0;


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

create fulltext index ftidx on src (body, title);

-- check fulltext_match with index error
create fulltext index ftidx02 on src (body, title);
select * from src where match(body) against('red');
select * from src where match(body,title) against('+]]]');

select match(body) against('red') from src;

-- add index for body column
alter table src add fulltext index ftidx2 (body);
create fulltext index ftidx03 on src (body);
create fulltext index ftidx03 on src (body, title);

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

-- insert data

insert into src values (0, 'color is red', 't1'), (1, 'car is yellow', 'crazy car'), (2, 'sky is blue', 'no limit'), (3, 'blue is not red', 'colorful'),
(4, '遠東兒童中文是針對6到9歲的小朋友精心設計的中文學習教材，共三冊，目前已出版一、二冊。', '遠東兒童中文'),
(5, '每冊均採用近百張全幅彩圖及照片，生動活潑、自然真實，加深兒童學習印象，洋溢學習樂趣。', '遠東兒童中文'),
(6, '各個單元主題內容涵蓋中華文化及生活應用的介紹。本套教材含課本、教學指引、生字卡、學生作業本與CD，中英對照，精美大字版。本系列有繁體字及簡體字兩種版本印行。', '中文短>篇小說'),
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

create fulltext index ftidx2 on src2 (body, title);
select * from src2 where match(body, title) against('red');
select src2.*, match(body, title) against('blue') from src2;

update src2 set body = 'orange' where id1='id0';

select * from src2 where match(body, title) against('red');

delete from src2 where id1='id3';

select * from src2 where match(body, title) against('t4');

insert into src2 values ('id4', 4, 'light brown', 't5');

select * from src2 where match(body, title) against('t5');

desc src;
show create table src;

drop table src2;

-- bytejson parser
create table src (id bigint primary key, json1 json, json2 json);
insert into src values  (0, '{"a":1, "b":"red"}', '{"d": "happy birthday", "f":"winter"}'),
(1, '{"a":2, "b":"中文學習教材"}', '["apple", "orange", "banana", "指引"]'),
(2, '{"a":3, "b":"red blue"}', '{"d":"兒童中文"}');

create fulltext index ftidx on src (json1) with parser json;

select * from src where match(json1) against('red' in boolean mode);

select * from src where match(json1) against('中文學習教材' in natural language mode);

create fulltext index ftidx2 on src (json1, json2) with parser json;
select * from src where match(json1, json2) against('+red +winter' in boolean mode);

select * from src where match(json1, json2) against('中文學習教材' in natural language mode);

desc src;
show create table src;

drop table src;

-- bytejson parser
create table src (id bigint primary key, json1 text, json2 varchar);
insert into src values  (0, '{"a":1, "b":"red"}', '{"d": "happy birthday", "f":"winter"}'),
(1, '{"a":2, "b":"中文學習教材"}', '["apple", "orange", "banana", "指引"]'),
(2, '{"a":3, "b":"red blue"}', '{"d":"兒童中文"}');

create fulltext index ftidx on src (json1) with parser json;

select * from src where match(json1) against('red' in boolean mode);

select * from src where match(json1) against('中文學習教材' in natural language  mode);

create fulltext index ftidx2 on src (json1, json2) with parser json;
select * from src where match(json1, json2) against('+red +winter' in boolean mode);

select * from src where match(json1, json2) against('中文學習教材' in natural language mode);

desc src;
show create table src;

drop table src;

-- update/insert/delete
create table src (id bigint primary key, body varchar, title text, FULLTEXT(title, body));
insert into src values (0, 'color is red', 't1'), (1, 'car is yellow', 'crazy car'), (2, 'sky is blue', 'no limit'), (3, 'blue is not red', 'colorful'),
(4, '遠東兒童中文是針對6到9歲的小朋友精心設計的中文學習教材，共三冊，目前已出版一、二冊。', '遠東兒童中文'),
(5, '每冊均採用近百張全幅彩圖及照片，生動活潑、自然真實，加深兒童學習印象，洋溢學習樂趣。', '遠東兒童中文'),
(6, '各個單元主題內容涵蓋中華文化及生活應用的介紹。本套教材含課本、教學指引、生字卡、學生作業本與CD，中英對照，精美大字版。本系列有繁體字及簡體字兩種版本印行。', '中文短篇小說
'),
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

desc src;
show create table src;

-- drop table
drop table src;

-- composite primary key
create table src2 (id1 varchar, id2 bigint, body char(128), title text, primary key (id1, id2), FULLTEXT(body, title));
insert into src2 values ('id0', 0, 'red', 't1'), ('id1', 1, 'yellow', 't2'), ('id2', 2, 'blue', 't3'), ('id3', 3, 'blue red', 't4');

select * from src2 where match(body, title) against('red');
select src2.*, match(body, title) against('blue') from src2;

desc src2;
show create table src2;

drop table src2;

-- bytejson parser
create table src (id bigint primary key, json1 json, json2 json, FULLTEXT(json1) with parser json);
insert into src values  (0, '{"a":1, "b":"red"}', '{"d": "happy birthday", "f":"winter"}'),
(1, '{"a":2, "b":"中文學習教材"}', '["apple", "orange", "banana", "指引"]'),
(2, '{"a":3, "b":"red blue"}', '{"d":"兒童中文"}');

select * from src where match(json1) against('red' in boolean mode);

select * from src where match(json1) against('中文學習教材' in natural language mode);

create fulltext index ftidx2 on src (json1, json2) with parser json;
select * from src where match(json1, json2) against('+red +winter' in boolean mode);

select * from src where match(json1, json2) against('中文學習教材' in natural language mode);

update src set json1='{"c":"update json"}' where id=0;

desc src;
show create table src;

drop table src;

-- bytejson parser
create table src (id bigint primary key, json1 text, json2 varchar, fulltext(json1) with parser json);
insert into src values  (0, '{"a":1, "b":"red"}', '{"d": "happy birthday", "f":"winter"}'),
(1, '{"a":2, "b":"中文學習教材"}', '["apple", "orange", "banana", "指引"]'),
(2, '{"a":3, "b":"red blue"}', '{"d":"兒童中文"}');

select * from src where match(json1) against('red' in boolean mode);

select * from src where match(json1) against('中文學習教材' in natural language mode);

create fulltext index ftidx2 on src (json1, json2) with parser json;
select * from src where match(json1, json2) against('+red +winter' in boolean mode);

select * from src where match(json1, json2) against('中文學習教材' in natural language mode);

update src set json1='{"c":"update json"}' where id=0;

select * from src where match(json1, json2) against('"update json"' in boolean mode);

desc src;
show create table src;

drop table src;

drop table if exists t1;
create table t1(a int primary key, b varchar(200));
insert into t1 select result, "test create big fulltext index" from generate_series(3840001) g;
create fulltext index ftidx on t1 (b);
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

create fulltext index ftidx on nation(n_comment);

select * from nation where match(n_comment) against('china');

delete from nation where n_nationkey = 0;

select * from nation where match(n_comment) against('china');

drop table nation;


-- pushdown limit
drop table if exists t1;
create table t1(a int primary key, b varchar(200));
insert into t1 select result, "pushdown limit is fast" from generate_series(30001) g;
create fulltext index ftidx on t1 (b);
-- no pushdown limit
select count(*) from t1 where match(b) against ('+pushdown +limit' in boolean mode);
-- pushdown limit
select count(*) from t1 where match(b) against ('+pushdown +limit' in boolean mode) limit 1;

drop table t1;

-- error drop column with multiple index parts
drop table if exists articles;
create table articles (id int auto_increment not null primary key, title varchar, body text);
create fulltext index ftidx on articles (title, body);
alter table articles drop column title;

-- drop column success
drop table if exists articles;
create table articles (id int auto_increment not null primary key, title varchar, body text);
create fulltext index ftidx on articles (title);
alter table articles drop column title;

-- #21678
drop table if exists src;
create table src (id bigint primary key, body varchar, FULLTEXT(body));
insert into src values (0, 'SGB11型号的检验报告在对素材文件进行搜索时'), (1, '读书会 提效 社群 案例 运营 因为现在生产'),
(2, '使用全文索引会肥胖的原因都是因为摄入脂肪多导致的吗测试背景说明'),
(3, '索引肥胖的原因都是因为摄入fat多导致的吗说明');

select id from src where match(body) against('+SGB11型号的检验报告' IN BOOLEAN MODE);

select id from src where match(body) against('肥胖的原因都是因为摄入脂肪多导致的吗' IN NATURAL LANGUAGE MODE);

select id from src where match(body) against('+读书会 +提效 +社群 +案例 +运营' IN BOOLEAN MODE);

select id from src where match(body) against('肥胖的原因都是因为摄入fat多导致的吗' IN NATURAL LANGUAGE MODE);
CREATE TABLE example_table (id INT PRIMARY KEY,english_text TEXT, chinese_text TEXT,json_data JSON);
INSERT INTO example_table (id, english_text, chinese_text, json_data) VALUES(1, 'Hello, world!', '你好世界', '{"name": "Alice", "age": 30}'),(2, 'This is a test.', '这是一个测试', '{"name": "Bob", "age": 25}'),(3, 'Full-text search is powerful.', '全文搜索很强大', '{"name": "Charlie", "age": 35}');
CREATE FULLTEXT INDEX idx_english_text ON example_table (english_text);
(with t as (SELECT * FROM example_table WHERE MATCH(english_text) AGAINST('+test' IN BOOLEAN MODE)) select * from t) union all (with t as (SELECT * FROM example_table WHERE MATCH(english_text) AGAINST('+test' IN BOOLEAN MODE)) select * from t) ;

create table empty_fulltext(a int primary key, b varchar, c varchar, fulltext `ftc` (c) WITH PARSER ngram);
insert into empty_fulltext select result, 'so long',null from generate_series(1, 300000) g;
insert into empty_fulltext values (300001, 'this is the end of our story', null);
-- big enough to trigger remote delete
delete from empty_fulltext where b like '%story%';
drop table empty_fulltext;
