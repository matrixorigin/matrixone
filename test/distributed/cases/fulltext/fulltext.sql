-- TODO: run all tests with both experimental_fulltext_index = 0 and 1
-- TODO: GENERATE the test case to cover all combinations of types (varchar, char and text)
set experimental_fulltext_index=1;

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
select * from src where match(body) against('red');

select match(body) against('red') from src;

-- add index for body column
alter table src add fulltext index ftidx2 (body);

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

-- drop table
drop table src;

-- composite primary key
create table src2 (id1 varchar, id2 bigint, body char(128), title text, primary key (id1, id2), FULLTEXT(body, title));
insert into src2 values ('id0', 0, 'red', 't1'), ('id1', 1, 'yellow', 't2'), ('id2', 2, 'blue', 't3'), ('id3', 3, 'blue red', 't4');

select * from src2 where match(body, title) against('red');
select src2.*, match(body, title) against('blue') from src2;

desc src;
show create table src;

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
select b from t1 where match(b) against ('test create' in natural language mode) limit 1;
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

create table articles (id INT UNSIGNED AUTO_INCREMENT NOT NULL PRIMARY KEY, title VARCHAR(200), body TEXT, FULLTEXT (title,body));
show create table articles;
drop table articles;

create table articles (id INT UNSIGNED AUTO_INCREMENT NOT NULL PRIMARY KEY, title VARCHAR(200), body TEXT);
create fulltext index fdx_01 on articles(title, body);
drop index fdx_01 on articles;
create fulltext index fdx_02 on articles(title);
drop  index fdx_02 on articles;
create fulltext index fdx_03 on articles(id);
drop  index fdx_04 on articles(title, body) with PARSER ngram;
drop  index fdx_04 on articles;
drop table articles;

create table src (id bigint primary key, json1 json, json2 json);
create fulltext index ftidx1 on src(json1) with parser json;
show create table src;
alter table src drop column json1;
drop table src;

create table articles (id INT UNSIGNED AUTO_INCREMENT NOT NULL PRIMARY KEY, title VARCHAR(200), body TEXT);
insert into articles (title,body) VALUES ('MySQL Tutorial','DBMS stands for DataBase ...'),
('How To Use MySQL Well','After you went through a ...'),
('Optimizing MySQL','In this tutorial, we show ...'),
('1001 MySQL Tricks','1. Never run mysqld as root. 2. ...'),
('MySQL vs. YourSQL','In the following database comparison ...'),
('MySQL Security','When configured properly, MySQL ...');
create fulltext index fdx_01 on articles(title, body) with parser ngram;
select * from articles where match(title,body)  AGAINST ('database' IN NATURAL LANGUAGE MODE) union select * from articles where match(title,body)  AGAINST ('YourSQL' IN NATURAL LANGUAGE MODE) order by id;
drop index fdx_01 on articles;
select * from articles where match(title,body)  AGAINST ('database' IN NATURAL LANGUAGE MODE) union select * from articles where match(title,body)  AGAINST ('YourSQL' IN NATURAL LANGUAGE MODE) order by id;
create fulltext index fdx_01 on articles(title, body);
select * from articles where match(title,body)  AGAINST ('database' IN NATURAL LANGUAGE MODE) union select * from articles where match(title,body)  AGAINST ('YourSQL' IN NATURAL LANGUAGE MODE) order by id;
select count(*) from (select * from articles where match(title,body)  AGAINST ('database' IN NATURAL LANGUAGE MODE));
drop table articles;

create table articles (id INT UNSIGNED AUTO_INCREMENT NOT NULL PRIMARY KEY, title VARCHAR(200), body TEXT);
insert into articles (title,body) VALUES ('神雕侠侣 第一回 风月无情','越女采莲秋水畔，窄袖轻罗，暗露双金钏 ...'),
('神雕侠侣 第二回 故人之子','正自发痴，忽听左首屋中传出一人喝道：“这是在人家府上，你又提小龙女干什么？” ...'),
('神雕侠侣 第三回 投师终南','郭靖在舟中潜运神功，数日间伤势便已痊愈了大半。 ...'),
('神雕侠侣 第四回 全真门下','郭靖摆脱众道纠缠，提气向重阳宫奔去，忽听得钟声镗镗响起 ...'),
('神雕侠侣 第五回 活死人墓','杨过摔下山坡，滚入树林长草丛中，便即昏晕 ...'),
('神雕侠侣 第六回 玉女心经','小龙女从怀里取出一个瓷瓶，交在杨过手里 ...');
create fulltext index fdx_01 on articles(title, body) with parser ngram;
select * from articles where match(title,body)  AGAINST ('风月无情' IN NATURAL LANGUAGE MODE);
select * from articles where match(title,body)  AGAINST ('杨过' IN NATURAL LANGUAGE MODE);
select * from articles where match(title,body)  AGAINST ('小龙女' IN NATURAL LANGUAGE MODE);
select * from articles where match(title,body)  AGAINST ('神雕侠侣' IN NATURAL LANGUAGE MODE);
drop table articles;

create table articles (id INT UNSIGNED AUTO_INCREMENT NOT NULL PRIMARY KEY, title json, body json);
insert into articles (title,body) VALUES ('{"title": "MySQL Tutorial"}','{"body":"DBMS stands for DataBase ..."}'),
('{"title":"How To Use MySQL Well"}','{"body":"After you went through a ..."}'),
('{"title":"Optimizing MySQL"}','{"body":"In this tutorial, we show ..."}'),
('{"title":"1001 MySQL Tricks"}','{"body":"1. Never run mysqld as root. 2. ..."}'),
('{"title":"MySQL vs. YourSQL"}','{"body":"In the following database comparison ..."}'),
('{"title":"MySQL Security"}','{"body":"When configured properly, MySQL ..."}');
create fulltext index fdx_01 on articles(title, body) with parser json;
select * from articles where match(title,body)  AGAINST ('database' IN NATURAL LANGUAGE MODE) union select * from articles where match(title,body)  AGAINST ('YourSQL' IN NATURAL LANGUAGE MODE) order by id;
drop index fdx_01 on articles;
select * from articles where match(title,body)  AGAINST ('database' IN NATURAL LANGUAGE MODE) union select * from articles where match(title,body)  AGAINST ('YourSQL' IN NATURAL LANGUAGE MODE) order by id;
create fulltext index fdx_01 on articles(title, body);
select * from articles where match(title,body)  AGAINST ('database' IN NATURAL LANGUAGE MODE) union select * from articles where match(title,body)  AGAINST ('YourSQL' IN NATURAL LANGUAGE MODE) order by id;
select count(*) from (select * from articles where match(title,body)  AGAINST ('database' IN NATURAL LANGUAGE MODE));
drop table articles;

create table articles (id INT UNSIGNED AUTO_INCREMENT NOT NULL PRIMARY KEY, title json, body json);
insert into articles (title,body) VALUES ('{"title": "神雕侠侣 第一回 风月无情"}','{"body":"越女采莲秋水畔，窄袖轻罗，暗露双金钏 ..."}'),
('{"title":"神雕侠侣 第二回 故人之子"}','{"body":"正自发痴，忽听左首屋中传出一人喝道：“这是在人家府上，你又提小龙女干什么？” ..."}'),
('{"title":"神雕侠侣 第三回 投师终南"}','{"body":"郭靖在舟中潜运神功，数日间伤势便已痊愈了大半。 ..."}'),
('{"title":"神雕侠侣 第四回 全真门下"}','{"body":"郭靖摆脱众道纠缠，提气向重阳宫奔去，忽听得钟声镗镗响起 ..."}'),
('{"title":"神雕侠侣 第五回 活死人墓"}','{"body":"杨过摔下山坡，滚入树林长草丛中，便即昏晕 ..."}'),
('{"title":"神雕侠侣 第六回 玉女心经"}','{"body":"小龙女从怀里取出一个瓷瓶，交在杨过手里 ..."}');
create fulltext index fdx_01 on articles(title, body) with parser json;
select * from articles where match(title,body)  AGAINST ('风月无情' IN NATURAL LANGUAGE MODE);
select * from articles where match(title,body)  AGAINST ('杨过' IN NATURAL LANGUAGE MODE);
select * from articles where match(title,body)  AGAINST ('小龙女' IN NATURAL LANGUAGE MODE);
select * from articles where match(title,body)  AGAINST ('神雕侠侣' IN NATURAL LANGUAGE MODE);
drop table articles;

drop table if exists t1;
create table t1(a int primary key, b varchar(200), c int);
insert into t1(a,b,c) select result, "test create big fulltext index" ,result from generate_series(10000) g;
create fulltext index ftidx on t1 (b);
create index index2 on t1 (c);
-- @separator:table
explain select * from t1 where c = 100;
drop table t1;