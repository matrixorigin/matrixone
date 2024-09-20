-- TODO: run all tests with both experimental_fulltext_index = 0 and 1
-- TODO: GENERATE the test case to cover all combinations of types (varchar, char and text)
set experimental_fulltext_index=1;

create table tmp (id bigint primary key, body varchar, title text, FULLTEXT(title, body));
drop table tmp;

create table src (id bigint primary key, body varchar, title text);

insert into src values (0, 'color is red', 't1'), (1, 'car is yellow', 'crazy car'), (2, 'sky is blue', 'no limit'), (3, 'blue is not red', 'colorful'),
(4, '遠東兒童中文是針對6到9歲的小朋友精心設計的中文學習教材，共三冊，目前已出版一、二冊。', '遠東兒童中文'),
(5, '每冊均採用近百張全幅彩圖及照片，生動活潑、自然真實，加深兒童學習印象，洋溢學習樂趣。', '遠東兒童中文'),
(6, '各個單元主題內容涵蓋中華文化及生活應用的介紹。本套教材含課本、教學指引、生字卡、學生作業本與CD，中英對照，精美大字版。本系列有繁體字及簡體字兩種版本印行。', '中文短篇小說'),
(7, '59個簡單的英文和中文短篇小說', '適合初學者');

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

drop table src2;

-- bytejson parser
create table src (id bigint primary key, json1 json, json2 json);
insert into src values  (0, '{"a":1, "b":"red"}', '{"d": "happy birthday", "f":"winter"}'), 
(1, '{"a":2, "b":"中文學習教材"}', '["apple", "orange", "banana", "指引"]'),
(2, '{"a":3, "b":"red blue"}', '{"d":"兒童中文"}');

create fulltext index ftidx on src (json1) with parser json;

select * from src where match(json1) against('red' in boolean mode);

select * from src where match(json1) against('中文學習教材' in boolean mode);

create fulltext index ftidx2 on src (json1, json2) with parser json;
select * from src where match(json1, json2) against('+red +winter' in boolean mode);

select * from src where match(json1, json2) against('中文學習教材' in boolean mode);

drop table src;

-- bytejson parser
create table src (id bigint primary key, json1 text, json2 varchar);
insert into src values  (0, '{"a":1, "b":"red"}', '{"d": "happy birthday", "f":"winter"}'),
(1, '{"a":2, "b":"中文學習教材"}', '["apple", "orange", "banana", "指引"]'),
(2, '{"a":3, "b":"red blue"}', '{"d":"兒童中文"}');

create fulltext index ftidx on src (json1) with parser json;

select * from src where match(json1) against('red' in boolean mode);

select * from src where match(json1) against('中文學習教材' in boolean mode);

create fulltext index ftidx2 on src (json1, json2) with parser json;
select * from src where match(json1, json2) against('+red +winter' in boolean mode);

select * from src where match(json1, json2) against('中文學習教材' in boolean mode);

drop table src;


