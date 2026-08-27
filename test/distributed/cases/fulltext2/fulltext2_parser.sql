-- fulltext2 parsers (synchronous build): ngram (default — CJK sliding 3-gram,
-- Latin whole-word), gojieba (dictionary word segmentation), json (values
-- flattened + indexed as ngram). NL is an EXACT ordered phrase for every parser
-- (no bag-of-words); gojieba segments into words first.

drop database if exists fulltext2_parser;
create database fulltext2_parser;
use fulltext2_parser;

-- CREATE FULLTEXT2 INDEX is gated behind experimental_fulltext2_index (default off).
set experimental_fulltext2_index = 1;

-- ================= ngram parser: CJK bag-of-words =================
drop table if exists zh;
create table zh(id bigint primary key, body text);
insert into zh values
 (0,'遠東兒童中文學習教材'),
 (1,'中文短篇小說適合初學者'),
 (2,'兒童學習樂趣'),
 (3,'教學指引與生字卡'),
 (4,'中文學習中文學習');
create fulltext2 index ft on zh(body) with parser ngram;
show create table zh;

select id from zh where match(body) against('中文學習') order by id;
select id from zh where match(body) against('兒童') order by id;
select id from zh where match(body) against('+中文 +學習' in boolean mode) order by id;
select id from zh where match(body) against('+兒童 -中文' in boolean mode) order by id;

-- ================= gojieba parser: word segmentation, exact phrase =================
drop table if exists zj;
create table zj(id bigint primary key, body text);
insert into zj values
 (0,'我来到北京清华大学'),
 (1,'苹果香蕉都好吃'),
 (2,'我爱北京天安门'),
 (3,'清华大学在北京'),
 (4,'香蕉和苹果');
create fulltext2 index ft on zj(body) with parser gojieba;

select id from zj where match(body) against('北京') order by id;
select id from zj where match(body) against('清华大学') order by id;
select id from zj where match(body) against('北京清华大学') order by id;
select id from zj where match(body) against('+苹果 +香蕉' in boolean mode) order by id;
select id from zj where match(body) against('+北京 -苹果' in boolean mode) order by id;

-- ================= json parser (JSON-typed column) =================
drop table if exists js;
create table js(id bigint primary key, doc json);
insert into js values
 (0,'{"a":1,"b":"red apple"}'),
 (1,'{"a":2,"b":"中文學習教材"}'),
 (2,'{"a":3,"b":"red blue"}');
create fulltext2 index ft on js(doc) with parser json;

select id from js where match(doc) against('red' in boolean mode) order by id;
select id from js where match(doc) against('+red +apple' in boolean mode) order by id;
select id from js where match(doc) against('中文學習') order by id;

drop database fulltext2_parser;
