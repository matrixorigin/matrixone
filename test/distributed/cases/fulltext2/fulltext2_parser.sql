-- fulltext2 parsers (synchronous build): ngram (default — CJK sliding 3-gram,
-- Latin whole-word), gojieba (dictionary word segmentation), json (each leaf
-- indexed as an order-preserving (key, value) TUPLE). NL is an EXACT ordered
-- phrase for ngram/gojieba (no bag-of-words); gojieba segments into words first.
-- The json parser is not a text parser at all: it answers json_extract
-- comparisons, not free-text MATCH — see the json section below.

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
-- The json parser indexes each leaf as a (key, value) tuple, so it distinguishes
-- {"b":"red"} from {"c":"red"} — which the old value-only flattening could not.
-- The tuples are consumed by the optimizer, which rewrites a json_extract
-- comparison into an index probe ANDed with the original predicate; the original
-- is always re-evaluated, so results equal an unindexed scan.
drop table if exists js;
create table js(id bigint primary key, doc json);
-- js_plain holds identical rows with NO index: the oracle for every query below.
drop table if exists js_plain;
create table js_plain(id bigint primary key, doc json);
insert into js values
 (0,'{"a":1,"b":"red apple"}'),
 (1,'{"a":2,"b":"中文學習教材"}'),
 (2,'{"a":3,"b":"red blue"}');
insert into js_plain select * from js;
create fulltext2 index ft on js(doc) with parser json;

-- string equality on a leaf, including CJK (the value is one whole term, not ngrams)
select id from js where json_extract_string(doc,'$.b') = 'red apple' order by id;
select id from js_plain where json_extract_string(doc,'$.b') = 'red apple' order by id;
select id from js where json_extract_string(doc,'$.b') = '中文學習教材' order by id;
select id from js_plain where json_extract_string(doc,'$.b') = '中文學習教材' order by id;

-- a SUBSTRING of a value is not a match: the tuple holds the whole value, so
-- 'red' alone finds nothing (the old flattened-ngram index would have hit here)
select id from js where json_extract_string(doc,'$.b') = 'red' order by id;
select id from js_plain where json_extract_string(doc,'$.b') = 'red' order by id;

-- the key is part of the term: the same value under a different key is distinct
select id from js where json_extract_string(doc,'$.a') = 'red apple' order by id;
select id from js_plain where json_extract_string(doc,'$.a') = 'red apple' order by id;

-- numeric equality and ranges over the numeric leaf
select id from js where json_extract_float64(doc,'$.a') = 2 order by id;
select id from js_plain where json_extract_float64(doc,'$.a') = 2 order by id;
select id from js where json_extract_float64(doc,'$.a') >= 2 order by id;
select id from js_plain where json_extract_float64(doc,'$.a') >= 2 order by id;
select id from js where json_extract_float64(doc,'$.a') < 3 order by id;
select id from js_plain where json_extract_float64(doc,'$.a') < 3 order by id;

drop database fulltext2_parser;
