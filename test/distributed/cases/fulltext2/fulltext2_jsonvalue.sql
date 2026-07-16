-- fulltext2 json_value parser: each json leaf value is ONE whole atomic token,
-- matched EXACTLY (no ngram, no bag-of-words) — parity with classic fulltext's
-- json_value (a whole value is indexed; a query matches `word = value`). A value's
-- substring never matches; punctuation and CJK values are single tokens.

drop database if exists fulltext2_jsonvalue;
create database fulltext2_jsonvalue;
use fulltext2_jsonvalue;

set experimental_fulltext2_index = 1;

-- ============ JSON-typed column ============
drop table if exists js;
create table js(id bigint primary key, doc json);
insert into js values
 (0,'{"a":"redbluebrownyelloworange","b":"winter"}'),
 (1,'{"a":"red","b":"winter"}'),
 (2,'{"a":"中文學習教材"}'),
 (3,'{"a":"happybirthday.happy-birthday_happybirthday"}');
create fulltext2 index ft on js(doc) with parser json_value;

-- whole-value exact match (punctuation / CJK kept as one token)
select id from js where match(doc) against('redbluebrownyelloworange' in boolean mode) order by id;
select id from js where match(doc) against('中文學習教材' in boolean mode) order by id;
select id from js where match(doc) against('happybirthday.happy-birthday_happybirthday' in boolean mode) order by id;

-- "red" is a WHOLE value only in row 1 — NOT a substring hit on row 0
select id from js where match(doc) against('red' in boolean mode) order by id;

-- distinct values OR / multi-operand MUST
select id from js where match(doc) against('winter' in boolean mode) order by id;
select id from js where match(doc) against('+red +winter' in boolean mode) order by id;

-- no bag-of-words: a substring of a value never matches
select id from js where match(doc) against('bluebrown' in boolean mode) order by id;
select id from js where match(doc) against('中文學習' in boolean mode) order by id;

-- ============ multi-column json_value ============
drop table if exists js2;
create table js2(id bigint primary key, j1 json, j2 json);
insert into js2 values
 (0,'{"x":"red"}','{"y":"winter"}'),
 (1,'{"x":"blue"}','{"y":"summer"}');
create fulltext2 index ft on js2(j1, j2) with parser json_value;
select id from js2 where match(j1, j2) against('+red +winter' in boolean mode) order by id;
select id from js2 where match(j1, j2) against('summer' in boolean mode) order by id;

-- ============ text/varchar column holding json ============
drop table if exists jt;
create table jt(id bigint primary key, doc text);
insert into jt values
 (0,'{"a":"red..blue--brown@@yellow::orange"}'),
 (1,'{"a":"兒童中文"}');
create fulltext2 index ft on jt(doc) with parser json_value;
select id from jt where match(doc) against('red..blue--brown@@yellow::orange' in boolean mode) order by id;
select id from jt where match(doc) against('兒童中文' in boolean mode) order by id;
select id from jt where match(doc) against('red' in boolean mode) order by id;

drop database fulltext2_jsonvalue;
