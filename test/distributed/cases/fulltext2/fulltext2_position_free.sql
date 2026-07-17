-- fulltext2 POSITION_FREE: a position-free index (WITH PARSER gojieba POSITION_FREE=TRUE)
-- drops the positional payload — bag-of-words retrieval only, ~half the footprint, FST
-- kept — and is queried with IN BM25 MODE. NL / BOOLEAN / default (phrase) modes are
-- rejected because there are no positions. Only the gojieba WORD parser is allowed
-- (ngram/json emit overlapping trigrams/values whose bag-of-words result is noise).
-- REINDEX POSITION_FREE=TRUE|FALSE toggles it both ways (a REBUILD from source).
set experimental_fulltext2_index = 1;
drop database if exists ft2_posfree;
create database ft2_posfree;
use ft2_posfree;

create table t (id bigint primary key, body text);
insert into t values
 (1,'我家有三个人'),(2,'三个人都住在我家'),(3,'我家的花园很漂亮'),
 (4,'教室里有三个人'),(5,'昨天我家有三个人来吃饭');

-- POSITION_FREE requires the gojieba parser: ngram is rejected ...
create fulltext2 index bad1 on t(body) with parser ngram position_free = true;
-- ... and the default parser (ngram) with a bare POSITION_FREE=TRUE is rejected too
create fulltext2 index bad2 on t(body) position_free = true;

-- valid: gojieba + POSITION_FREE=TRUE
create fulltext2 index ft on t(body) with parser gojieba position_free = true;
select algo_params from mo_catalog.mo_indexes where name = 'ft' and algo_params like '%parser%' limit 1;

-- IN BM25 MODE: bag-of-words — every doc containing any of 我家/有/三个/人
select id from t where match(body) against('我家有三个人' in bm25 mode) order by id;
-- a single token: only doc 4 has 教室
select id from t where match(body) against('教室' in bm25 mode) order by id;

-- NL / BOOLEAN / default phrase modes need positions -> rejected on a position-free index
select id from t where match(body) against('我家有三个人' in boolean mode) order by id;
select id from t where match(body) against('我家有三个人' in natural language mode) order by id;
select id from t where match(body) against('我家有三个人') order by id;

-- REINDEX POSITION_FREE=FALSE -> positional rebuild: BOOLEAN exact phrase now works.
-- '我家有三个人' is a contiguous substring only in docs 1 and 5.
alter table t alter reindex ft fulltext2 position_free = false;
select algo_params from mo_catalog.mo_indexes where name = 'ft' and algo_params like '%parser%' limit 1;
select id from t where match(body) against('我家有三个人' in boolean mode) order by id;

-- REINDEX POSITION_FREE=TRUE -> position-free again: BOOLEAN rejected, IN BM25 MODE works.
alter table t alter reindex ft fulltext2 position_free = true;
select algo_params from mo_catalog.mo_indexes where name = 'ft' and algo_params like '%parser%' limit 1;
select id from t where match(body) against('我家有三个人' in boolean mode) order by id;
select id from t where match(body) against('三个人' in bm25 mode) order by id;

drop database ft2_posfree;
