-- bm25 mode contract: a bm25 index answers default + natural-language (ranked
-- bag-of-words) MATCH, but rejects boolean mode (position-free, no operators).
-- Ported/adapted from fulltext_retrieval_mode_guard.sql for the bm25 surface.
drop database if exists bm25_mode;
create database bm25_mode;
use bm25_mode;
create table t (id bigint primary key, txt text);
insert into t values (1,'apple banana'),(2,'banana cherry'),(3,'cherry date');
create index ft using bm25 on t(txt) with parser gojieba;
select id from t where match(txt) against('apple') order by id;
select id from t where match(txt) against('apple' in natural language mode) order by id;
select id from t where match(txt) against('+apple' in boolean mode) order by id;
drop database bm25_mode;
