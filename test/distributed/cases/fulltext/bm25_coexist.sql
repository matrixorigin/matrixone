-- A classic fulltext index and a bm25 index coexist on the SAME column, cleanly
-- disambiguated by query verb (no mode routing): MATCH(txt) -> the classic postings
-- index (supports boolean / natural language), BM25(txt) -> the bm25 ranked index.
-- Same-category duplicates (two classic, or two bm25) on one column stay rejected.
drop database if exists bm25_coexist;
create database bm25_coexist;
use bm25_coexist;
create table t (id bigint primary key, txt text);
insert into t values (1,'apple banana cherry'),(2,'apple banana'),(3,'apple'),(4,'durian'),(5,'apple apple apple banana');
create fulltext index ftc on t(txt);
create index ftr using bm25 on t(txt) with parser gojieba;
-- MATCH -> classic fulltext (boolean mode: OR bag-of-words over any term)
select id from t where match(txt) against('apple banana cherry' in boolean mode) order by id;
-- MATCH -> classic fulltext (natural language)
select id from t where match(txt) against('apple' in natural language mode) order by id;
-- BM25 -> bm25 ranked top-K (BM25 score DESC)
select id from t where bm25(txt) against('apple');
-- BM25 ranked bag-of-words, multi-term
select id from t where bm25(txt) against('apple banana');
-- same-category duplicate on the same column is still rejected
create fulltext index ftc2 on t(txt);
create index ftr2 using bm25 on t(txt) with parser gojieba;
drop database bm25_coexist;
