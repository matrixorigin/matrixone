-- bm25 query-surface contract: a bm25 index is queried ONLY through BM25(col)
-- AGAINST('query') — a distinct, mode-free ranked bag-of-words surface. It is NOT
-- queried through MATCH() (that is the classic fulltext surface); MATCH() on a
-- bm25-only column finds no fulltext index and errors. BM25() itself has no mode
-- modifiers (no boolean / natural language / query expansion) — those are not even
-- expressible in the grammar.
drop database if exists bm25_mode;
create database bm25_mode;
use bm25_mode;
create table t (id bigint primary key, txt text);
insert into t values (1,'apple banana'),(2,'banana cherry'),(3,'cherry date');
create index ft using bm25 on t(txt) with parser gojieba;
-- BM25() ranked retrieval works
select id from t where bm25(txt) against('apple') order by id;
select id from t where bm25(txt) against('banana') order by id;
-- multi-term bag-of-words
select id from t where bm25(txt) against('apple cherry') order by id;
-- MATCH() on a bm25-only column has no classic fulltext index -> error
select id from t where match(txt) against('apple');
drop database bm25_mode;
