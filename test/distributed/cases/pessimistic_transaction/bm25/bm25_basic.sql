-- bm25 ranked-retrieval index: synchronous build from source + BM25() ranked
-- retrieval (bag-of-words BM25 top-K), LIMIT top-K pushdown, and the distinct
-- query surface (BM25(), not MATCH()).
drop database if exists bm25_basic;
create database bm25_basic;
use bm25_basic;
set experimental_bm25_index = 1;
create table docs (id bigint primary key, body text);
insert into docs values (1,'apple banana cherry'),(2,'apple banana'),(3,'apple'),(4,'durian mango'),(5,'apple apple apple banana');
create index ftx using bm25 on docs(body) with parser gojieba;
-- membership: docs containing 'apple' (doc 4 excluded)
select id from docs where bm25(body) against('apple');
-- ranked by BM25 score DESC (no ORDER BY): doc 5 (apple x3) first, doc 1 (longest) last
select id from docs where bm25(body) against('apple');
-- multi-term bag-of-words
select id from docs where bm25(body) against('apple banana');
-- LIMIT top-K pushdown: the two highest-scored docs
select id from docs where bm25(body) against('apple') limit 2;
-- distinct surface: MATCH() on a bm25-only column has no fulltext index -> error
select id from docs where match(body) against('apple');
drop database bm25_basic;
