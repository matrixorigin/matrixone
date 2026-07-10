-- bm25 ranked-retrieval index: synchronous build from source + MATCH ranked
-- retrieval (bag-of-words BM25 top-K), LIMIT top-K pushdown, and the
-- position-free contract (boolean mode rejected).
drop database if exists bm25_basic;
create database bm25_basic;
use bm25_basic;
create table docs (id bigint primary key, body text);
insert into docs values (1,'apple banana cherry'),(2,'apple banana'),(3,'apple'),(4,'durian mango'),(5,'apple apple apple banana');
create index ftx using bm25 on docs(body) with parser gojieba;
-- membership: docs containing 'apple' (doc 4 excluded)
select id from docs where match(body) against('apple') order by id;
-- ranked by BM25 score DESC (no ORDER BY): doc 5 (apple x3) first, doc 1 (longest) last
select id from docs where match(body) against('apple');
-- multi-term bag-of-words
select id from docs where match(body) against('apple banana') order by id;
-- LIMIT top-K pushdown: the two highest-scored docs
select id from docs where match(body) against('apple') limit 2;
-- position-free contract: boolean mode is rejected
select id from docs where match(body) against('apple' in boolean mode);
drop database bm25_basic;
