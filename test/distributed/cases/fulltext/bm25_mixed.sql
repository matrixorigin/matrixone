-- A single query can combine BM25() and classic MATCH() — one on a bm25-indexed
-- column, the other on a fulltext-indexed column. The planner serves both in ONE
-- pass: one join chain (bm25_search + fulltext_index_scan) feeding one sort keyed by
-- BOTH scores. This is the payoff of the unified match-rewrite loop.
drop database if exists bm25_mixed;
create database bm25_mixed;
use bm25_mixed;
create table t (id bigint primary key, a text, b text);
insert into t values
(1,'apple banana','cat dog'),
(2,'apple','dog'),
(3,'cherry','cat'),
(4,'apple apple','cat cat'),
(5,'banana','bird');
create index ba using bm25 on t(a) with parser gojieba;
create fulltext index fb on t(b);
-- BM25(a) apple ∩ MATCH(b) cat  -> docs 1,4 (apple in a AND cat in b)
select id from t where bm25(a) against('apple') and match(b) against('cat');
-- order of the two verbs does not matter
select id from t where match(b) against('cat') and bm25(a) against('apple');
-- BM25(a) apple ∩ MATCH(b) dog -> docs 1,2
select id from t where bm25(a) against('apple') and match(b) against('dog');
-- combined with a normal SQL filter
select id from t where bm25(a) against('apple') and match(b) against('cat') and id > 1;
-- no overlap -> empty (banana in a, but cat in b matches none of the banana docs)
select id from t where bm25(a) against('cherry') and match(b) against('dog');
drop database bm25_mixed;
