-- A classic fulltext index and a bm25 index on the SAME column coexist; MATCH
-- routes by mode: IN BOOLEAN MODE -> the classic postings index (bm25 has no
-- operators), default / NATURAL LANGUAGE -> the bm25 ranked index. Same-category
-- duplicates (two classic, or two bm25) on one column stay rejected. Ported from
-- fulltext_retrieval_coexist.sql (retrieval index -> bm25).
drop database if exists bm25_coexist;
create database bm25_coexist;
use bm25_coexist;
create table t (id bigint primary key, txt text);
insert into t values (1,'apple banana cherry'),(2,'apple banana'),(3,'apple'),(4,'durian'),(5,'apple apple apple banana');
create fulltext index ftc on t(txt);
create index ftr using bm25 on t(txt) with parser gojieba;
-- boolean -> classic postings index (OR bag-of-words over any term)
select id from t where match(txt) against('apple banana cherry' in boolean mode) order by id;
-- default -> bm25 ranked top-K (BM25 score DESC)
select id from t where match(txt) against('apple');
-- natural language -> bm25 ranked bag-of-words
select id from t where match(txt) against('apple' in natural language mode);
-- same-category duplicate on the same column is still rejected
create fulltext index ftc2 on t(txt);
create index ftr2 using bm25 on t(txt) with parser gojieba;
drop database bm25_coexist;
