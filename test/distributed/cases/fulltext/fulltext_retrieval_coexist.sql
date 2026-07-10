-- Two fulltext indexes on the SAME column: a classic (postings) index and a retrieval
-- (WAND) index. Allowed because the query MODE routes: IN RETRIEVAL MODE -> the retrieval
-- index, IN BOOLEAN / NATURAL LANGUAGE / DEFAULT -> the classic index. Same-category
-- duplicates (two classic, or two retrieval, on one column) stay rejected.
drop database if exists ft_coexist;
create database ft_coexist;
use ft_coexist;
create table t (id bigint primary key, txt text);
insert into t values (1,'apple banana cherry'),(2,'apple banana'),(3,'apple'),(4,'durian');
create fulltext index ftc on t(txt);
create fulltext index ftr on t(txt) with parser retrieval;
-- The retrieval index's initial build is synchronous (like HNSW), so no wait is needed.
-- All modes sort by score DESC by default. The query 'apple banana cherry' separates the
-- three semantics AND gives distinct scores, so no ORDER BY tie-breaker is needed:
--   boolean (classic)            -> OR bag-of-words, BM25-ranked; docs match 3/2/1 terms
--                                   -> distinct scores -> [1,2,3]
--   natural language / default   -> PHRASE search; only doc 1 has the exact phrase -> [1]
--   retrieval (WAND)             -> OR bag-of-words, BM25-ranked -> distinct scores [1,2,3]
-- BOOLEAN / NATURAL LANGUAGE / DEFAULT -> classic postings index
select id from t where match(txt) against('apple banana cherry' in boolean mode);
select id from t where match(txt) against('apple banana cherry' in natural language mode);
select id from t where match(txt) against('apple banana cherry');
-- RETRIEVAL -> retrieval (WAND) index
select id from t where match(txt) against('apple banana cherry' in retrieval mode);
-- same-category duplicate on the same column is still rejected
create fulltext index ftc2 on t(txt);
create fulltext index ftr2 on t(txt) with parser retrieval;
drop database ft_coexist;
