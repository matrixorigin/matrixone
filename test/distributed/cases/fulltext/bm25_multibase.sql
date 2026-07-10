-- bm25 with max_index_capacity=2 over 6 rows -> the tag=0 base is split into
-- multiple sub-indexes; the search loads + merges all subs. Ported from
-- fulltext_retrieval_multibase.sql.
drop database if exists bm25_multibase;
create database bm25_multibase;
use bm25_multibase;
create table t (id bigint primary key, txt text);
insert into t values
 (1,'apple banana'),
 (2,'banana cherry'),
 (3,'cherry date'),
 (4,'date apple'),
 (5,'elderberry fig'),
 (6,'fig grape');
create index ft using bm25 on t(txt) with parser gojieba max_index_capacity=2;
select id from t where bm25(txt) against('apple') order by id;
select id from t where bm25(txt) against('cherry') order by id;
select id from t where bm25(txt) against('banana') order by id;
select id from t where bm25(txt) against('fig') order by id;
drop database bm25_multibase;
