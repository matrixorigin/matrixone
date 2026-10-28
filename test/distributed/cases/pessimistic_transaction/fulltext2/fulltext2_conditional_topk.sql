-- Conditional exact stream TopK: deterministic PK ties, page boundaries and runtime visibility.
-- Twelve rows keep alpha positive under BM25; the eight matching bodies are identical.
-- Column 1 score display is ignored; IDs/order are hand oracles and native FLOAT bits are
-- independently covered by the real-service baseline/candidate harness.
set @previous_experimental_fulltext2_index = @@session.experimental_fulltext2_index;
set @previous_ft2_relevancy_algorithm = @@session.ft2_relevancy_algorithm;
set experimental_fulltext2_index = 1;
set ft2_relevancy_algorithm = 'BM25';
drop database if exists ft2_conditional_topk;
create database ft2_conditional_topk;
use ft2_conditional_topk;
create table fulltext_documents (doc_id varchar(255) primary key, body longtext);
insert into fulltext_documents values ('z','alpha omega'),('b','alpha omega'),('aa','alpha omega'),('a','alpha omega'),('2','alpha omega'),('10','alpha omega'),('1','alpha omega'),('02','alpha omega'),('n0','beta gamma'),('n1','beta gamma'),('n2','beta gamma'),('n3','beta gamma');
create fulltext2 index ft_body on fulltext_documents(body) with parser gojieba position_free = true;
-- Equal scores must retain byte-order PK ties through the literal TopK boundary.
-- @ignore:1
select doc_id, match(body) against('alpha' in bm25 mode) as relevance from fulltext_documents where match(body) against('alpha' in bm25 mode) > 0 order by relevance desc, doc_id limit 3;
-- A literal OFFSET collects LIMIT+OFFSET before the outer slice.
-- @ignore:1
select doc_id, match(body) against('alpha' in bm25 mode) as relevance from fulltext_documents where match(body) against('alpha' in bm25 mode) > 0 order by relevance desc, doc_id limit 3 offset 1;
-- @ignore:1
select doc_id, match(body) against('alpha' in bm25 mode) as relevance from fulltext_documents where match(body) against('alpha' in bm25 mode) > 0 order by relevance desc, doc_id limit 0;
-- @ignore:1
select doc_id, match(body) against('nomatchtoken' in bm25 mode) as relevance from fulltext_documents where match(body) against('nomatchtoken' in bm25 mode) > 0 order by relevance desc, doc_id limit 3;
-- Residual filtering must not lose qualifying rows to an early bound.
-- @ignore:1
select doc_id, match(body) against('alpha' in bm25 mode) as relevance from fulltext_documents where match(body) against('alpha' in bm25 mode) > 0 and doc_id >= 'a' order by relevance desc, doc_id limit 3;
prepare ft_cached from 'select doc_id,relevance from (select doc_id,match(body) against(? in bm25 mode) as relevance from fulltext_documents) q where relevance>0 order by relevance desc,doc_id limit 3';
set @term = 'alpha';
-- One prepared MATCH identity is eligible, but visibility must be rechecked per execution.
-- @ignore:1
execute ft_cached using @term;
begin;
delete from fulltext_documents where doc_id in ('02','1','10');
-- Own writes must remain visible even if a prepared plan previously used TopK.
-- @ignore:1
execute ft_cached using @term;
rollback;
-- @ignore:1
execute ft_cached using @term;
deallocate prepare ft_cached;
delete from fulltext_documents where doc_id in ('02','1');
-- Committed source deletions must retain the membership join and safe fallback.
-- @ignore:1
select doc_id, match(body) against('alpha' in bm25 mode) as relevance from fulltext_documents where match(body) against('alpha' in bm25 mode) > 0 order by relevance desc, doc_id limit 3;
drop database ft2_conditional_topk;
set ft2_relevancy_algorithm = @previous_ft2_relevancy_algorithm;
set experimental_fulltext2_index = @previous_experimental_fulltext2_index;
