-- fulltext2 token_filter: the Lucene-style analyzer chain. Step 1 supports the "stop"
-- filter, which drops stopwords (gojieba's builtin English+Chinese stop_words.utf8) at
-- BUILD, CDC and the BM25 query, shrinking the index. It requires POSITION_FREE=TRUE (a
-- bag-of-words index): removing a stopword's postings leaves a positional gap a phrase
-- query cannot reason about, so NL / BOOLEAN are unsupported (POSITION_FREE already
-- rejects them). POSITION_FREE also requires the gojieba word parser, so token_filter
-- inherits that too.
set experimental_fulltext2_index = 1;
drop database if exists ft2_tokfilter;
create database ft2_tokfilter;
use ft2_tokfilter;

create table t (id bigint primary key, body text);
insert into t values
 (1,'the quick brown fox'),
 (2,'a lazy dog'),
 (3,'the fox and the hound'),
 (4,'brown bear'),
 (5,'and the a of');

-- token_filter requires POSITION_FREE=TRUE: rejected without it (default parser) ...
create fulltext2 index bad1 on t(body) token_filter = 'stop';
-- ... and rejected with the gojieba parser but no POSITION_FREE ...
create fulltext2 index bad2 on t(body) with parser gojieba token_filter = 'stop';
-- ... POSITION_FREE requires gojieba, so ngram + POSITION_FREE is rejected before token_filter ...
create fulltext2 index bad3 on t(body) with parser ngram position_free = true token_filter = 'stop';
-- ... and an unknown filter name is rejected.
create fulltext2 index bad4 on t(body) with parser gojieba position_free = true token_filter = 'bogus';

-- valid: gojieba + POSITION_FREE=TRUE + token_filter='stop'
create fulltext2 index ft on t(body) with parser gojieba position_free = true token_filter = 'stop';
select algo_params from mo_catalog.mo_indexes where name = 'ft' and algo_params like '%parser%' limit 1;
-- SHOW CREATE renders token_filter so a checkpoint-restore DDL rebuilds it.
show create table t;

-- IN BM25 MODE: a content word matches its docs.
select id from t where match(body) against('fox' in bm25 mode) order by id;
select id from t where match(body) against('brown' in bm25 mode) order by id;
-- a pure stopword is NOT in the index -> no rows.
select id from t where match(body) against('the' in bm25 mode) order by id;
-- a mixed query drops the stopword, so 'the fox' behaves like 'fox'.
select id from t where match(body) against('the fox' in bm25 mode) order by id;
-- a query of only stopwords matches nothing.
select id from t where match(body) against('and the a of' in bm25 mode) order by id;
-- doc 5 is all stopwords -> indexed empty -> never a BM25 hit.
select id from t where match(body) against('lazy dog' in bm25 mode) order by id;

-- NL / BOOLEAN need positions -> rejected on this POSITION_FREE token_filter index.
select id from t where match(body) against('the fox' in boolean mode) order by id;
select id from t where match(body) against('the fox' in natural language mode) order by id;

-- REINDEX cannot turn POSITION_FREE off on a token_filter index (bag-of-words only):
-- it would leave a positional index whose NL/boolean queries can't match stripped stopwords.
alter table t alter reindex ft fulltext2 position_free = false;
-- re-asserting POSITION_FREE=TRUE on the REINDEX is fine, token_filter is preserved.
alter table t alter reindex ft fulltext2 position_free = true;
select algo_params from mo_catalog.mo_indexes where name = 'ft' and algo_params like '%token_filter%' limit 1;
select id from t where match(body) against('the fox' in bm25 mode) order by id;

-- No segment holds a stopword posting, on the CREATE build and REINDEX (the CDC tail is
-- checked in pessimistic_transaction/fulltext2/fulltext2_token_filter_cdc.sql). A
-- document's indexed length feeds BM25, so 'fox' and 'the the the the fox' score equally
-- only when the stopwords were stripped; the unfiltered index p scores them differently.
create table s (id bigint primary key, body text);
insert into s values (1, 'fox'), (2, 'the the the the fox'), (3, 'bear');
create fulltext2 index sx on s(body) with parser gojieba position_free = true token_filter = 'stop';
create table p (id bigint primary key, body text);
insert into p values (1, 'fox'), (2, 'the the the the fox'), (3, 'bear');
create fulltext2 index px on p(body) with parser gojieba position_free = true;
select count(distinct match(body) against('fox' in bm25 mode)) as scores from s where match(body) against('fox' in bm25 mode);
select count(distinct match(body) against('fox' in bm25 mode)) as scores from p where match(body) against('fox' in bm25 mode);
alter table s alter reindex sx fulltext2 position_free = true;
select count(distinct match(body) against('fox' in bm25 mode)) as scores from s where match(body) against('fox' in bm25 mode);

drop database ft2_tokfilter;
