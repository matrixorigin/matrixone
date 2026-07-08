-- ALTER ... REINDEX <idx> FULLTEXT MERGE runs incremental fold+tiered compaction over a
-- WAND "retrieval" index's already-built segments (no re-tokenize): it folds the tag=1 CDC
-- tail into the tag=0 base and coalesces small subs, in the ALTER's txn. Plain
-- ALTER ... FULLTEXT stays a full rebuild-from-source. Deleted docs stay deleted and updated
-- docs resolve to their newest version across folds; MERGE on a non-retrieval index is rejected.
drop database if exists ft_merge;
create database ft_merge;
use ft_merge;
create table t (id bigint primary key, txt text);
-- base rows present at CREATE -> tag=0 base built synchronously
insert into t values (1,'apple red'),(2,'apple green'),(3,'apple blue');
create fulltext index ft on t(txt) with parser retrieval;
select id from t where match(txt) against('apple' in retrieval mode) order by id;
-- CDC: a new row + a delete of a base row flow into the tag=1 tail
insert into t values (4,'apple yellow');
delete from t where id=2;
select sleep(20);
select id from t where match(txt) against('apple' in retrieval mode) order by id;
-- MERGE: fold the tail into the base; deleted doc 2 must not resurface
alter table t alter reindex ft fulltext merge;
select id from t where match(txt) against('apple' in retrieval mode) order by id;
select id from t where match(txt) against('green' in retrieval mode) order by id;
-- second CDC round (insert + update) then a second MERGE (multi-fold + tiered coalesce)
insert into t values (5,'apple pink');
update t set txt='apple orange' where id=4;
select sleep(20);
alter table t alter reindex ft fulltext merge;
select id from t where match(txt) against('apple' in retrieval mode) order by id;
select id from t where match(txt) against('orange' in retrieval mode) order by id;
select id from t where match(txt) against('yellow' in retrieval mode) order by id;
-- plain reindex (full rebuild-from-source) yields the same result set
alter table t alter reindex ft fulltext;
select id from t where match(txt) against('apple' in retrieval mode) order by id;
-- non-retrieval (ngram) fulltext index: a plain REBUILD works (clear + re-tokenize from
-- source, idempotent), but MERGE (WAND compaction) is rejected with an explicit
-- retrieval-only message (not the raw JSON parse error it used to surface).
create table n (id bigint primary key, txt text);
insert into n values (1,'hello world'),(2,'hello mo');
create fulltext index fn on n(txt);
select id from n where match(txt) against('hello') order by id;
alter table n alter reindex fn fulltext;
select id from n where match(txt) against('hello') order by id;
alter table n alter reindex fn fulltext merge;
drop database ft_merge;
