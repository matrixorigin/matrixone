-- ALTER ... REINDEX <idx> FULLTEXT MERGE runs incremental fold+tiered compaction over a
-- WAND "retrieval" index's already-built segments (no re-tokenize): it folds the tag=1 CDC
-- tail into the tag=0 base and coalesces small subs. Deleted docs stay deleted and updated
-- docs resolve to their newest version across folds; MERGE on a non-retrieval index is rejected.
--
-- Multi-CN note: the per-CN search cache is not cross-CN invalidated, so we issue NO MATCH
-- until the very end — the final checks are the querying CN's first load, reading the fresh
-- folded base. The only waits are the modest CDC settle before each MERGE (the merge folds
-- the tag=1 tail, which the ISCP sinker must have written to the store first).
drop database if exists ft_merge;
create database ft_merge;
use ft_merge;
create table t (id bigint primary key, txt text);
-- base rows present at CREATE -> tag=0 base built synchronously
insert into t values (1,'apple red'),(2,'apple green'),(3,'apple blue');
create fulltext index ft on t(txt) with parser retrieval;
-- round 1: CDC insert + delete flow into the tag=1 tail, then MERGE folds it into the base
insert into t values (4,'apple yellow');
delete from t where id=2;
select sleep(30);
alter table t alter reindex ft fulltext merge;
-- round 2: CDC insert + update, then a second MERGE (multi-fold + tiered coalesce)
insert into t values (5,'apple pink');
update t set txt='apple orange' where id=4;
select sleep(30);
alter table t alter reindex ft fulltext merge;
-- final checks (first MATCH on the querying CN -> fresh load of the folded base):
-- deleted doc 2 gone, updated doc 4 resolves to its newest version (orange, not yellow)
select id from t where match(txt) against('apple' in retrieval mode) order by id;
select id from t where match(txt) against('green' in retrieval mode) order by id;
select id from t where match(txt) against('orange' in retrieval mode) order by id;
select id from t where match(txt) against('yellow' in retrieval mode) order by id;
-- non-retrieval (ngram) fulltext index: a plain REBUILD works (clear + re-tokenize from
-- source, idempotent), but MERGE (WAND compaction) is rejected with an explicit
-- retrieval-only message. The classic index queries the postings table directly (no
-- per-CN cache), so MATCH here is always fresh.
create table n (id bigint primary key, txt text);
insert into n values (1,'hello world'),(2,'hello mo');
create fulltext index fn on n(txt);
select id from n where match(txt) against('hello') order by id;
alter table n alter reindex fn fulltext;
select id from n where match(txt) against('hello') order by id;
alter table n alter reindex fn fulltext merge;
drop database ft_merge;
