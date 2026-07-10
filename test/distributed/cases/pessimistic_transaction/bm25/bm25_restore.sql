-- Snapshot + RESTORE of a bm25 index: the restore rebuilds the index from the
-- restored rows (RestoreInitSQL), rolling back the post-snapshot mutation;
-- post-restore rows flow in via the re-armed CDC. Ported from
-- fulltext_retrieval_restore.
drop database if exists bm25_restore;
drop snapshot if exists sn_bm25_restore;
create database bm25_restore;
use bm25_restore;
create table t (id bigint primary key, txt text);
insert into t values (1,'apple banana'),(2,'banana cherry'),(3,'cherry date'),(4,'date apple');
create index ft using bm25 on t(txt) with parser gojieba max_index_capacity=100;
select id from t where bm25(txt) against('apple') order by id;
create snapshot sn_bm25_restore for account sys;
insert into t values (5,'fig apple');
restore database bm25_restore {snapshot = "sn_bm25_restore"};
use bm25_restore;
insert into t values (6,'grape apple');
select sleep(30);
select id from t where bm25(txt) against('apple') order by id;
select id from t where bm25(txt) against('grape') order by id;
drop database bm25_restore;
drop snapshot if exists sn_bm25_restore;
