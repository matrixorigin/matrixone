-- Snapshot + RESTORE of a WAND "retrieval" fulltext index. The restore replays the
-- table via CREATE TABLE ... CLONE; Scope.RestoreTable rebuilds the tag=0 base from the
-- restored rows via the plugin RestoreInitSQL (ALTER ... REINDEX ... FULLTEXT FORCE_SYNC,
-- run as the re-armed CDC's InitSQL). This exercises the regression fixed in 3edae8b5c:
-- without the rebuild the block-clone appends the source base onto CreateTable's seed,
-- leaving a doubled tag=0 base. Post-restore rows flow in via the re-armed CDC.
-- The restore's async reindex AND the post-restore CDC insert are both async; issue both
-- up front, then a SINGLE settle wait, and check the converged state.
drop database if exists ft_restore;
drop snapshot if exists sn_ft_restore;
create database ft_restore;
use ft_restore;
create table t (id bigint primary key, txt text);
insert into t values (1,'apple banana'),(2,'banana cherry'),(3,'cherry date'),(4,'date apple');
create fulltext index ft on t(txt) with parser retrieval max_index_capacity=100;
select id from t where match(txt) against('apple' in retrieval mode) order by id;
create snapshot sn_ft_restore for account sys;
-- mutate after the snapshot; the restore must roll this back
insert into t values (5,'fig apple');
restore database ft_restore {snapshot = "sn_ft_restore"};
use ft_restore;
-- CDC re-armed: a post-restore insert
insert into t values (6,'grape apple');

-- single settle wait for the restore's async reindex + the post-restore CDC insert
select sleep(60);

-- converged: restored base with row 5 rolled back, plus the post-restore insert (6)
select id from t where match(txt) against('apple' in retrieval mode) order by id;
select id from t where match(txt) against('grape' in retrieval mode) order by id;
drop database ft_restore;
drop snapshot if exists sn_ft_restore;
