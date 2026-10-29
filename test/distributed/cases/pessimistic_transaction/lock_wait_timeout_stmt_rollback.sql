-- @suite
-- @case
-- @desc: an expired lock wait rolls back only the failing statement (MySQL innodb_rollback_on_timeout=OFF); the transaction stays open and a following COMMIT keeps earlier writes, while the failed statement leaves no partial rows
-- @label:bvt

-- The rollback scope is what an application's retry logic depends on: a
-- waiter that hits lock_wait_timeout keeps everything its transaction wrote
-- before the failed statement, so the later COMMIT still persists it. The
-- failed statement itself is rolled back as a whole, so no row it touched
-- before the contention keeps a partial effect.
drop database if exists lock_wait_stmt_rollback_db;
create database lock_wait_stmt_rollback_db;
use lock_wait_stmt_rollback_db;

create table t(id int primary key, v int);
insert into t values (1, 11), (2, 22), (3, 33), (4, 44);
create table b(id int primary key, v int);
insert into b values (1, 1);

set @saved_lock_timeout = @@session.lock_wait_timeout;
set session lock_wait_timeout = 1;

-- The holder locks rows 2 and 4 and keeps its transaction open across blocks.
-- @session:id=1{
use lock_wait_stmt_rollback_db;
begin;
update t set v = 100 where id in (2, 4);
-- @session}

-- The waiter: one successful write, then a statement that locks row 1,
-- blocks on row 2 and expires.
begin;
update b set v = 77 where id = 1;
-- @regex("(?s)Lock wait timeout exceeded; try restarting transaction",true)
update t set v = v + 1 where id in (1, 2, 3, 4);
-- The transaction is still open: the in-transaction view shows the earlier
-- b write, not the pre-transaction committed value.
select v as txn_still_open from b where id = 1;
update b set v = 78 where id = 1;
commit;
select v as b_committed from b where id = 1;

-- @session:id=1{
commit;
-- @session}

-- The failed statement left no partial rows: v+1 appears on no row, while
-- the holder's committed values are visible on rows 2 and 4.
select id, v from t order by id;

set session lock_wait_timeout = @saved_lock_timeout;
drop database lock_wait_stmt_rollback_db;
