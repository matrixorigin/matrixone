-- Issue #30955: under REPEATABLE READ a locking read must read the latest
-- committed state of the rows it locks (InnoDB current read), while plain
-- reads in the same transaction keep the SI snapshot.
-- Sequence uses explicit session ordering (no sleeps):
--   session A: RR, BEGIN, plain read sees v=100;
--   session B: autocommit UPDATE 100 -> 999 (committed before A's lock read);
--   session A: FOR UPDATE must return 999 (current read);
--   session A: plain read must still return 100 (RR snapshot intact);
--   session A: COMMIT, then plain read returns 999.
drop table if exists su_fu_rr;
create table su_fu_rr(id int primary key, v int);
insert into su_fu_rr values (1, 100);
set session transaction isolation level repeatable read;
begin;
select v from su_fu_rr where id = 1;
-- @session:id=1{
use select_for_update_rr_current_read;
update su_fu_rr set v = 999 where id = 1;
select v from su_fu_rr where id = 1;
-- @session}
select v from su_fu_rr where id = 1 for update;
select v from su_fu_rr where id = 1;
commit;
select v from su_fu_rr where id = 1;

-- FOR SHARE returns the same current value (locking read), then a new
-- transaction observes the newer commit through its own plain reads.
set session transaction isolation level repeatable read;
begin;
select v from su_fu_rr where id = 1;
-- @session:id=1{
use select_for_update_rr_current_read;
update su_fu_rr set v = 333 where id = 1;
-- @session}
select v from su_fu_rr where id = 1 for share;
select v from su_fu_rr where id = 1 lock in share mode;
commit;
select v from su_fu_rr where id = 1;

-- Locking-read candidate discovery also current-reads: a row whose new
-- value only exists after this transaction's snapshot is discovered by the
-- locked occurrence's predicate (base rediscovered nothing at the snapshot).
drop table if exists su_fu_rr_disc;
create table su_fu_rr_disc(id int primary key, v int);
insert into su_fu_rr_disc values (1, 100);
begin;
select v from su_fu_rr_disc where v = 778 for update;
-- @session:id=1{
use select_for_update_rr_current_read;
update su_fu_rr_disc set v = 778 where id = 1;
-- @session}
select v from su_fu_rr_disc where v = 778 for update;
select v from su_fu_rr_disc where id = 1;
commit;
select v from su_fu_rr_disc where id = 1;

-- Same-table dual role: the locked outer occurrence current-reads while a
-- scalar subquery occurrence in the same statement keeps the SI snapshot.
-- The peer animates both roles (id=1 -> 888, id=2 -> 999): only the
-- current-outer/snapshot-subquery contract returns row 1 here — an
-- all-current implementation sees the raised subquery (888 > 999 = no
-- row) and an all-snapshot implementation keeps 100 > 100 = no row.
drop table if exists su_fu_rr_dual;
create table su_fu_rr_dual(id int primary key, v int);
insert into su_fu_rr_dual values (1, 100), (2, 100);
begin;
-- before the peer commit both implementations agree: 100 > 100 selects
-- nothing.
select o.id
from su_fu_rr_dual o
where o.v > (select i.v from su_fu_rr_dual i where i.id = 2 limit 1)
and o.id = 1
for update;
-- @session:id=1{
use select_for_update_rr_current_read;
update su_fu_rr_dual set v = 999 where id = 2;
update su_fu_rr_dual set v = 888 where id = 1;
-- @session}
select o.id
from su_fu_rr_dual o
where o.v > (select i.v from su_fu_rr_dual i where i.id = 2 limit 1)
and o.id = 1
for update;
-- plain snapshot read still shows the snapshot value for row 1
select v from su_fu_rr_dual where id = 1;
commit;
select v from su_fu_rr_dual where id = 1;

-- Lock release: after ROLLBACK a peer can take the row lock immediately.
drop table if exists su_fu_rr_rel;
create table su_fu_rr_rel(id int primary key, v int);
insert into su_fu_rr_rel values (1, 100);
begin;
select v from su_fu_rr_rel where id = 1 for update;
rollback;
-- @session:id=1{
use select_for_update_rr_current_read;
set session lock_wait_timeout = 3;
begin;
update su_fu_rr_rel set v = 556 where id = 1;
select v from su_fu_rr_rel where id = 1;
rollback;
-- @session}
select v from su_fu_rr_rel where id = 1;

-- READ COMMITTED keeps its established behavior: the locking read (and the
-- plain reads after the peer commit) see the newly committed value.
drop table if exists su_fu_rr_rc;
create table su_fu_rr_rc(id int primary key, v int);
insert into su_fu_rr_rc values (1, 100);
set session transaction isolation level read committed;
begin;
select v from su_fu_rr_rc where id = 1;
-- @session:id=1{
use select_for_update_rr_current_read;
update su_fu_rr_rc set v = 777 where id = 1;
-- @session}
select v from su_fu_rr_rc where id = 1 for update;
select v from su_fu_rr_rc where id = 1;
commit;
select v from su_fu_rr_rc where id = 1;

-- cleanup
drop table if exists su_fu_rr;
drop table if exists su_fu_rr_disc;
drop table if exists su_fu_rr_dual;
drop table if exists su_fu_rr_rel;
drop table if exists su_fu_rr_rc;

-- Lazy-branch UNION with an outer LIMIT (pre-PR review round 2): the
-- branch-locking UNION form defers its branch data sources to branch start,
-- so the locking statement completes its execution-read admission BEFORE any
-- branch can produce rows; both branches' locked reads current-read the
-- peer's commit while the plain snapshot reads keep 100.
drop table if exists su_fu_rr_lazyu;
create table su_fu_rr_lazyu(id int primary key, v int);
insert into su_fu_rr_lazyu values (1, 100), (2, 100);
set session transaction isolation level repeatable read;
begin;
select id from su_fu_rr_lazyu where id = 1;
-- @session:id=1{
use select_for_update_rr_current_read;
update su_fu_rr_lazyu set v = 888 where id = 1;
update su_fu_rr_lazyu set v = 999 where id = 2;
-- @session}
(select v from su_fu_rr_lazyu where id = 1 for update)
union all
(select v from su_fu_rr_lazyu where id = 2 for update)
limit 2;
-- plain snapshot reads keep the snapshot values
select v from su_fu_rr_lazyu where id = 1;
select v from su_fu_rr_lazyu where id = 2;
commit;
select v from su_fu_rr_lazyu where id = 1;
select v from su_fu_rr_lazyu where id = 2;

-- Correlated FOR UPDATE (pre-PR review round 2): the locked OUTER occurrence
-- current-reads the peer's commits (888) while the nested correlated reads
-- keep their SI snapshot (50) in the SAME statement — an all-current
-- implementation returns the raised nested read (999) and an
-- all-snapshot implementation returns the stale 100, so only the delivered
-- current-outer/snapshot-nested contract passes.
drop table if exists su_fu_rr_coro_o;
drop table if exists su_fu_rr_coro_i;
create table su_fu_rr_coro_o(id int primary key, v int);
create table su_fu_rr_coro_i(id int primary key, v int);
insert into su_fu_rr_coro_o values (1, 100);
insert into su_fu_rr_coro_i values (1, 50);
set session transaction isolation level repeatable read;
begin;
select v from su_fu_rr_coro_o where id = 1;
select v from su_fu_rr_coro_i where id = 1;
-- @session:id=1{
use select_for_update_rr_current_read;
update su_fu_rr_coro_o set v = 888 where id = 1;
update su_fu_rr_coro_i set v = 999 where id = 1;
-- @session}
select o.v
from su_fu_rr_coro_o o
where o.id = 1
and exists (select 1 from su_fu_rr_coro_i i where i.id = o.id)
for update;
select (select v from su_fu_rr_coro_i i where i.id = o.id)
from su_fu_rr_coro_o o
where o.id = 1;
select v from su_fu_rr_coro_o where id = 1 for update;
select v from su_fu_rr_coro_o where id = 1;
select v from su_fu_rr_coro_i where id = 1;
commit;
select v from su_fu_rr_coro_o where id = 1;
select v from su_fu_rr_coro_i where id = 1;

-- Deep correlated-projection FOR UPDATE (pre-PR review round 3): the LOCKING
-- statement itself animates BOTH values through the plan family the
-- production optimizer keeps on the dependent (Correlation) path — the
-- nested-scalar COUNT-chain form pinned by the compile member
-- TestDependentForUpdateDeliveredBvtShapeIsCorrelationPlan. The outer locked
-- occurrence current-reads the commit (888); the NESTED correlated chain in
-- the SAME statement reads the child table THROUGH the outer row and keeps
-- its SI snapshot: over the snapshot rows (50, 60) only one has v >= 55
-- (count 1), while the committed rows (999, 60) both do (count 2). An
-- all-current implementation reports (888, 2); an all-snapshot implementation
-- reports (100, 1); only the delivered current-outer / snapshot-nested
-- contract reports (888, 1) with the plain reads back at the snapshot
-- (100, 50).
drop table if exists su_fu_rr_dee_o;
drop table if exists su_fu_rr_dee_i;
create table su_fu_rr_dee_o(id int primary key, v int);
create table su_fu_rr_dee_i(id int primary key, parent int, v int);
insert into su_fu_rr_dee_o values (1, 100);
insert into su_fu_rr_dee_i values (1, 1, 50);
insert into su_fu_rr_dee_i values (2, 1, 60);
set session transaction isolation level repeatable read;
begin;
select v from su_fu_rr_dee_o where id = 1;
select v from su_fu_rr_dee_i where id = 1;
-- @session:id=1{
use select_for_update_rr_current_read;
update su_fu_rr_dee_o set v = 888 where id = 1;
update su_fu_rr_dee_i set v = 999 where id = 1;
-- @session}
select o.v as outer_locked,
       (select (select count(*) from su_fu_rr_dee_i i
                where i.parent = o.id and i.v >= 55)) as nested_correlated
from su_fu_rr_dee_o o
where o.id = 1
for update;
select v from su_fu_rr_dee_o where id = 1;
select v from su_fu_rr_dee_i where id = 1;
commit;
select v from su_fu_rr_dee_o where id = 1;
select v from su_fu_rr_dee_i where id = 1;

-- Covering hinted locking read (pre-PR review round 4, finding 1): the FORCE
-- INDEX FOR ORDER BY hint routes this locked query through the hinted
-- COVERING-index constructor (tryHintedCoveringIndexScan). Before the rev-18
-- provenance repair the fabricated covering scan carried a zero scan role and
-- no owning-table id, so compile's covered-index admission rejected it as a
-- vanished mark and the locking read silently kept the SI snapshot - the
-- locked occurrence could MISS A NEWLY MATCHING COMMITTED ROW, the exact
-- shape animated here: the peer commits a brand-new row (id=2) matching the
-- covered predicate after this transaction's snapshot was established.
-- Repaired: the covering locked read current-reads and returns BOTH rows
-- (1,5) and (2,5); the plain reads keep the SI snapshot (only row 1) with no
-- wait exception; after COMMIT the plain read sees both.
drop table if exists su_fu_rr_cov;
create table su_fu_rr_cov(id int primary key, val int, extra int, key idx_extra(extra));
insert into su_fu_rr_cov values (1, 100, 5);
begin;
select id, extra from su_fu_rr_cov force index for order by(idx_extra)
where extra = 5 order by extra, id;
-- @session:id=1{
use select_for_update_rr_current_read;
insert into su_fu_rr_cov values (2, 200, 5);
-- @session}
select id, extra from su_fu_rr_cov force index for order by(idx_extra)
where extra = 5 order by extra, id for update;
select id, extra from su_fu_rr_cov where extra = 5 order by id;
commit;
select id, extra from su_fu_rr_cov where extra = 5 order by id;

-- Return the shared default session to its default state: mo-tester reuses
-- one default connection across every case file of a run, and the
-- object-lifecycle gate in frontend/txn.go refuses DDL inside an active
-- transaction that is not pessimistic READ-COMMITTED, so leaving this
-- session at REPEATABLE READ would break every later case that runs DDL
-- inside BEGIN (special.sql was the first CI victim). Same restore
-- convention as locking_read_limit_retry.sql.
set session transaction_isolation=default;
