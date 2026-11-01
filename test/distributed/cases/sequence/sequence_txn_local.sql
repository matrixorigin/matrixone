-- Transaction-local sequence generations (explicit-transaction CLONE) and
-- the issue-once protocol (design §4 D / I1; v10 per §0 I5):
--   * rolled-back statements must not rewind issued values (K1 repair) on a
--     GENUINE local generation: the clone, the failing statement and the
--     retry share one explicit transaction (v12-review F10),
--   * SETVAL is a deliberate obligation boundary on the local route (J2),
--   * CTAS population inside the transaction shares the session obligation
--     store through the internal child process (v12-review F5),
--   * drop-and-recreate inside one transaction routes by generation
--     identity (db-id + rel-id), never by name into the old committed
--     generation (v12-review F4),
--   * no composition below ever routes an allocation by name existence,
--   * foreground sequence DDL stays rejected inside explicit transactions
--     (supported-path controls),
--   * a merge whose destination database exists only inside the caller's
--     uncommitted transaction is rejected atomically with ERROR 20101
--     (pre-existing merge-machinery catalog boundary, independent of
--     sequences) — pinned by the boundary control below; the merge
--     reconcile protocol itself is covered on the committed route here and
--     by git4data/branch/merge/merge_sequence_reconcile.sql, and the
--     transaction-local preserve arm of the commit stamp is covered by the
--     deterministic UTs, promotable to BVT once the probe is RYOW-correct.
drop database if exists stl_base;
drop database if exists stl_br;
drop database if exists stl_brd;
drop database if exists stl_dst;
drop database if exists stl_dstr;
drop database if exists stl_dstc;
drop database if exists stl_dstdc;
drop database if exists stl_k1;
drop database if exists stl_j2;
drop database if exists stl_j2p;
drop database if exists stl_ctas;
drop database if exists stl_sh1;
drop database if exists stl_shd;
create database stl_base;
use stl_base;
create sequence s as bigint increment by 1 start with 1 no cycle;
create table t (id bigint primary key default nextval('s'), v int);
insert into t(v) values (10);
data branch create database stl_br from stl_base;
insert into stl_br.t(id, v) values (2, 20), (3, 30);

-- I1 boundary control (v10, §0 I5): explicit-txn clone, a default INSERT
-- (issues 2), then the merge is rejected before any row apply; the
-- transaction stays usable and COMMIT persists the pre-merge position.
begin;
create database stl_dst clone stl_base;
insert into stl_dst.t(v) values (99);
-- @regex("cannot find database", true)
data branch merge stl_br.t into stl_dst.t when conflict accept;
select id, v from stl_dst.t order by id;
select last_seq_num, is_called from stl_dst.s;
commit;
use stl_dst;
select id, v from t order by id;
select last_seq_num, is_called from s;
insert into t(v) values (100);
select id, v from t order by id;
select last_seq_num, is_called from s;
drop database stl_dst;

-- ROLLBACK arm of the boundary control: row, position, and generation
-- vanish together with the rejected merge.
use stl_base;
begin;
create database stl_dstr clone stl_base;
insert into stl_dstr.t(v) values (97);
-- @regex("cannot find database", true)
data branch merge stl_br.t into stl_dstr.t when conflict accept;
rollback;
show databases like 'stl_dstr';

-- Committed-route equivalent: an explicit-transaction merge into a
-- committed clone destination reconciles the destination sequence through
-- the autonomous unit within the same COMMIT; a fresh-session default
-- INSERT lands beyond the merged keys.
create database stl_dstc clone stl_base;
begin;
data branch merge stl_br.t into stl_dstc.t when conflict accept;
commit;
use stl_dstc;
select id, v from t order by id;
select last_seq_num, is_called from s;
insert into t(v) values (101);
select id, v from t order by id;
select last_seq_num, is_called from s;
drop database stl_dstc;
drop database stl_br;

-- Descending mirror on the committed route.
use stl_base;
create sequence sd as bigint increment by -1 start with 10 minvalue 1 maxvalue 10 no cycle;
create table td (id bigint primary key default nextval('sd'), v int);
insert into td(v) values (1);
data branch create database stl_brd from stl_base;
insert into stl_brd.td(id, v) values (9, 90), (8, 80);
create database stl_dstdc clone stl_base;
begin;
data branch merge stl_brd.td into stl_dstdc.td when conflict accept;
commit;
use stl_dstdc;
select id, v from td order by id;
select last_seq_num, is_called from sd;
insert into td(v) values (66);
select id, v from td order by id;
select last_seq_num, is_called from sd;
drop database stl_dstdc;
drop database stl_brd;

-- K1 repair on a GENUINE local generation (v12-review F10): the clone, the
-- emissions, the failing statement and the retry all share one explicit
-- transaction, so every allocation below is transaction-local. The failing
-- statement emits its next value before the NOT NULL boundary rejects the
-- row; statement-level rewind reclaims the row but never the issued value,
-- and the retry emits the value after it.
use stl_base;
begin;
create database stl_k1 clone stl_base;
use stl_k1;
create table kn (n int);
insert into kn values (NULL);
create table tk (id bigint primary key default nextval('s'), v int not null);
insert into tk(v) values (61);
select * from tk;
select last_seq_num, is_called from s;
-- runtime NULL source defeats constant folding: the allocation runs before
-- the write boundary rejects the statement:
-- @regex("null", true)
insert into tk(v) select n from kn;
insert into tk(v) values (62);
select id, v from tk order by id;
select last_seq_num, is_called from s;
commit;
-- a fresh session observes the committed local-generation position: no row
-- from the failed statement, no re-issue of its issued value.
-- @session:id=1
use stl_k1;
select id, v from tk order by id;
select last_seq_num, is_called from s;
insert into tk(v) values (63);
select id, v from tk order by id;
select last_seq_num, is_called from s;
-- @session
use stl_base;
drop database stl_k1;

-- J2 deliberate boundaries on a GENUINE local generation (v12-review F10):
-- the clone and the whole SETVAL arm share one explicit transaction. Each
-- successful SETVAL is a boundary whose staged obligation removal applies
-- at statement success; allocation follows the assignment, and the
-- committed position survives the fresh-session check.
use stl_base;
begin;
create database stl_j2 clone stl_base;
use stl_j2;
insert into t(v) values (70);
select setval('s', 40, true);
insert into t(v) values (71);
select setval('s', 50, false);
insert into t(v) values (72);
commit;
select id, v from t order by id;
select last_seq_num, is_called from s;
-- a fresh session observes the committed boundary: called at the uncalled
-- assignment's first emission; the next value follows it.
-- @session:id=1
use stl_j2;
select last_seq_num, is_called from s;
insert into t(v) values (80);
select id, v from t order by id;
select last_seq_num, is_called from s;
-- @session
use stl_base;
drop database stl_j2;

-- F5 CTAS on the local route (v12-review F5/F10): population runs in an
-- internal child process which must share the session obligation store, so
-- issued values of a transaction-local generation stay protected through a
-- failing CTAS, its retry and the commit.
use stl_base;
begin;
create database stl_ctas clone stl_base;
use stl_ctas;
create table copied as select nextval('s') as id, v from stl_base.t;
select id, v from copied order by id;
select last_seq_num, is_called from s;
-- failure arm: the second source row violates the primary key only after
-- both rows materialized (and allocated); statement rewind reclaims the
-- rows but never the issued values.
create table csrc (v int);
insert into csrc values (7), (7);
-- @regex("Duplicate", true)
create table cfail (primary key (v)) as select nextval('s') as id, v from csrc;
show tables like 'cfail';
-- the retry continues past the failed statement's issued values:
create table cfix (primary key (v)) as select nextval('s') as id, v from csrc where v = 7 limit 1;
select id, v from cfix order by id;
select last_seq_num, is_called from s;
commit;
-- a fresh session continues past the committed position of the generation:
-- @session:id=1
use stl_ctas;
select nextval('s');
select last_seq_num, is_called from s;
-- @session
use stl_base;
drop database stl_ctas;

-- F4 generation-identity routing (v12-review F4): the caller's allocation
-- routes by generation identity (database id + relation id), never by name
-- existence. Drop-and-recreate inside one transaction targets the NEW
-- transaction-local generation: no allocation into the old committed
-- generation and no self-wait on lifecycle locks.
create database stl_sh1;
use stl_sh1;
create sequence s as bigint increment by 1 start with 1 no cycle;
create table t (id bigint primary key default nextval('s'), v int);
insert into t(v) values (1);
create database stl_shd clone stl_sh1;
begin;
drop database stl_shd;
create database stl_shd clone stl_sh1;
use stl_shd;
insert into t(v) values (2);
insert into t(v) values (3);
commit;
select id, v from t order by id;
select last_seq_num, is_called from s;
-- a fresh session continues past the committed local-generation position;
-- the old committed generation was never advanced.
-- @session:id=1
use stl_shd;
insert into t(v) values (4);
select id, v from t order by id;
select last_seq_num, is_called from s;
-- @session
use stl_base;
drop database stl_shd;
drop database stl_sh1;

-- Supported-path controls: foreground sequence DDL is rejected inside an
-- explicit transaction (transaction-local generations come only from the
-- supported clone/restore routes).
use stl_base;
begin;
-- @regex("Only CREATE of DDL is supported in transactions", true)
create sequence rejected_seq;
rollback;
begin;
-- @regex("unclassified statement appears in uncommitted transaction", true)
alter sequence stl_base.s start with 5;
rollback;
select last_seq_num, is_called from s;

drop database stl_base;
