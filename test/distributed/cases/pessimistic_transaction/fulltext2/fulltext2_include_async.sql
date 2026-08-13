-- fulltext2 INCLUDE columns over CDC (always-async) — the actual include values
-- (status, prio) must travel through the ISCP CDC tail on INSERT/UPDATE/DELETE, so
-- the in-index prefilter + covering projection reflect the mutations. Modeled on
-- fulltext2_async: inline FULLTEXT2 is AlwaysAsync, sleep() waits for CDC to settle.
-- Cache-safe (per-CN index cache is not cross-invalidated): each table is searched
-- only AFTER its final mutation has settled, so its cache loads fresh.
set experimental_fulltext2_index = 1;

-- src_ins: insert-only — verifies INSERT carries include values through CDC.
create table src_ins (id bigint primary key, body varchar(200), status varchar(20), prio int,
  FULLTEXT2 ftidx (body) INCLUDE (status, prio));
insert into src_ins values
(1, 'quick brown fox',  'active',   10),
(2, 'lazy fox sleeps',  'archived', 20),
(3, 'red fox runs',     'active',   30),
(4, 'quiet sleepy cat', 'active',   40);

-- src_upd: mutated below (UPDATE include value + DELETE) before its first search.
create table src_upd (id bigint primary key, body varchar(200), status varchar(20), prio int,
  FULLTEXT2 ftidx (body) INCLUDE (status, prio));
insert into src_upd values
(10, 'silver fox hunts', 'active',   100),
(11, 'golden fox naps',  'active',   200),
(12, 'bronze fox digs',  'archived', 300);

-- wait for the initial CDC sync
select sleep(30);

-- src_ins is never re-mutated: safe to search now (first search loads fresh cache).
-- covering projection carries include values; cat(4) has no 'fox'.
select id, status, prio from src_ins where match(body) against('fox') order by id;
-- in-index prefilter on the CDC-carried include value.
select id from src_ins where match(body) against('fox') and status = 'active' order by id;
select id, prio from src_ins where match(body) against('fox') and prio > 15 order by id;

-- mutate src_upd: UPDATE an include value (LWW) + DELETE, BEFORE its first search.
update src_upd set status = 'archived', prio = 999 where id = 10;
delete from src_upd where id = 11;
select sleep(30);

-- src_upd's FIRST search (fresh cache post-mutation): id10 include values updated
-- (active/100 -> archived/999), id11 gone, id12 unchanged.
select id, status, prio from src_upd where match(body) against('fox') order by id;
-- prefilter reflects the NEW include values: no 'active' left (10 updated, 11 deleted).
select id from src_upd where match(body) against('fox') and status = 'active' order by id;
select id from src_upd where match(body) against('fox') and status = 'archived' order by id;
-- id10's updated prio wins.
select id, prio from src_upd where match(body) against('fox') and prio > 500 order by id;

-- src_phantom: the covered-path CDC last-writer-wins regression. An UPSERT (UPDATE) followed
-- by a DELETE of the SAME pk in ONE transaction (so both mutations flow through CDC together)
-- must resolve to the DELETE. The classic 2-JOIN path masked a resurrected row via the source
-- join; the covered 0-JOIN path has no such join, so a phantom would surface a deleted row with
-- stale INCLUDE values. Ordered by score (never ORDER BY id) so the covered fast path is used.
create table src_phantom (id bigint primary key, body varchar(200), status varchar(20), prio int,
  FULLTEXT2 ftidx (body) INCLUDE (status, prio));
insert into src_phantom values
(20, 'silver fox alpha', 'active', 500),
(21, 'golden fox beta',  'active', 600);
-- upsert then delete the SAME pk in one txn: the exact upsert->delete phantom case.
begin;
update src_phantom set body = 'silver fox gamma', status = 'updated', prio = 999 where id = 20;
delete from src_phantom where id = 20;
commit;
select sleep(30);

-- covered fast path (0-JOIN: Project -> Sort(score) -> Table Function, no base Table Scan/Join).
explain select id, status, prio from src_phantom where match(body) against('fox');
-- pk 20 must be GONE (no phantom, no stale 'updated'/999); only pk 21 remains.
select id, status, prio from src_phantom where match(body) against('fox') order by match(body) against('fox') desc, id;

drop table src_ins;
drop table src_upd;
drop table src_phantom;
