-- A DATA BRANCH copies an already-built fulltext2 index and then maintains the
-- copy from a CDC range that starts BEFORE the clone's own row appends, so those
-- appends share one range with the branch's first DML. A branch DELETE of a row
-- inherited from the copied base must therefore still shadow the copied posting:
-- the COVERED match path projects straight out of the index, so there is no
-- base-table join to mask a stale entry (#28847).
set experimental_fulltext2_index = 1;

drop database if exists issue28847_branch_ft2_delete;
create database issue28847_branch_ft2_delete;
use issue28847_branch_ft2_delete;

create table src(
  id bigint primary key,
  body text not null,
  tag int not null
);
insert into src values
  (1, 'alpha inherited row', 10),
  (2, 'beta retained row', 20);
create fulltext2 index ft on src(body) include(tag);
alter table src alter reindex ft fulltext2 force_sync;

-- Control: the source index is BUILT rather than copied and has always been
-- correct. It must stay correct after the branch deletes its own copy.
-- @separator:table
-- @regex("Fulltext Index Scan on ft", true)
-- @regex("Join", false)
explain select id, tag from src
where match(body) against('alpha' in boolean mode) order by id;
select id, tag from src
where match(body) against('alpha' in boolean mode) order by id;

data branch create table leaf from src;

-- The inherited row is deleted and a new branch row inserted. A third row is
-- created and destroyed inside the same maintenance window: a tombstone for a
-- key the copied index never held must be inert, because keeping the trailing
-- delete of an insert-then-delete key is what makes the inherited delete win.
delete from leaf where id = 1;
insert into leaf values (3, 'alpha branch new', 30);
insert into leaf values (9, 'alpha phantom', 90);
delete from leaf where id = 9;

-- Readiness: wait on the ISCP watermark, never on MATCH. fulltext2's per-CN
-- search cache is not cross-invalidated by the CDC consumer, so an early MATCH
-- would pin a stale snapshot and could hide the very ghost this case exists to
-- catch. The watermark bound is captured AFTER the DML committed, so once every
-- live job on the branch has passed it the delete frame is durable. REINDEX is
-- deliberately not used: a rebuild would give the right answer without proving
-- the copied index was maintained correctly.
set @leaf_id = (
  select rel_id from mo_catalog.mo_tables
  where reldatabase = database() and relname = 'leaf'
);
set @leaf_dml_done_ns = cast(unix_timestamp(now(6)) * 1000000000 as unsigned);
set @leaf_ft2_ready_sql = concat(
  'select coalesce(min(cast(substring_index(watermark, ''-'', 1) as unsigned)), 0) >= ',
  @leaf_dml_done_ns, ' as ready from mo_catalog.mo_iscp_log',
  ' where table_id = ', @leaf_id, ' and drop_at is null'
);
prepare wait_leaf_ft2 from @leaf_ft2_ready_sql;
-- @wait_expect(2, 90)
execute wait_leaf_ft2;
deallocate prepare wait_leaf_ft2;

-- Base truth: only the retained inherited row and the new branch row survive.
-- @separator:table
select id, body, tag from leaf order by id;

-- COVERED path (id and tag both come from the index, so the plan has no join).
-- Before the fix this returned the deleted inherited row 1 alongside 3.
-- @separator:table
-- @regex("Fulltext Index Scan on ft", true)
-- @regex("Join", false)
explain select id, tag from leaf
where match(body) against('alpha' in boolean mode) order by id;
select id, tag from leaf
where match(body) against('alpha' in boolean mode) order by id;
select id from leaf
where match(body) against('alpha' in boolean mode) order by id;

-- JOINED path: projecting a base-table column always filtered the ghost, which
-- is why the two equivalent predicates disagreed. They must agree now.
-- @separator:table
-- @regex("Join Type: INNER", true)
explain select id, body from leaf
where match(body) against('alpha' in boolean mode) order by id;
select id, body from leaf
where match(body) against('alpha' in boolean mode) order by id;

-- The retained inherited row keeps its copied INCLUDE value: the tombstone of a
-- sibling must not disturb it.
-- @separator:table
select id, tag from leaf
where match(body) against('beta' in boolean mode) order by id;

-- The row created and destroyed inside the branch window is gone from both the
-- base table and the index, and it did not take a real row with it.
-- @separator:table
select id, tag from leaf
where match(body) against('phantom' in boolean mode) order by id;

-- Branch DML never reaches back into the source index.
-- @separator:table
select id, tag from src
where match(body) against('alpha' in boolean mode) order by id;

drop database issue28847_branch_ft2_delete;
set experimental_fulltext2_index = 0;
