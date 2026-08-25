-- fulltext2 INCLUDE columns over CDC (always-async) — the actual include values
-- (status, prio) must travel through the ISCP CDC tail on INSERT/UPDATE/DELETE, so
-- the in-index prefilter + covering projection reflect the mutations. Modeled on
-- fulltext2_async: inline FULLTEXT2 is AlwaysAsync, so durable tail state is polled.
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

-- Wait for both initial CDC tails; the semantic MATCH checks stay single-shot so
-- an early probe cannot pin a stale per-CN search cache.
set @src_ins_ft2 = (select index_table_name from mo_catalog.mo_indexes where name = 'ftidx' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'src_ins') limit 1);
set @src_upd_ft2 = (select index_table_name from mo_catalog.mo_indexes where name = 'ftidx' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'src_upd') limit 1);
set @wait_include_initial_sql = concat(
    'select ',
    '(select coalesce(max(chunk_id), -1) >= 0 from `', database(), '`.`', @src_ins_ft2, '` where index_id = ''cdc_tail'' and tag = 1) as src_ins_ready, ',
    '(select coalesce(max(chunk_id), -1) >= 0 from `', database(), '`.`', @src_upd_ft2, '` where index_id = ''cdc_tail'' and tag = 1) as src_upd_ready'
);
prepare wait_include_initial from @wait_include_initial_sql;
-- @wait_expect(2, 120)
execute wait_include_initial;
deallocate prepare wait_include_initial;

-- src_ins is never re-mutated: safe to search now (first search loads fresh cache).
-- covering projection carries include values; cat(4) has no 'fox'.
select id, status, prio from src_ins where match(body) against('fox') order by id;
-- in-index prefilter on the CDC-carried include value.
select id from src_ins where match(body) against('fox') and status = 'active' order by id;
select id, prio from src_ins where match(body) against('fox') and prio > 15 order by id;

-- mutate src_upd: UPDATE an include value (LWW) + DELETE, BEFORE its first search.
set @capture_src_upd_tail_sql = concat(
    'select coalesce(max(chunk_id), -1) into @src_upd_tail_before_mutation from `', database(), '`.`', @src_upd_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare capture_src_upd_tail from @capture_src_upd_tail_sql;
execute capture_src_upd_tail;
deallocate prepare capture_src_upd_tail;
update src_upd set status = 'archived', prio = 999 where id = 10;
set @wait_src_upd_mutation_sql = concat(
    'select coalesce(max(chunk_id), -1) > ', @src_upd_tail_before_mutation,
    ' as src_upd_update_ready from `', database(), '`.`', @src_upd_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_src_upd_mutation from @wait_src_upd_mutation_sql;
-- @wait_expect(2, 120)
execute wait_src_upd_mutation;
deallocate prepare wait_src_upd_mutation;
set @capture_src_upd_tail_sql = concat(
    'select coalesce(max(chunk_id), -1) into @src_upd_tail_before_mutation from `', database(), '`.`', @src_upd_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare capture_src_upd_tail from @capture_src_upd_tail_sql;
execute capture_src_upd_tail;
deallocate prepare capture_src_upd_tail;
delete from src_upd where id = 11;
set @wait_src_upd_mutation_sql = concat(
    'select coalesce(max(chunk_id), -1) > ', @src_upd_tail_before_mutation,
    ' as src_upd_delete_ready from `', database(), '`.`', @src_upd_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_src_upd_mutation from @wait_src_upd_mutation_sql;
-- @wait_expect(2, 120)
execute wait_src_upd_mutation;
deallocate prepare wait_src_upd_mutation;

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
set @src_phantom_ft2 = (select index_table_name from mo_catalog.mo_indexes where name = 'ftidx' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'src_phantom') limit 1);
set @wait_phantom_initial_sql = concat(
    'select coalesce(max(chunk_id), -1) >= 0 as src_phantom_ready from `', database(), '`.`', @src_phantom_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_phantom_initial from @wait_phantom_initial_sql;
-- @wait_expect(2, 120)
execute wait_phantom_initial;
deallocate prepare wait_phantom_initial;
set @capture_phantom_tail_sql = concat(
    'select coalesce(max(chunk_id), -1) into @phantom_tail_before_mutation from `', database(), '`.`', @src_phantom_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare capture_phantom_tail from @capture_phantom_tail_sql;
execute capture_phantom_tail;
deallocate prepare capture_phantom_tail;
-- upsert then delete the SAME pk in one txn: the exact upsert->delete phantom case.
begin;
update src_phantom set body = 'silver fox gamma', status = 'updated', prio = 999 where id = 20;
delete from src_phantom where id = 20;
commit;
set @wait_phantom_mutation_sql = concat(
    'select coalesce(max(chunk_id), -1) > ', @phantom_tail_before_mutation,
    ' as src_phantom_mutation_ready from `', database(), '`.`', @src_phantom_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_phantom_mutation from @wait_phantom_mutation_sql;
-- @wait_expect(2, 120)
execute wait_phantom_mutation;
deallocate prepare wait_phantom_mutation;

-- covered fast path (0-JOIN: Project -> Sort(score) -> Table Function, no base Table Scan/Join).
-- Compare every plan row exactly while ignoring only JDBC metadata: the plan
-- column's reported precision can be 0 or -1 without changing the physical path.
-- @metacmp(false)
explain select id, status, prio from src_phantom where match(body) against('fox');
-- pk 20 must be GONE (no phantom, no stale 'updated'/999); only pk 21 remains.
select id, status, prio from src_phantom where match(body) against('fox') order by match(body) against('fox') desc, id;

drop table src_ins;
drop table src_upd;
drop table src_phantom;
