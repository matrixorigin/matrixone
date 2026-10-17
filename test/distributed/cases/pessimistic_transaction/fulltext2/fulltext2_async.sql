-- fulltext2 CDC (always-async) — ported from pessimistic_transaction/fulltext
-- (fulltext_async). fulltext2 is AlwaysAsync (no ASYNC keyword): the inline index
-- builds an empty tag=0 base at CREATE, then post-create INSERT/UPDATE/DELETE flow
-- into the tag=1 CdcTail via ISCP CDC. Poll that durable tail before MATCH so the
-- test waits only as long as needed and cannot silently search before CDC settles.

-- CREATE FULLTEXT2 INDEX is gated behind experimental_fulltext2_index (default off).
set experimental_fulltext2_index = 1;

-- create src async (multi-column body+title)
create table src (id bigint primary key, body varchar, title text, FULLTEXT2 ftidx (body, title));

insert into src values (0, 'color is red', 't1'), (1, 'car is yellow', 'crazy car'), (2, 'sky is blue', 'no limit'), (3, 'blue is not red', 'colorful'),
(4, '遠東兒童中文是針對6到9歲的小朋友精心設計的中文學習教材，共三冊，目前已出版一、二冊。', '遠東兒童中文'),
(5, '每冊均採用近百張全幅彩圖及照片，生動活潑、自然真實，加深兒童學習印象，洋溢學習樂趣。', '遠東兒童中文'),
(6, '各個單元主題內容涵蓋中華文化及生活應用的介紹。本套教材含課本、教學指引、生字卡、學生作業本與CD，中英對照，精美大字版。本系列有繁體字及簡體字兩種版本印行。', '中文短篇小說'),
(7, '59個簡單的英文和中文短篇小說', '適合初學者'),
(8, NULL, 'NOT INCLUDED'),
(9, 'NOT INCLUDED BODY', NULL),
(10, NULL, NULL);

-- composite primary key
create table src2 (id1 varchar, id2 bigint, body char(128), title text, primary key (id1, id2), FULLTEXT2 ftidx(body, title));

insert into src2 values ('id0', 0, 'red', 't1'), ('id1', 1, 'yellow', 't2'), ('id2', 2, 'blue', 't3'), ('id3', 3, 'blue red', 't4'), ('id4', 4, 'bright red null', NULL);

-- Wait for the initial CDC sync of src and src2. Do not poll MATCH: an early
-- MATCH can pin a stale per-CN cache. A cdc_tail chunk is the durable readiness
-- signal produced in the same transaction that advances the CDC watermark.
set @src_ft2_index = (
    select index_table_name from mo_catalog.mo_indexes
    where name = 'ftidx' and algo = 'fulltext2' and algo_table_type = 'ftv2_index'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'src')
    limit 1
);
set @src2_ft2_index = (
    select index_table_name from mo_catalog.mo_indexes
    where name = 'ftidx' and algo = 'fulltext2' and algo_table_type = 'ftv2_index'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'src2')
    limit 1
);
set @wait_initial_ft2_sql = concat(
    'select ',
    '(select coalesce(max(chunk_id), -1) >= 0 from `', database(), '`.`', @src_ft2_index,
    '` where index_id = ''cdc_tail'' and tag = 1) as src_ready, ',
    '(select coalesce(max(chunk_id), -1) >= 0 from `', database(), '`.`', @src2_ft2_index,
    '` where index_id = ''cdc_tail'' and tag = 1) as src2_ready'
);
prepare wait_initial_ft2 from @wait_initial_ft2_sql;
-- @wait_expect(2, 120)
execute wait_initial_ft2;
deallocate prepare wait_initial_ft2;

-- Independent CDC boundary: create an empty JSON_VALUE index first so this
-- lifecycle is not masked by an initial CREATE mismatch.
drop table if exists json_null_cdc;
create table json_null_cdc(id bigint primary key, left_doc json, right_doc json);
create fulltext2 index ftv on json_null_cdc(left_doc, right_doc) with parser json_value;
set @json_ft2_index = (
    select index_table_name from mo_catalog.mo_indexes
    where name = 'ftv' and algo = 'fulltext2' and algo_table_type = 'ftv2_index'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'json_null_cdc')
    limit 1
);
insert into json_null_cdc values
(1,'{"k":"leftboth"}','{"k":"rightboth"}'),
(2,NULL,'{"k":"onlyrighttoken"}'),
(3,'{"k":"onlylefttoken"}',NULL),
(4,NULL,NULL),
(5,'null','{"k":"literalcontrol"}'),
(6,'"null"',NULL);
set @wait_json_sql = concat(
    'select coalesce(max(chunk_id), -1) >= 0 as json_ready from `', database(), '`.`', @json_ft2_index,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_json from @wait_json_sql;
-- @wait_expect(2, 120)
execute wait_json;
deallocate prepare wait_json;

-- Initial CDC INSERT: partial NULL rows retain the non-NULL sibling; all-NULL
-- has no searchable terms and the JSON string "null" remains searchable.
select id from json_null_cdc where match(left_doc, right_doc) against('onlyrighttoken' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('onlylefttoken' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('+leftboth +rightboth' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('literalcontrol' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('null' in boolean mode) order by id;

-- The per-CN fulltext2 cache is not cross-invalidated by a CDC consumer on another CN.
-- Initial-insert checks own this first cold index. The independent mutation
-- scenarios share a second cold index below.
drop table json_null_cdc;

-- A non-empty generation must eventually refresh WITHOUT rebuilding the index
-- or invalidating it from CDC. Keep MATCH hot while waiting: sliding TTL alone
-- cannot satisfy this assertion; the ordinary pull-based stale sweep must work.
-- Exercise the existing periodic sweep at a shorter cadence, not its production
-- wall-clock interval. The control only rearms timers; it never evicts entries.
-- Restore the default immediately after this scenario, before the other cases.
create table warm_refresh (id bigint primary key, body text);
insert into warm_refresh values (1, 'refreshmarker baseline'), (2, 'unrelated control');
create fulltext2 index ft_refresh on warm_refresh(body);
select id from warm_refresh where match(body) against('refreshmarker' in boolean mode) order by id;
set @warm_storage = (select index_table_name from mo_catalog.mo_indexes where name = 'ft_refresh' and algo = 'fulltext2' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'warm_refresh') limit 1);
set @freshness_override = mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '1s');
-- A partial fan-out must fail this case rather than leaving a slow CN unnoticed.
select @freshness_override not like '%failed=[%' as all_cn_updated;
insert into warm_refresh values (101, 'refreshmarker committed');
set @warm_tail_sql = concat('select count(*) > 0 as tail_ready from `', database(), '`.`', @warm_storage, '` where index_id = ''cdc_tail'' and tag = 1');
prepare warm_tail from @warm_tail_sql;
-- @wait_expect(2, 120)
execute warm_tail;
deallocate prepare warm_tail;
-- @wait_expect(1, 60)
select id from warm_refresh where match(body) against('refreshmarker' in boolean mode) order by id;
set @freshness_restore = mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '0');
select @freshness_restore not like '%failed=[%' as all_cn_restored;
drop table warm_refresh;
-- Each independent mutation has its own PK and alphabetic token domain. Keep
-- this index cold until all final mutations are durable; the first MATCH then
-- loads every scenario's final state without relying on a warm-cache refresh.
create table json_null_cdc(id bigint primary key, left_doc json, right_doc json);
create fulltext2 index ftv on json_null_cdc(left_doc, right_doc) with parser json_value;
set @json_ft2_index = (
    select index_table_name from mo_catalog.mo_indexes
    where name = 'ftv' and algo = 'fulltext2' and algo_table_type = 'ftv2_index'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'json_null_cdc')
    limit 1
);
insert into json_null_cdc values
(21,'{"k":"alphaoldleft"}','{"k":"alpharight"}'),
(22,NULL,'{"k":"betaoldright"}'),
(23,'{"k":"gammaoldleft"}',NULL),
(24,NULL,NULL),
(25,NULL,'{"k":"epsilonoldright"}'),
(26,NULL,'{"k":"zetaoldright"}'),
(2,NULL,'{"k":"onlyrighttoken"}'),
(3,'{"k":"onlylefttoken"}',NULL),
(4,NULL,NULL),
(5,'null','{"k":"literalcontrol"}'),
(6,'"null"',NULL);
set @wait_json_sql = concat(
    'select coalesce(max(chunk_id), -1) >= 0 as json_ready from `', database(), '`.`', @json_ft2_index,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_json from @wait_json_sql;
-- @wait_expect(2, 120)
execute wait_json;
deallocate prepare wait_json;
set @capture_json_tail_sql = concat(
    'select coalesce(max(chunk_id), -1) into @json_tail_before from `', database(), '`.`', @json_ft2_index,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare capture_json_tail from @capture_json_tail_sql;
execute capture_json_tail;
deallocate prepare capture_json_tail;
-- One source commit gives these mutations one visibility boundary. Fulltext2
-- publishes a tail chunk only after consuming the complete range, in the same
-- transaction as its watermark; an earlier mutation cannot satisfy this wait.
begin;
-- Left value -> SQL NULL preserves its right sibling.
update json_null_cdc set left_doc = NULL where id = 21;
-- Right value -> a new left value replaces the old right term.
update json_null_cdc set right_doc = NULL, left_doc = '{"k":"betanewleft"}' where id = 22;
-- All-NULL UPSERT retains the PK with no searchable terms.
update json_null_cdc set left_doc = NULL where id = 23;
-- An all-NULL document can regain content.
update json_null_cdc set right_doc = '{"k":"deltarevived"}' where id = 24;
-- Both deletions must be durable before the second PK is reinserted.
delete from json_null_cdc where id in (25, 26);
commit;
set @wait_json_sql = concat(
    'select coalesce(max(chunk_id), -1) > ', @json_tail_before,
    ' as json_update_ready from `', database(), '`.`', @json_ft2_index,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_json from @wait_json_sql;
-- @wait_expect(2, 120)
execute wait_json;
deallocate prepare wait_json;
set @capture_json_tail_sql = concat(
    'select coalesce(max(chunk_id), -1) into @json_tail_before from `', database(), '`.`', @json_ft2_index,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare capture_json_tail from @capture_json_tail_sql;
execute capture_json_tail;
deallocate prepare capture_json_tail;
insert into json_null_cdc values (26,NULL,'{"k":"zetareinserted"}');
set @wait_json_sql = concat(
    'select coalesce(max(chunk_id), -1) > ', @json_tail_before,
    ' as json_reinsert_ready from `', database(), '`.`', @json_ft2_index,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_json from @wait_json_sql;
-- @wait_expect(2, 120)
execute wait_json;
deallocate prepare wait_json;
-- Left value -> NULL; the right sibling remains searchable.
select id from json_null_cdc where match(left_doc, right_doc) against('alphaoldleft' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('alpharight' in boolean mode) order by id;
-- Right value -> new left value; the old right term is gone.
select id from json_null_cdc where match(left_doc, right_doc) against('betaoldright' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('betanewleft' in boolean mode) order by id;
-- All-NULL UPSERT keeps the source PK and shadows its old posting.
select id from json_null_cdc where id = 23;
select id from json_null_cdc where match(left_doc, right_doc) against('gammaoldleft' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('onlyrighttoken' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('onlylefttoken' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('literalcontrol' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('null' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('+alphaoldleft +alpharight' in boolean mode) order by id;
-- All-NULL -> right value; controls still survive.
select id from json_null_cdc where match(left_doc, right_doc) against('deltarevived' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('onlyrighttoken' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('onlylefttoken' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('literalcontrol' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('null' in boolean mode) order by id;
-- DELETE removes the old word without removing control documents.
select id from json_null_cdc where match(left_doc, right_doc) against('epsilonoldright' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('onlyrighttoken' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('onlylefttoken' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('literalcontrol' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('null' in boolean mode) order by id;
-- DELETE/reinsert keeps the final version and removes the old word.
select id from json_null_cdc where match(left_doc, right_doc) against('zetareinserted' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('zetaoldright' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('onlyrighttoken' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('onlylefttoken' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('literalcontrol' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('null' in boolean mode) order by id;
-- MERGE/REBUILD must not resurrect zero-word or deleted postings.
alter table json_null_cdc alter reindex ftv fulltext2 merge force_sync;
select id from json_null_cdc where match(left_doc, right_doc) against('gammaoldleft' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('onlyrighttoken' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('onlylefttoken' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('literalcontrol' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('null' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('+alphaoldleft +alpharight' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('alphaoldleft' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('alpharight' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('betaoldright' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('betanewleft' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('deltarevived' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('epsilonoldright' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('zetaoldright' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('zetareinserted' in boolean mode) order by id;
-- MERGE/REBUILD must not resurrect zero-word or deleted postings.
alter table json_null_cdc alter reindex ftv fulltext2 force_sync;
select id from json_null_cdc where match(left_doc, right_doc) against('gammaoldleft' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('onlyrighttoken' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('onlylefttoken' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('literalcontrol' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('null' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('+alphaoldleft +alpharight' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('alphaoldleft' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('alpharight' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('betaoldright' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('betanewleft' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('deltarevived' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('epsilonoldright' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('zetaoldright' in boolean mode) order by id;
select id from json_null_cdc where match(left_doc, right_doc) against('zetareinserted' in boolean mode) order by id;

-- src's initial-sync state is verified via src2 below (a never-re-mutated table).
-- src itself is deliberately NOT searched here: fulltext2's per-CN index cache is
-- not cross-invalidated by the CDC consumer (which evicts only its own CN), so a
-- pre-update search on the mo-tester CN would pin the pre-update snapshot and the
-- post-update queries below would then read it stale on multi-CN. src is first
-- searched only after its final mutation has settled, so its cache loads fresh.
show create table src;

-- select src2 (composite pk): id0 (red), id3 (blue red), id4 (body with NULL title)
select id1, id2 from src2 where match(body, title) against('red') order by id1;
show create table src2;

-- post-create UPDATE + DELETE flow through CDC too
set @capture_src_tail_sql = concat(
    'select coalesce(max(chunk_id), -1) into @src_tail_before_mutation from `', database(), '`.`', @src_ft2_index,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare capture_src_tail from @capture_src_tail_sql;
execute capture_src_tail;
deallocate prepare capture_src_tail;
update src set body = 'color is green' where id = 0;
set @wait_src_mutation_sql = concat(
    'select coalesce(max(chunk_id), -1) > ', @src_tail_before_mutation,
    ' as src_update_ready from `', database(), '`.`', @src_ft2_index,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_src_mutation from @wait_src_mutation_sql;
-- @wait_expect(2, 120)
execute wait_src_mutation;
deallocate prepare wait_src_mutation;
set @capture_src_tail_sql = concat(
    'select coalesce(max(chunk_id), -1) into @src_tail_before_mutation from `', database(), '`.`', @src_ft2_index,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare capture_src_tail from @capture_src_tail_sql;
execute capture_src_tail;
deallocate prepare capture_src_tail;
delete from src where id = 3;
set @wait_src_mutation_sql = concat(
    'select coalesce(max(chunk_id), -1) > ', @src_tail_before_mutation,
    ' as src_delete_ready from `', database(), '`.`', @src_ft2_index,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_src_mutation from @wait_src_mutation_sql;
-- @wait_expect(2, 120)
execute wait_src_mutation;
deallocate prepare wait_src_mutation;

-- doc 0's old text (red) is superseded, doc 3 is deleted -> 'red' now empty.
-- This is src's FIRST MATCH, so its per-CN cache loads the fresh post-update index.
select id from src where match(body, title) against('red') order by id;
-- doc 0's new text
select id from src where match(body, title) against('green') order by id;

-- rename keeps the index + CDC
alter table src rename to src1;
select id from src1 where match(body, title) against('green') order by id;

drop table src1;
drop table src2;
drop table json_null_cdc;
