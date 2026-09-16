-- fulltext2 NULL siblings through the empty-index CDC path.  Each indexed
-- column is filtered independently: a SQL NULL contributes no terms while a
-- non-NULL sibling is still parsed, positioned and carried with INCLUDE data.
-- The cdc_mut table is searched only after its final mutation so the first
-- MATCH loads a fresh per-CN cache; MERGE and REBUILD then prove the same
-- source-row oracle survives both migration paths.
set experimental_fulltext2_index = 1;
drop database if exists ft2_null_columns;
create database ft2_null_columns;
use ft2_null_columns;

-- Initial CDC oracle: the index is created before any source row exists.
create table cdc_seed (
  id bigint primary key,
  left_doc json,
  right_doc json,
  note varchar(32)
);
create fulltext2 index ftidx on cdc_seed(left_doc, right_doc) include(note) with parser json_value;
insert into cdc_seed values
 (1,'{"v":"seedleft"}','{"v":"seedright"}','both'),
 (2,NULL,'{"v":"seedrightonly"}','right-only'),
 (3,'{"v":"seedleftonly"}',NULL,'left-only'),
 (4,NULL,NULL,'all-null'),
 (5,'null','{"v":"seedliteral"}','literal-null'),
 (6,'"null"',NULL,'string-null'),
 (7,'{}','{"v":"seedemptyobject"}','empty-object'),
 (8,'[]','{"v":"seedemptyarray"}','empty-array');

-- Mutation oracle: all rows also enter through CDC, then exercise replacement,
-- NULL-to-value, delete/reinsert and repeated last-writer-wins updates.
create table cdc_mut (
  id bigint primary key,
  left_doc json,
  right_doc json,
  note varchar(32)
);
create fulltext2 index ftidx on cdc_mut(left_doc, right_doc) include(note) with parser json_value;
insert into cdc_mut values
 (10,'{"v":"oldleft"}',NULL,'old-left'),
 (11,NULL,'{"v":"oldright"}','old-right'),
 (12,'{"v":"deleteold"}','{"v":"keepold"}','delete-old'),
 (13,'{"v":"reinsertold"}',NULL,'reinsert-old'),
 (14,NULL,NULL,'zero-old');

set @seed_ft2 = (
    select index_table_name from mo_catalog.mo_indexes
    where name = 'ftidx' and algo_table_type = 'ftv2_index'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'cdc_seed')
    limit 1
);
set @mut_ft2 = (
    select index_table_name from mo_catalog.mo_indexes
    where name = 'ftidx' and algo_table_type = 'ftv2_index'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'cdc_mut')
    limit 1
);
set @wait_initial_sql = concat(
    'select ',
    '(select coalesce(max(chunk_id), -1) >= 0 from `', database(), '`.`', @seed_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1) as seed_ready, ',
    '(select coalesce(max(chunk_id), -1) >= 0 from `', database(), '`.`', @mut_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1) as mut_ready'
);
prepare wait_initial from @wait_initial_sql;
-- @wait_expect(2, 120)
execute wait_initial;
deallocate prepare wait_initial;

-- Every positive result below comes from a non-NULL sibling in the CDC row.
select id from cdc_seed where match(left_doc, right_doc) against('+seedleft +seedright' in boolean mode) order by id;
select id, note from cdc_seed where match(left_doc, right_doc) against('seedrightonly' in boolean mode) order by id;
select id, note from cdc_seed where match(left_doc, right_doc) against('seedleftonly' in boolean mode) order by id;
select id, note from cdc_seed where match(left_doc, right_doc) against('seedliteral' in boolean mode) order by id;
select id, note from cdc_seed where match(left_doc, right_doc) against('null' in boolean mode) order by id;
select id, note from cdc_seed where match(left_doc, right_doc) against('seedemptyobject' in boolean mode) order by id;
select id, note from cdc_seed where match(left_doc, right_doc) against('seedemptyarray' in boolean mode) order by id;

set @capture_mut_tail_sql = concat(
    'select coalesce(max(chunk_id), -1) into @mut_tail_before from `', database(), '`.`', @mut_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare capture_mut_tail from @capture_mut_tail_sql;
execute capture_mut_tail;
deallocate prepare capture_mut_tail;
update cdc_mut set left_doc = NULL, right_doc = NULL, note = 'zero-new' where id = 10;
set @wait_mut_sql = concat(
    'select coalesce(max(chunk_id), -1) > ', @mut_tail_before,
    ' as mut_all_null_ready from `', database(), '`.`', @mut_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_mut from @wait_mut_sql;
-- @wait_expect(2, 120)
execute wait_mut;
deallocate prepare wait_mut;

set @capture_mut_tail_sql = concat(
    'select coalesce(max(chunk_id), -1) into @mut_tail_before from `', database(), '`.`', @mut_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare capture_mut_tail from @capture_mut_tail_sql;
execute capture_mut_tail;
deallocate prepare capture_mut_tail;
update cdc_mut set right_doc = '{"v":"newright"}', note = 'right-new' where id = 11;
set @wait_mut_sql = concat(
    'select coalesce(max(chunk_id), -1) > ', @mut_tail_before,
    ' as mut_value_ready from `', database(), '`.`', @mut_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_mut from @wait_mut_sql;
-- @wait_expect(2, 120)
execute wait_mut;
deallocate prepare wait_mut;

set @capture_mut_tail_sql = concat(
    'select coalesce(max(chunk_id), -1) into @mut_tail_before from `', database(), '`.`', @mut_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare capture_mut_tail from @capture_mut_tail_sql;
execute capture_mut_tail;
deallocate prepare capture_mut_tail;
delete from cdc_mut where id = 12;
set @wait_mut_sql = concat(
    'select coalesce(max(chunk_id), -1) > ', @mut_tail_before,
    ' as mut_delete_ready from `', database(), '`.`', @mut_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_mut from @wait_mut_sql;
-- @wait_expect(2, 120)
execute wait_mut;
deallocate prepare wait_mut;

set @capture_mut_tail_sql = concat(
    'select coalesce(max(chunk_id), -1) into @mut_tail_before from `', database(), '`.`', @mut_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare capture_mut_tail from @capture_mut_tail_sql;
execute capture_mut_tail;
deallocate prepare capture_mut_tail;
insert into cdc_mut values (12,'{"v":"reinsert"}',NULL,'reinsert-new');
set @wait_mut_sql = concat(
    'select coalesce(max(chunk_id), -1) > ', @mut_tail_before,
    ' as mut_reinsert_ready from `', database(), '`.`', @mut_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_mut from @wait_mut_sql;
-- @wait_expect(2, 120)
execute wait_mut;
deallocate prepare wait_mut;

set @capture_mut_tail_sql = concat(
    'select coalesce(max(chunk_id), -1) into @mut_tail_before from `', database(), '`.`', @mut_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare capture_mut_tail from @capture_mut_tail_sql;
execute capture_mut_tail;
deallocate prepare capture_mut_tail;
update cdc_mut set left_doc = NULL, right_doc = NULL, note = 'lww-zero' where id = 13;
set @wait_mut_sql = concat(
    'select coalesce(max(chunk_id), -1) > ', @mut_tail_before,
    ' as mut_lww_zero_ready from `', database(), '`.`', @mut_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_mut from @wait_mut_sql;
-- @wait_expect(2, 120)
execute wait_mut;
deallocate prepare wait_mut;

set @capture_mut_tail_sql = concat(
    'select coalesce(max(chunk_id), -1) into @mut_tail_before from `', database(), '`.`', @mut_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare capture_mut_tail from @capture_mut_tail_sql;
execute capture_mut_tail;
deallocate prepare capture_mut_tail;
update cdc_mut set right_doc = '{"v":"lww-final"}', note = 'lww-final' where id = 13;
set @wait_mut_sql = concat(
    'select coalesce(max(chunk_id), -1) > ', @mut_tail_before,
    ' as mut_lww_final_ready from `', database(), '`.`', @mut_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_mut from @wait_mut_sql;
-- @wait_expect(2, 120)
execute wait_mut;
deallocate prepare wait_mut;

-- First search of cdc_mut is after all CDC generations settled.  Old terms
-- must stay absent; later NULL-to-value and delete/reinsert terms must win.
select id from cdc_mut where match(left_doc, right_doc) against('oldleft' in boolean mode) order by id;
select id from cdc_mut where match(left_doc, right_doc) against('oldright' in boolean mode) order by id;
select id, note from cdc_mut where match(left_doc, right_doc) against('newright' in boolean mode) order by id;
select id from cdc_mut where match(left_doc, right_doc) against('deleteold' in boolean mode) order by id;
select id from cdc_mut where match(left_doc, right_doc) against('keepold' in boolean mode) order by id;
select id, note from cdc_mut where match(left_doc, right_doc) against('reinsert' in boolean mode) order by id;
select id from cdc_mut where match(left_doc, right_doc) against('reinsertold' in boolean mode) order by id;
select id, note from cdc_mut where match(left_doc, right_doc) against('lww-final' in boolean mode) order by id;

-- MERGE folds the correct CDC tail into the base without re-tokenizing source.
alter table cdc_mut alter reindex ftidx fulltext2 merge force_sync;
select id, note from cdc_mut where match(left_doc, right_doc) against('newright' in boolean mode) order by id;
select id, note from cdc_mut where match(left_doc, right_doc) against('reinsert' in boolean mode) order by id;
select id, note from cdc_mut where match(left_doc, right_doc) against('lww-final' in boolean mode) order by id;
select id from cdc_mut where match(left_doc, right_doc) against('oldleft' in boolean mode) order by id;

-- REBUILD rereads the current source rows; its result must match the merged
-- index and must not resurrect any superseded or deleted term.
alter table cdc_mut alter reindex ftidx fulltext2 force_sync;
select id, note from cdc_mut where match(left_doc, right_doc) against('newright' in boolean mode) order by id;
select id, note from cdc_mut where match(left_doc, right_doc) against('reinsert' in boolean mode) order by id;
select id, note from cdc_mut where match(left_doc, right_doc) against('lww-final' in boolean mode) order by id;
select id from cdc_mut where match(left_doc, right_doc) against('oldleft' in boolean mode) order by id;

drop database ft2_null_columns;
