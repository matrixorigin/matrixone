-- #27926/#27941: a json_extract partial probe must stay correct across an ALTER. An ALTER does a
-- copy-table rebuild -- new base table AND a new index table whose build_ts reflects the rebuild
-- snapshot (>= the ALTER = the new schema version's start). So a table_changes tail over
-- (build_ts, snapshot] always sits inside the current schema version and never spans the ALTER
-- boundary: the query returns correct rows, it does not error. This forces the partial path to fire
-- on the rebuilt index (bake the rebuild, then a gap insert) so the invariant is actually exercised.
set experimental_fulltext2_index = 1;
drop database if exists ft2_json_alter;
create database ft2_json_alter;
use ft2_json_alter;

create table t (id bigint primary key, j json);
create fulltext2 index ftj on t(j) with parser json;
insert into t values
 (1, '{"foo":"needle"}'),
 (2, '{"foo":"hay"}'),
 (3, '{"foo":"needle"}');

-- Copy-table rebuild: new base table id and a new index table, rebuilt against the new schema.
alter table t add column extra int default 0;

-- Wait for the NEW index generation to build (its metadata build_ts becomes non-zero), so the query
-- below hits the partial path (bulk over the rebuilt base + a table_changes tail) rather than a
-- full scan against an empty new index. The metadata table is re-resolved from the post-ALTER
-- catalog, so this reads the new generation's build_ts, not the dropped one.
set @meta = (select index_table_name from mo_catalog.mo_indexes where name = 'ftj' and algo_table_type = 'ftv2_meta' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @wait_meta_sql = concat('select coalesce(max(build_ts), 0) > 0 as ready from `', database(), '`.`', @meta, '`');
prepare wait_meta from @wait_meta_sql;
-- @wait_expect(2, 120)
execute wait_meta;
deallocate prepare wait_meta;

-- Gap insert on the rebuilt index, then query immediately: the tail spans (build_ts, snapshot] which
-- is entirely within the post-ALTER schema version, so row 4 is returned via the tail with no
-- schema-window error.
insert into t values (4, '{"foo":"needle"}', 9), (5, '{"foo":"hay"}', 9);
select id from t where json_extract_string(j,'$.foo') = 'needle' order by id;

drop database ft2_json_alter;
