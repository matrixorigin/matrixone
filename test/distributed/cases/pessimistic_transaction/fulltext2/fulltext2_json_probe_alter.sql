-- #27926/#27941: a json_extract probe must return correct rows across an ALTER. The copy-table
-- rebuild creates a new base and index generation whose build_ts reflects the new schema. A later
-- insert may be read from the index, a table_changes tail, or a safe fallback depending on CDC
-- progress; every path must return the same rows without a schema-version error.
set experimental_fulltext2_index = 1;
drop database if exists ft2_json_alter;
create database ft2_json_alter;
use ft2_json_alter;

create table t (id bigint primary key, j json);
insert into t values
 (1, '{"foo":"needle"}'),
 (2, '{"foo":"hay"}'),
 (3, '{"foo":"needle"}');
-- The initial index builds synchronously from these rows. This test concerns
-- the rebuilt generation, not ALTER racing an unrelated initial CDC write.
create fulltext2 index ftj on t(j) with parser json;

-- Copy-table rebuild: new base table id and a new index table, rebuilt against the new schema.
alter table t add column extra int default 0;

-- Wait for the NEW index generation's build_ts. Re-resolve its metadata table
-- from the post-ALTER catalog rather than reading the dropped generation.
set @meta = (select index_table_name from mo_catalog.mo_indexes where name = 'ftj' and algo_table_type = 'ftv2_meta' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @wait_meta_sql = concat('select coalesce(max(build_ts), 0) > 0 as ready from `', database(), '`.`', @meta, '`');
prepare wait_meta from @wait_meta_sql;
-- @wait_expect(2, 120)
execute wait_meta;
deallocate prepare wait_meta;

-- Insert after the rebuild and query immediately. CDC may or may not have
-- consumed the new rows before the query; both cases must return row 4.
insert into t values (4, '{"foo":"needle"}', 9), (5, '{"foo":"hay"}', 9);
select id from t where json_extract_string(j,'$.foo') = 'needle' order by id;

drop database ft2_json_alter;
