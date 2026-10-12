-- #27926/#27941: a json_extract partial probe whose (searched generation, snapshot] gap SPANS a
-- source schema-version change must still return correct rows. table_changes refuses to span a
-- version change, so the fulltext2_search operator falls back to a selective base-table scan
-- (SELECT pk FROM src WHERE json_extract_*_internal(...)) instead of the tail. A secondary CREATE
-- INDEX bumps the source rel_version WITHOUT rebuilding the fulltext2 index, so a gap that predates
-- it deterministically spans the bump. Before this fallback, the same shape errored with
-- "table_changes requires a single source schema version"; now it returns the correct rows.
set experimental_fulltext2_index = 1;
drop database if exists ft2_json_fb_ddl;
create database ft2_json_fb_ddl;
use ft2_json_fb_ddl;

create table t (id bigint primary key, j json, content varchar(64));
create fulltext2 index ftj on t(j) with parser json;
insert into t values
 (1, '{"foo":"needle"}', 'c1'),
 (2, '{"foo":"hay"}', 'c2'),
 (3, '{"foo":"needle"}', 'c3');

-- Bake the initial rows into the index (cdc_tail chunk written) so the later insert forms a genuine
-- gap. No wall-clock arithmetic: readiness is gated on the chunk row, same as the partial case.
set @ftj = (select index_table_name from mo_catalog.mo_indexes where name = 'ftj' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @wait_ftj_sql = concat('select coalesce(max(chunk_id), -1) >= 0 as ready from `', database(), '`.`', @ftj, '` where index_id = ''cdc_tail'' and tag = 1');
prepare wait_ftj from @wait_ftj_sql;
-- @wait_expect(2, 120)
execute wait_ftj;
deallocate prepare wait_ftj;

-- Gap insert (row 4 needle), then bump the SOURCE schema version with a secondary index. CREATE INDEX
-- on a non-json column changes rel_version but does NOT rebuild ftj, so the gap (searched, now] now
-- crosses the version boundary -- the schema-span fallback path.
insert into t values (4, '{"foo":"needle"}', 'c4'), (5, '{"foo":"hay"}', 'c5');
create index idx2 on t(content);

-- Correct rows (1,3,4) returned via the fallback -- NOT a "single source schema version" error, and
-- NOT a dropped gap row. The base scan re-checks the predicate, so the result is exact.
select id from t where json_extract_string(j,'$.foo') = 'needle' order by id;

-- Second WHERE conjunct on a non-indexed column stays on the base scan: row 4 (c4) matches, a
-- would-be gap needle with other content does not.
select id from t where json_extract_string(j,'$.foo') = 'needle' and content = 'c4' order by id;

drop database ft2_json_fb_ddl;
