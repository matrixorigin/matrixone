-- #27926: with fulltext_index_scan_watermark_delay = 0 the coverage gate is
-- strict, so a row written before CDC indexes it (watermark < its commit ts)
-- makes the probe decline and the query falls back to Table Scan, still returning
-- the row. Asserts the plan has no fulltext2_search probe and the indexed result
-- equals an unindexed twin.
set experimental_fulltext2_index = 1;
set fulltext_index_scan_watermark_delay = 0;
drop database if exists ft2_json_probe_fallback;
create database ft2_json_probe_fallback;
use ft2_json_probe_fallback;

create table t (id bigint primary key, j json);
create table t_plain (id bigint primary key, j json);
create fulltext2 index ftj on t(j) with parser json;
insert into t values (1,'{"foo":"needle"}'),(2,'{"foo":"hay"}');
insert into t_plain select * from t;

-- wait for the initial CDC tail so a live job with a watermark exists (this is not
-- the no-job fail-closed case; the decline below is the strict delay=0 gate).
set @ftj = (select index_table_name from mo_catalog.mo_indexes where name = 'ftj' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @wait_ftj_sql = concat('select coalesce(max(chunk_id), -1) >= 0 as ready from `', database(), '`.`', @ftj, '` where index_id = ''cdc_tail'' and tag = 1');
prepare wait_ftj from @wait_ftj_sql;
-- @wait_expect(2, 120)
execute wait_ftj;
deallocate prepare wait_ftj;

-- write a matching row after the watermark; query it before CDC indexes it.
insert into t values (3,'{"foo":"needle"}');
insert into t_plain values (3,'{"foo":"needle"}');

-- delay=0: watermark < current snapshot, so the probe declines; no fulltext2_search.
-- @separator:table
-- @regex("Table Function on fulltext2_search", false)
explain select id from t where json_extract_string(j,'$.foo') = 'needle';

-- the fresh row (3) is returned and matches the unindexed twin.
select id from t where json_extract_string(j,'$.foo') = 'needle' order by id;
select id from t_plain where json_extract_string(j,'$.foo') = 'needle' order by id;

drop database ft2_json_probe_fallback;
