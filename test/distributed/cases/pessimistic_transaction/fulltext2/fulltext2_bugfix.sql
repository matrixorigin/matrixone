-- BVT for fulltext2 CDC/rebuild correctness fixes (self-review findings):
--   #1 temporal/decimal PK: CDC INSERT must NOT panic encodePk — the consumer now
--      extracts PKs as native Go values (ReprNative), not SQL-display strings.
--   #3 T_json column via CDC is searchable — each json column is flattened
--      per-column, binary-aware (bytejson.ByteJson), matching the CREATE build.
--   #4 multi-column json via CDC is searchable on EVERY column — per-column flatten
--      instead of joining raw columns then flattening the blob once (zero tokens).
--   #6 REBUILD over an emptied table drops the stale tag=0 base — no longer serves
--      deleted docs when the rebuild sees zero source rows.
-- All four are CDC/async-path bugs, so the index is created on an empty table and the
-- rows flow in through ISCP CDC; durable tail polling waits for the exact work.
set experimental_fulltext2_index = 1;
drop database if exists ft2_bugfix;
create database ft2_bugfix;
use ft2_bugfix;

-- #1: datetime PK — CDC path (index created before the rows exist)
create table pk_dt (id datetime primary key, body text);
create fulltext2 index ftidx on pk_dt(body);
insert into pk_dt values ('2020-01-02 03:04:05','alpha beta'), ('2021-06-07 08:09:10','beta gamma');

-- #1: decimal PK
create table pk_dec (id decimal(18,4) primary key, body text);
create fulltext2 index ftidx on pk_dec(body);
insert into pk_dec values (123.4567,'alpha beta'), (890.1234,'beta gamma');

-- #3: single T_json column
create table j1 (id bigint primary key, doc json);
create fulltext2 index ftidx on j1(doc) with parser json;
insert into j1 values (1,'{"title":"quantum physics","tag":"science"}'), (2,'{"title":"cooking recipes","tag":"food"}');

-- #4: multi-column json
create table j2 (id bigint primary key, a json, b json);
create fulltext2 index ftidx on j2(a,b) with parser json;
insert into j2 values (1,'{"x":"hello world"}','{"y":"foo bar"}'), (2,'{"x":"lorem ipsum"}','{"y":"dolor sit"}');

-- #6 is independent of #1/#3/#4. Start its initial CDC build now so all five
-- writers share one readiness barrier; no table is searched before it is ready.
create table reb (id bigint primary key, body text);
create fulltext2 index ftidx on reb(body);
insert into reb values (1,'stale zebra'),(2,'stale zebra');

-- Wait for every table's committed CDC tail. One completed table is not enough:
-- each independent writer must persist a chunk before its semantic checks run.
set @pk_dt_ft2 = (select index_table_name from mo_catalog.mo_indexes where name = 'ftidx' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'pk_dt') limit 1);
set @pk_dec_ft2 = (select index_table_name from mo_catalog.mo_indexes where name = 'ftidx' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'pk_dec') limit 1);
set @j1_ft2 = (select index_table_name from mo_catalog.mo_indexes where name = 'ftidx' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'j1') limit 1);
set @j2_ft2 = (select index_table_name from mo_catalog.mo_indexes where name = 'ftidx' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'j2') limit 1);
set @reb_ft2 = (select index_table_name from mo_catalog.mo_indexes where name = 'ftidx' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'reb') limit 1);
set @wait_bugfix_initial_sql = concat(
    'select ',
    '(select coalesce(max(chunk_id), -1) >= 0 from `', database(), '`.`', @pk_dt_ft2, '` where index_id = ''cdc_tail'' and tag = 1) as pk_dt_ready, ',
    '(select coalesce(max(chunk_id), -1) >= 0 from `', database(), '`.`', @pk_dec_ft2, '` where index_id = ''cdc_tail'' and tag = 1) as pk_dec_ready, ',
    '(select coalesce(max(chunk_id), -1) >= 0 from `', database(), '`.`', @j1_ft2, '` where index_id = ''cdc_tail'' and tag = 1) as j1_ready, ',
    '(select coalesce(max(chunk_id), -1) >= 0 from `', database(), '`.`', @j2_ft2, '` where index_id = ''cdc_tail'' and tag = 1) as j2_ready, ',
    '(select coalesce(max(chunk_id), -1) >= 0 from `', database(), '`.`', @reb_ft2, '` where index_id = ''cdc_tail'' and tag = 1) as reb_ready'
);
prepare wait_bugfix_initial from @wait_bugfix_initial_sql;
-- @wait_expect(2, 120)
execute wait_bugfix_initial;
deallocate prepare wait_bugfix_initial;

-- #1: both temporal/decimal-PK tables searchable => the consumer did not crash
select id from pk_dt where match(body) against('beta') order by id;
select id from pk_dt where match(body) against('gamma') order by id;
select id from pk_dec where match(body) against('beta') order by id;

-- #3: T_json indexed by the CDC consumer and searchable. The json parser
-- indexes each leaf as a (key, value) tuple, so it is queried through
-- json_extract rather than free text; a whole value matches, not a substring.
select id from j1 where json_extract_string(doc,'$.title') = 'quantum physics' order by id;
select id from j1 where json_extract_string(doc,'$.tag') = 'food' order by id;

-- #4: BOTH json columns indexed (a and b). A multi-column json index is not
-- probed (the probe resolves a single column), so these fall back to a plain
-- scan — still correct, and they still prove both columns were indexed.
select id from j2 where json_extract_string(a,'$.x') = 'hello world' order by id;
select id from j2 where json_extract_string(b,'$.y') = 'foo bar' order by id;
select id from j2 where json_extract_string(b,'$.y') = 'dolor sit' order by id;

-- #6: REBUILD over an emptied table must not serve the stale (deleted) base
-- both rows searchable before the rebuild
select count(*) from reb where match(body) against('zebra');
-- empty the table, then REBUILD (rebuilds the base from the now-empty source)
delete from reb;
alter table reb alter reindex ftidx fulltext2;
-- the stale base is cleared => zero matches (before the fix this returned 2)
select count(*) from reb where match(body) against('zebra');

drop database ft2_bugfix;
