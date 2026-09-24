-- #27926/#27941: a json_extract probe combined with a filter on a NON-indexed column must keep that
-- second filter on the base scan -- the probe (and, when behind, the table_changes tail) is only a
-- candidate-pk source, so the non-indexed conjunct is the backstop that makes multi-column WHERE
-- correct. This asserts the PLAN is valid once coverage holds (the probe fires AND the base Table
-- Scan retains the status predicate), then asserts RESULTS for a covered read and a behind (partial)
-- read -- the latter proving the multi-column plan executes correctly whatever timing selects.
set experimental_fulltext2_index = 1;
drop database if exists ft2_json_multicol;
create database ft2_json_multicol;
use ft2_json_multicol;

create table t (id bigint primary key, j json, status varchar(16));
create fulltext2 index ftj on t(j) with parser json;
insert into t values
 (1, '{"foo":"needle"}', 'active'),
 (2, '{"foo":"needle"}', 'archived'),
 (3, '{"foo":"hay"}', 'active');

-- Wait for coverage so the EXPLAIN below is deterministic (a caught-up index yields the covered
-- probe). Readiness is gated on the cdc_tail chunk, so no wall-clock arithmetic and no timezone use.
set @ftj = (select index_table_name from mo_catalog.mo_indexes where name = 'ftj' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @wait_ftj_sql = concat('select coalesce(max(chunk_id), -1) >= 0 as ready from `', database(), '`.`', @ftj, '` where index_id = ''cdc_tail'' and tag = 1');
prepare wait_ftj from @wait_ftj_sql;
-- @wait_expect(2, 120)
execute wait_ftj;
deallocate prepare wait_ftj;

-- Valid plan: the json probe fires (fulltext2_search), AND the base Table Scan retains the status
-- predicate -- the non-indexed conjunct is enforced on the base rows, not delegated to the index.
-- @separator:table
-- @wait_expect(2, 120)
-- @regex("Table Function on fulltext2_search", true)
explain select id from t where json_extract_string(j,'$.foo') = 'needle' and status = 'active';
-- @separator:table
-- @regex("status = 'active'", true)
explain select id from t where json_extract_string(j,'$.foo') = 'needle' and status = 'active';

-- Covered result: only row 1 (needle AND active). Row 2 is a needle but archived (dropped by the
-- base status filter); row 3 is active but hay.
select id from t where json_extract_string(j,'$.foo') = 'needle' and status = 'active' order by id;

-- Behind (partial) read: a gap needle that also matches the status filter (4) and one that does not
-- (5). The result must include 4 and exclude 5 -- the tail supplies the candidate pk and the base
-- status filter still decides, whatever plan timing selects.
insert into t values (4, '{"foo":"needle"}', 'active'), (5, '{"foo":"needle"}', 'archived');
select id from t where json_extract_string(j,'$.foo') = 'needle' and status = 'active' order by id;

drop database ft2_json_multicol;
