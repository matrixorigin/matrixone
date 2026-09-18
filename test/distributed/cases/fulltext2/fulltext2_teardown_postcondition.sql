-- #29062 P2: prove the fulltext2 CHAR-INCLUDE lifecycle tears itself down cleanly. A case that
-- opens with `drop database if exists` can have that entry-guard silently mask residue left by a
-- failed teardown, so a second passing run does NOT prove cleanup. This case instead asserts
-- catalog state AFTER its own explicit `drop database`: the drop -- not a later entry-guard --
-- must leave zero residue for the schema, the base + fulltext2 hidden index tables, their
-- columns, and the fulltext2 rows in mo_indexes. The case is self-cleaning and idempotent, so
-- rerunning it on the same instance is deterministic.
set experimental_fulltext2_index = 1;
drop database if exists ft2_teardown;
create database ft2_teardown;
use ft2_teardown;

create table t(id bigint primary key, body text, ch char(4));
insert into t values (1,'alpha document','a'),(2,'alpha document','a '),(3,'beta text','b');
create fulltext2 index ft on t(body) include(ch);
alter table t alter reindex ft fulltext2 force_sync;

-- the fulltext2 CHAR INCLUDE predicate works (#29062): ch = 'a' matches 'a' and 'a ' -> 1,2
select id from t where match(body) against('+alpha' in boolean mode) and ch = 'a' order by id;

-- capture the database id so the post-drop index-residue check stays scoped to THIS database
set @dbid = (select dat_id from mo_catalog.mo_database where datname='ft2_teardown');

-- BEFORE teardown: the fulltext2 catalog footprint exists, so the drop has real resources to remove
select count(*) as ft2_index_rows_before from mo_catalog.mo_indexes where database_id=@dbid and algo='fulltext2';
select count(*) as ft2_hidden_tables_before from mo_catalog.mo_tables where reldatabase='ft2_teardown' and relname like '__mo_index_secondary_%';

-- explicit teardown, then assert ZERO residue -- this postcondition is what an entry-guard cannot mask
use mo_catalog;
drop database ft2_teardown;

select count(*) as schemata_residue from information_schema.schemata where schema_name='ft2_teardown';
select count(*) as mo_tables_residue from mo_catalog.mo_tables where reldatabase='ft2_teardown';
select count(*) as mo_columns_residue from mo_catalog.mo_columns where att_database='ft2_teardown';
select count(*) as mo_indexes_residue from mo_catalog.mo_indexes where database_id=@dbid;
