-- Smoke test for the new index-plugin dispatch path on the fulltext index.
-- Proves CREATE FULLTEXT INDEX dispatches to the plugin, registers the
-- algorithm + its hidden table in mo_catalog.mo_indexes, round-trips through
-- SHOW CREATE TABLE, and answers a MATCH query — the plugin framework's
-- registration/dispatch/catalog hooks, end to end.

drop database if exists fulltext_plugin_smoke;
create database fulltext_plugin_smoke;
use fulltext_plugin_smoke;

set experimental_fulltext_index = 1;
create table d (id int primary key, body text);
insert into d values (1, 'hello world'), (2, 'foo bar baz'), (3, 'hello again');
create fulltext index ftidx on d (body);

-- plugin registration: algo + hidden-table type written by the catalog hook
select algo, algo_table_type from mo_catalog.mo_indexes
  where table_id = (select rel_id from mo_catalog.mo_tables
                    where relname = 'd' and reldatabase = 'fulltext_plugin_smoke')
  order by algo_table_type;
show create table d;
-- @sortkey:0
select id from d where match(body) against('hello');
set experimental_fulltext_index = 0;

drop database fulltext_plugin_smoke;
