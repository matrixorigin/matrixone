drop publication if exists prepared_management_pub;
drop database if exists prepared_management_db;
drop account if exists prepared_management_reader;
create account prepared_management_reader admin_name = 'admin' identified by '111';
create database prepared_management_db;
create table prepared_management_db.t1 (a int);
create table prepared_management_db.t2 (a int);

-- PREPARE must not create the publication.
prepare pub_create from 'create publication prepared_management_pub database prepared_management_db table t1 account all comment ''initial comment''';
show publications like 'prepared_management_pub';
execute pub_create;
execute pub_create;
prepare pub_create_if from 'create publication if not exists prepared_management_pub database prepared_management_db table t2 account all';

-- SHOW statements must return real rows, including after repeated execution.
prepare pub_pattern from 'show publications like ?';
set @pub_filter = 'prepared_management%';
-- @ignore:5,6
execute pub_pattern using @pub_filter;
-- A bare marker must not reuse bindings from the previous EXECUTE.
show publications like ?;
set @pub_filter = 'no_matching_publication%';
execute pub_pattern using @pub_filter;
-- Invalid bindings must leave the handle usable.
execute pub_pattern;
set @pub_filter = 42;
execute pub_pattern using @pub_filter;
set @pub_filter = null;
execute pub_pattern using @pub_filter;
set @pub_filter = 'x'' OR 1=1 -- ';
execute pub_pattern using @pub_filter;
set @pub_filter = 'prepared_management_pub';
-- @ignore:5,6
execute pub_pattern using @pub_filter;
prepare pub_show from 'show publications like ''prepared_management_pub''';
prepare pub_ddl from 'show create publication prepared_management_pub';
prepare pub_coverage from 'show publication coverage prepared_management_pub';
-- @ignore:5,6
execute pub_show;
execute pub_ddl;
execute pub_coverage;

-- PREPARE must not alter the publication.
prepare pub_alter from 'alter publication prepared_management_pub database prepared_management_db table t2 comment ''prepared update''';
execute pub_coverage;
execute pub_alter;
execute pub_alter;
-- Wire TEXT metadata must remain usable by saved-result materialization.
set @publication_saved_result = @@save_query_result;
set save_query_result = on;
-- @ignore:5,6
execute pub_show;
-- @ignore:5,6
select * from result_scan(last_query_id()) as saved;
set save_query_result = @publication_saved_result;
execute pub_ddl;
execute pub_coverage;

-- Access is checked again against the current publication state.
-- @session:id=1&user=prepared_management_reader:admin&password=111
prepare reader_coverage from 'show publication coverage prepared_management_pub';
execute reader_coverage;
prepare reader_pattern from 'show publications like ?';
set @reader_filter = 'prepared_management%';
execute reader_pattern using @reader_filter;
-- @session
alter publication prepared_management_pub account sys;
-- @session:id=1&user=prepared_management_reader:admin&password=111
execute reader_coverage;
deallocate prepare reader_coverage;
execute reader_pattern using @reader_filter;
deallocate prepare reader_pattern;
-- @session

-- PREPARE must not drop the publication; missing-object errors must not break reuse.
prepare pub_drop from 'drop publication prepared_management_pub';
execute pub_coverage;
execute pub_drop;
execute pub_drop;
execute pub_alter;
execute pub_coverage;
execute pub_show;
execute pub_pattern using @pub_filter;
prepare pub_drop_if from 'drop publication if exists prepared_management_pub';
execute pub_drop_if;
execute pub_create;
-- @ignore:5,6
execute pub_pattern using @pub_filter;
execute pub_coverage;
execute pub_drop_if;
execute pub_drop_if;
execute pub_create_if;
execute pub_coverage;
execute pub_drop_if;

deallocate prepare pub_create;
deallocate prepare pub_create_if;
deallocate prepare pub_alter;
deallocate prepare pub_show;
deallocate prepare pub_pattern;
deallocate prepare pub_ddl;
deallocate prepare pub_coverage;
deallocate prepare pub_drop;
deallocate prepare pub_drop_if;
drop database prepared_management_db;
drop account prepared_management_reader;
