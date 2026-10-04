-- @bvt:issue#27353
drop database if exists mongodb_external_default_test;
create database mongodb_external_default_test;
use mongodb_external_default_test;

create external table ext_default (
  id varchar(8) mongodb_path '_id',
  missing_v varchar(16) default 'fallback' mongodb_path 'missing_v'
) engine=mongodb with (
  'connection'='unused_connection',
  'database'='unused_database',
  'collection'='unused_collection',
  'schema_mode'='explicit'
);

select count(*) from mo_catalog.mo_tables
where reldatabase = 'mongodb_external_default_test' and relname = 'ext_default';
select count(*) from mo_catalog.mo_mongodb_tables m
join mo_catalog.mo_tables t
  on m.account_id = t.account_id and m.db_id = t.reldatabase_id and m.table_id = t.rel_id
where t.reldatabase = 'mongodb_external_default_test' and t.relname = 'ext_default';

drop database mongodb_external_default_test;
-- @bvt:issue
