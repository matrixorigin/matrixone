-- @bvt:issue#25368
set global enable_privilege_cache = off;
drop iceberg catalog if exists dist_iceberg_cat;
create iceberg catalog dist_iceberg_cat with ('uri' = 'https://catalog.example.com/rest', 'type' = 'rest', 'warehouse' = 's3://warehouse', 'auth_mode' = 'none');
show iceberg catalogs;
alter iceberg catalog dist_iceberg_cat set ('warehouse' = 's3://warehouse_v2');
show iceberg catalogs;
create iceberg catalog dist_iceberg_missing with ('type' = 'rest');
create iceberg catalog dist_iceberg_cat with ('uri' = 'https://catalog.example.com/dup');

drop user if exists dist_iceberg_user;
drop role if exists dist_iceberg_reader;
create role dist_iceberg_reader;
create user dist_iceberg_user identified by '111' default role dist_iceberg_reader;
grant connect on account * to dist_iceberg_reader;
-- @session:id=1&user=sys:dist_iceberg_user:dist_iceberg_reader&password=111
create iceberg catalog denied_cat with ('uri' = 'https://catalog.example.com/rest');
show iceberg catalogs;
drop iceberg catalog dist_iceberg_cat;
-- @session

drop user if exists dist_iceberg_user;
drop role if exists dist_iceberg_reader;
drop iceberg catalog dist_iceberg_cat;
show iceberg catalogs;
set global enable_privilege_cache = on;
-- @bvt:issue
