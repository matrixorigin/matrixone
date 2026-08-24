-- Database CLONE must distinguish an existing destination, an empty source,
-- and a missing source without leaving catalog residue.

drop database if exists clone_existence_src;
drop database if exists clone_existence_existing;
drop database if exists clone_existence_ifne_dst;
drop database if exists clone_existence_empty_src;
drop database if exists clone_existence_empty_dst;
drop database if exists clone_existence_missing_dst;
drop database if exists clone_existence_missing_ifne_dst;
drop database if exists clone_existence_table_dst;
drop database if exists clone_existence_snapshot_dst;
drop snapshot if exists clone_existence_snapshot;

create database clone_existence_src;
create table clone_existence_src.payload(id int primary key, note varchar(20));
insert into clone_existence_src.payload values (1, 'source row');

-- IF NOT EXISTS is a no-op for an existing destination: no source table may
-- be added and the pre-existing data must remain unchanged.
create database clone_existence_existing;
create table clone_existence_existing.sentinel(v int);
insert into clone_existence_existing.sentinel values (11);
create database if not exists clone_existence_existing clone clone_existence_src;
show tables from clone_existence_existing;
select * from clone_existence_existing.sentinel;

-- The no-op does not require the source to exist once the destination does.
create database if not exists clone_existence_existing clone clone_existence_missing_src;
show tables from clone_existence_existing;
select * from clone_existence_existing.sentinel;

-- Without IF NOT EXISTS, retain the duplicate-database error and leave the
-- destination unchanged.
create database clone_existence_existing clone clone_existence_src;
show tables from clone_existence_existing;
select * from clone_existence_existing.sentinel;

-- IF NOT EXISTS still clones a missing destination, preserving both schema
-- and rows from the populated source.
create database if not exists clone_existence_ifne_dst clone clone_existence_src;
show create table clone_existence_ifne_dst.payload;
select * from clone_existence_ifne_dst.payload;

-- A real empty source is valid and produces an empty destination.
create database clone_existence_empty_src;
create database clone_existence_empty_dst clone clone_existence_empty_src;
show tables from clone_existence_empty_dst;
select count(*) from mo_catalog.mo_database where datname = 'clone_existence_empty_dst';

-- A missing database source must fail and leave no destination database.
create database clone_existence_missing_dst clone clone_existence_missing_src;
select count(*) from mo_catalog.mo_database where datname = 'clone_existence_missing_dst';

-- IF NOT EXISTS must not turn a missing source into a successful empty clone.
create database if not exists clone_existence_missing_ifne_dst clone clone_existence_missing_src;
select count(*) from mo_catalog.mo_database where datname = 'clone_existence_missing_ifne_dst';

-- Keep the existing missing-table behavior and verify it has no residue.
create database clone_existence_table_dst;
create table clone_existence_table_dst.missing clone clone_existence_src.missing;
select count(*) from mo_catalog.mo_tables
where reldatabase = 'clone_existence_table_dst' and relname = 'missing';

-- A source present only at the requested snapshot remains a valid source.
create snapshot clone_existence_snapshot for database clone_existence_src;
drop database clone_existence_src;
create database clone_existence_snapshot_dst clone clone_existence_src {snapshot = 'clone_existence_snapshot'};
show create table clone_existence_snapshot_dst.payload;
select * from clone_existence_snapshot_dst.payload;

-- A database owned only by another tenant must not suppress a sys-account
-- clone target with the same name.
drop account if exists clone_existence_tenant;
create account clone_existence_tenant admin_name "root" identified by "111";
-- @session:id=7&user=clone_existence_tenant:root&password=111
create database clone_existence_account_collision;
create table clone_existence_account_collision.tenant_sentinel(v int);
insert into clone_existence_account_collision.tenant_sentinel values (7);
create database clone_existence_tenant_only_source;
create snapshot clone_existence_cross_account_snapshot for database clone_existence_tenant_only_source;
-- @session

create database clone_existence_account_collision_source;
create table clone_existence_account_collision_source.payload(id int primary key);
insert into clone_existence_account_collision_source.payload values (2);
create database if not exists clone_existence_account_collision clone clone_existence_account_collision_source;
select * from clone_existence_account_collision.payload;

-- A source visible only in another tenant must remain missing to sys and must
-- not leave an empty destination behind.
create database clone_existence_tenant_only_destination clone clone_existence_tenant_only_source;
select count(*) from mo_catalog.mo_database
where datname = 'clone_existence_tenant_only_destination' and account_id = 0;

-- Target authorization is checked before IF NOT EXISTS can make this a no-op.
drop account if exists clone_existence_target;
create account clone_existence_target admin_name "root" identified by "111";
-- @session:id=8&user=clone_existence_target:root&password=111
create database clone_existence_target_probe;
create table clone_existence_target_probe.sentinel(v int);
insert into clone_existence_target_probe.sentinel values (8);
-- @session:id=7&user=clone_existence_tenant:root&password=111
create database if not exists clone_existence_target_probe clone clone_existence_tenant_only_source {snapshot = 'clone_existence_cross_account_snapshot'} to account clone_existence_target;
-- @session:id=8&user=clone_existence_target:root&password=111
select * from clone_existence_target_probe.sentinel;
-- @session:id=7&user=clone_existence_tenant:root&password=111
drop snapshot clone_existence_cross_account_snapshot;
-- @session

drop database clone_existence_account_collision;
drop database clone_existence_account_collision_source;
drop database if exists clone_existence_tenant_only_destination;
drop account clone_existence_target;
drop account clone_existence_tenant;

drop snapshot clone_existence_snapshot;
drop database clone_existence_snapshot_dst;
drop database clone_existence_table_dst;
drop database clone_existence_empty_dst;
drop database clone_existence_empty_src;
drop database if exists clone_existence_missing_ifne_dst;
drop database clone_existence_ifne_dst;
drop database clone_existence_existing;
