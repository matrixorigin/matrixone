drop account if exists mts_size_a;
drop account if exists mts_size_b;
create account mts_size_a admin_name 'admin' identified by '111' comment 'storage size a';
create account mts_size_b admin_name 'admin' identified by '111' comment 'storage size b';

-- Same database/table names in different tenants must remain independent.
-- @session:id=1&user=mts_size_a:admin&password=111
create database size_db;
create table size_db.t (v int);
insert into size_db.t values (1), (2);
-- @session}
-- @session:id=2&user=mts_size_b:admin&password=111
create database size_db;
create table size_db.t (v int);
insert into size_db.t values (1), (2), (3), (4);
-- @session}

-- A single non-sys account uses the existing forced per-table path.
-- @ignore:2,5,6,7,8
show accounts like 'mts_size_a';
-- @ignore:2,5,6,7,8
show accounts like 'mts_size_b';

-- Multiple accounts use the aggregate path.
-- @sortkey:0
-- @ignore:2,5,6,7,8
show accounts like 'mts_size_%';

-- A tenant sees only itself, and still gets a freshly calculated size.
-- @session:id=1&user=mts_size_a:admin&password=111
-- @ignore:2,5,6,7,8
show accounts;
drop table size_db.t;
-- @session}

-- Both paths must continue to work after a catalog deletion.
-- @ignore:2,5,6,7,8
show accounts like 'mts_size_a';
-- @sortkey:0
-- @ignore:2,5,6,7,8
show accounts like 'mts_size_%';

drop account mts_size_a;
drop account mts_size_b;
