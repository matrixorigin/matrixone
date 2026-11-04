drop account if exists mts_size_a;
drop account if exists mts_size_b;
drop database if exists mts_account_sizes;
create database mts_account_sizes;
create table mts_account_sizes.expected_size (account_name varchar(100), size double);
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

set @previous_save_query_result = @@save_query_result;
set save_query_result = on;

-- A single non-sys account uses the existing forced per-table path.
-- Ignore unstable display columns here; compare size explicitly below.
-- @ignore:2,5,6,7,8
show accounts like 'mts_size_a';
insert into mts_account_sizes.expected_size
select account_name, size from result_scan(last_query_id()) as r;
-- @ignore:2,5,6,7,8
show accounts like 'mts_size_b';
insert into mts_account_sizes.expected_size
select account_name, size from result_scan(last_query_id()) as r;

-- Multiple accounts use the aggregate path and must equal the forced results.
-- @sortkey:0
-- @ignore:2,5,6,7,8
show accounts like 'mts_size_%';
select r.account_name, r.size = e.size as matches_forced,
       r.size > 0 as has_size, r.snapshot_size = 0 as no_snapshot
from result_scan(last_query_id()) as r
join mts_account_sizes.expected_size as e on r.account_name = e.account_name
order by r.account_name;

-- A tenant sees only itself, and still gets a freshly calculated size.
-- @session:id=1&user=mts_size_a:admin&password=111
set save_query_result = on;
-- @ignore:2,5,6,7,8
show accounts;
select account_name, size > 0 as has_size, snapshot_size = 0 as no_snapshot
from result_scan(last_query_id()) as r;
set save_query_result = off;
drop table size_db.t;
-- @session}

-- Catalog deletion must exclude the old table's statistics from both paths.
delete from mts_account_sizes.expected_size where account_name = 'mts_size_a';
-- @ignore:2,5,6,7,8
show accounts like 'mts_size_a';
insert into mts_account_sizes.expected_size
select account_name, size from result_scan(last_query_id()) as r;
-- @sortkey:0
-- @ignore:2,5,6,7,8
show accounts like 'mts_size_%';
select r.account_name, r.size = e.size as matches_forced,
       r.snapshot_size = 0 as no_snapshot
from result_scan(last_query_id()) as r
join mts_account_sizes.expected_size as e on r.account_name = e.account_name
order by r.account_name;

set save_query_result = @previous_save_query_result;
drop account mts_size_a;
drop account mts_size_b;
drop database mts_account_sizes;
