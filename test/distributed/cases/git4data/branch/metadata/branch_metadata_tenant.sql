-- Verify data branch create/delete works in a normal tenant.

drop account if exists acc_branch_meta;
create account acc_branch_meta admin_name "root1" identified by "111";

-- @session:id=1&user=acc_branch_meta:root1&password=111
-- Case 1: data branch create/delete table in normal tenant.
drop database if exists br_meta_db;
create database br_meta_db;
use br_meta_db;

drop table if exists base_tbl;
create table base_tbl(a int primary key, b varchar(10));
insert into base_tbl values (1, 'a'), (2, 'b');

drop snapshot if exists sp_base_tbl;
create snapshot sp_base_tbl for table br_meta_db base_tbl;

drop table if exists branch_tbl;
data branch create table branch_tbl from base_tbl{snapshot="sp_base_tbl"};

-- @session
set @acc_id = (
  select account_id from mo_catalog.mo_account
  where account_name = 'acc_branch_meta'
);
set @branch_tbl_id = (
  select rel_id from mo_catalog.mo_tables
  where account_id = @acc_id and reldatabase = 'br_meta_db' and relname = 'branch_tbl'
);
select table_deleted from mo_catalog.mo_branch_metadata where table_id = @branch_tbl_id;
-- @session:id=1&user=acc_branch_meta:root1&password=111

data branch delete table br_meta_db.branch_tbl;

-- @session
select table_deleted from mo_catalog.mo_branch_metadata where table_id = @branch_tbl_id;
-- @session:id=1&user=acc_branch_meta:root1&password=111

drop snapshot if exists sp_base_tbl;
drop database if exists br_meta_db;

-- Case 2: data branch create/delete database in normal tenant.
drop database if exists src_db;
drop database if exists dst_db;

create database src_db;
use src_db;

create table t1(a int primary key);
create table t2(a int primary key);
insert into t1 values (1);
insert into t2 values (2);

data branch create database dst_db from src_db;

-- @session
set @dst_t1_id = (
  select rel_id from mo_catalog.mo_tables
  where account_id = @acc_id and reldatabase = 'dst_db' and relname = 't1'
);
set @dst_t2_id = (
  select rel_id from mo_catalog.mo_tables
  where account_id = @acc_id and reldatabase = 'dst_db' and relname = 't2'
);

select table_deleted from mo_catalog.mo_branch_metadata
  where table_id in (@dst_t1_id, @dst_t2_id)
  order by table_id;
-- @session:id=1&user=acc_branch_meta:root1&password=111

data branch delete database dst_db;

-- @session
select table_deleted from mo_catalog.mo_branch_metadata
  where table_id in (@dst_t1_id, @dst_t2_id)
  order by table_id;
-- @session:id=1&user=acc_branch_meta:root1&password=111

drop database if exists src_db;
drop database if exists dst_db;
-- @session

drop account if exists acc_branch_meta;
