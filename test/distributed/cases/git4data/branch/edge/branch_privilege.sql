-- DATA BRANCH CREATE privilege bypass repro.
--
-- Official contract:
--   1. The user needs read privileges on the source table/database.
--   2. The user needs create privileges on the destination table/database.
--
-- Current bug:
--   DATA BRANCH CREATE TABLE succeeds when either source SELECT or target
--   CREATE TABLE privilege is missing.
--   DATA BRANCH CREATE DATABASE succeeds when either source read privilege or
--   CREATE DATABASE privilege is missing.

-- @bvt:issue#24840
set global enable_privilege_cache = off;

drop account if exists br_priv_bypass_acc;
create account br_priv_bypass_acc admin_name "admin" identified by "111";

-- @session:id=1&user=br_priv_bypass_acc:admin&password=111
drop database if exists br_priv_src;
create database br_priv_src;
use br_priv_src;

create table base_tbl(a int primary key, b varchar(20));
insert into base_tbl values (1, 'one'), (2, 'two');
create snapshot br_priv_tbl_sp for table br_priv_src base_tbl;
create snapshot br_priv_db_sp for database br_priv_src;

-- Roles for table-branch privilege matrix.
create role r_tbl_select_only;
create role r_tbl_create_only;
create role r_tbl_both;

grant select on table br_priv_src.base_tbl to r_tbl_select_only;
grant create table on database br_priv_src to r_tbl_create_only;
grant select on table br_priv_src.base_tbl to r_tbl_both;
grant create table on database br_priv_src to r_tbl_both;

create user u_tbl_select_only identified by '111' default role r_tbl_select_only;
create user u_tbl_create_only identified by '111' default role r_tbl_create_only;
create user u_tbl_both identified by '111' default role r_tbl_both;

-- Roles for database-branch privilege matrix.
create role r_db_select_only;
create role r_db_create_only;
create role r_db_both;

grant select on table br_priv_src.base_tbl to r_db_select_only;
grant create database on account * to r_db_create_only;
grant select on table br_priv_src.base_tbl to r_db_both;
grant create database on account * to r_db_both;

create user u_db_select_only identified by '111' default role r_db_select_only;
create user u_db_create_only identified by '111' default role r_db_create_only;
create user u_db_both identified by '111' default role r_db_both;

-- Case 1: TABLE branch with only source SELECT privilege.
-- Expected after fix:
--   should fail because the user does not have CREATE TABLE on br_priv_src.
-- Current actual:
--   succeeds and creates br_tbl_select_only.
-- @session:id=2&user=br_priv_bypass_acc:u_tbl_select_only:r_tbl_select_only&password=111
data branch create table br_priv_src.br_tbl_select_only
  from br_priv_src.base_tbl{snapshot='br_priv_tbl_sp'};

-- @session:id=1&user=br_priv_bypass_acc:admin&password=111
select count(*) as br_tbl_select_only_exists
  from mo_catalog.mo_tables
 where reldatabase = 'br_priv_src'
   and relname = 'br_tbl_select_only';

-- Case 2: TABLE branch with only target CREATE TABLE privilege.
-- Expected after fix:
--   should fail because the user does not have SELECT on br_priv_src.base_tbl.
-- Current actual:
--   succeeds and creates br_tbl_create_only.
-- @session:id=3&user=br_priv_bypass_acc:u_tbl_create_only:r_tbl_create_only&password=111
data branch create table br_priv_src.br_tbl_create_only
  from br_priv_src.base_tbl{snapshot='br_priv_tbl_sp'};

-- @session:id=1&user=br_priv_bypass_acc:admin&password=111
select count(*) as br_tbl_create_only_exists
  from mo_catalog.mo_tables
 where reldatabase = 'br_priv_src'
   and relname = 'br_tbl_create_only';

-- Case 3: TABLE branch with both privileges.
-- Expected:
--   succeeds.
-- @session:id=4&user=br_priv_bypass_acc:u_tbl_both:r_tbl_both&password=111
data branch create table br_priv_src.br_tbl_both
  from br_priv_src.base_tbl{snapshot='br_priv_tbl_sp'};

-- @session:id=1&user=br_priv_bypass_acc:admin&password=111
select count(*) as br_tbl_both_exists
  from mo_catalog.mo_tables
 where reldatabase = 'br_priv_src'
   and relname = 'br_tbl_both';

-- Case 4: DATABASE branch with only source-table SELECT privilege.
-- Expected after fix:
--   should fail because the user does not have CREATE DATABASE.
-- Current actual:
--   succeeds and creates br_db_select_only.
-- @session:id=5&user=br_priv_bypass_acc:u_db_select_only:r_db_select_only&password=111
data branch create database br_db_select_only
  from br_priv_src{snapshot='br_priv_db_sp'};

-- @session:id=1&user=br_priv_bypass_acc:admin&password=111
select count(*) as br_db_select_only_exists
  from mo_catalog.mo_database
 where datname = 'br_db_select_only';

-- Case 5: DATABASE branch with only CREATE DATABASE privilege.
-- Expected after fix:
--   should fail because the user does not have read privilege on source data.
-- Current actual:
--   succeeds and creates br_db_create_only.
-- @session:id=6&user=br_priv_bypass_acc:u_db_create_only:r_db_create_only&password=111
data branch create database br_db_create_only
  from br_priv_src{snapshot='br_priv_db_sp'};

-- @session:id=1&user=br_priv_bypass_acc:admin&password=111
select count(*) as br_db_create_only_exists
  from mo_catalog.mo_database
 where datname = 'br_db_create_only';

-- Case 6: DATABASE branch with both privileges.
-- Expected:
--   succeeds.
-- @session:id=7&user=br_priv_bypass_acc:u_db_both:r_db_both&password=111
data branch create database br_db_both
  from br_priv_src{snapshot='br_priv_db_sp'};

-- @session:id=1&user=br_priv_bypass_acc:admin&password=111
select count(*) as br_db_both_exists
  from mo_catalog.mo_database
 where datname = 'br_db_both';

drop snapshot br_priv_tbl_sp;
drop snapshot br_priv_db_sp;
drop database if exists br_priv_src;
drop database if exists br_db_select_only;
drop database if exists br_db_create_only;
drop database if exists br_db_both;

-- @session
drop account if exists br_priv_bypass_acc;
set global enable_privilege_cache = on;
-- @bvt:issue
