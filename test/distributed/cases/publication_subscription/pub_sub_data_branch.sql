-- Issue #30949: subscription databases are aliases over a published
-- database; CREATE SNAPSHOT and DATA BRANCH resolve their tables through
-- the physical owner. Write destinations into a subscription stay
-- prohibited, publication scope/liveness keep their documented causes, and
-- publication-routed deletion binds coverage plus recorded identity.
-- Design (v7, approved in
-- artifacts/issue-30949/pre-pr-review-1792751823194.json):
-- docs/design/20261023-subscription-table-branch-snapshot-routing.md
--
-- B4's finding-10 companion case (a persisted foreign-source mo_pubs row
-- whose general-account publisher targets accounts beyond the source owner)
-- has no SQL reproducer: CREATE and ALTER PUBLICATION both revalidate the
-- foreign-source rule and mo_pubs is not writable through SQL. Its
-- executable proof is the persisted-row unit coverage in
-- pkg/frontend/snapshot_publication_test.go (U7), mirroring
-- publication_view_scope_test.go's injected rows.
--
-- B7's deterministic create/drop-vs-ALTER race interleavings are not
-- expressible in the single-threaded runner; the file asserts the
-- sequential revoke-then-drop outcome and the all-or-nothing race property
-- rests on the unit-level admission/revalidation coverage (U5, U7).
--
-- R1/R2 (review finding 1): operands pinned to an old snapshot or an MO_TS
-- classify in that timestamp's namespace. R1 snapshots a LOCAL table twice,
-- replaces the database with a same-named subscription, and proves the
-- pinned operands still resolve the historical local identity (the
-- pre-fix code classified with the current namespace and failed scope
-- validation against the publisher). R2 compares MO_TS-pinned reads of a
-- subscription with ordinary SELECT: visible results, invalid timestamp
-- rejection, and absent-relation rejection match the read path.

drop account if exists tenant_a;
drop account if exists tenant_b;
drop account if exists vw02;
drop account if exists vw03;
create account tenant_a admin_name = 'test_account' identified by '111';
create account tenant_b admin_name = 'test_account' identified by '111';
create account vw02 admin_name = 'test_account' identified by '111';
create account vw03 admin_name = 'test_account' identified by '111';

-- =====================================================================
-- setup: publisher database tdb{t1,t2} published to tenant_b
-- =====================================================================
-- @session:id=1&user=tenant_a:test_account&password=111
drop database if exists tdb;
create database tdb;
use tdb;
create table t1(a int primary key, b varchar(10));
insert into t1 values (1,'1'),(2,'2'),(3,'3');
create table t2(a int primary key, b varchar(10));
insert into t2 values (1,'1'),(2,'2');
create snapshot pub_owned_sp for table tdb t2;
create publication pub1 database tdb account tenant_b;
-- @session

-- =====================================================================
-- B1 (issue statements, alias form): snapshot + self-diff on sub3
-- =====================================================================
-- @session:id=2&user=tenant_b:test_account&password=111
drop database if exists sub3;
create database sub3 from tenant_a publication pub1;
select count(*) from sub3.t1;
show tables from sub3;
select table_name from information_schema.tables where table_schema='sub3' order by table_name;
create snapshot sp_sub_t1 for table sub3 t1;
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname = 'sp_sub_t1';
data branch diff sub3.t1 against sub3.t1;
-- @session

-- the record lives under the physical owner and anchors a cross-time diff
-- @session:id=1&user=tenant_a:test_account&password=111
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname = 'sp_sub_t1';
insert into tdb.t1 values (4,'4');
data branch diff tdb.t1{snapshot="sp_sub_t1"} against tdb.t1;
-- @session

-- subscriber-side after the publisher insert: self diff still empty
-- @session:id=2&user=tenant_b:test_account&password=111
data branch diff sub3.t1 against sub3.t1;
-- @session

-- the explicit form records the same way; scope-bound drops remove only
-- the bound rows, and the unrelated publisher snapshot survives (B9)
-- @session:id=2&user=tenant_b:test_account&password=111
create snapshot sp_explicit_t1 for table tdb t1 from tenant_a publication pub1;
drop snapshot sp_sub_t1 from tenant_a publication pub1;
drop snapshot sp_explicit_t1 from tenant_a publication pub1;
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots;
-- @session
-- @session:id=1&user=tenant_a:test_account&password=111
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots order by sname;
-- @session

-- =====================================================================
-- B2: mixed operands. Reads route; writes into a subscription stay
-- prohibited with the write-destination error.
-- =====================================================================
-- @session:id=2&user=tenant_b:test_account&password=111
drop database if exists ldb;
create database ldb;
use ldb;
create table localt(a int primary key, b varchar(10));
insert into localt values (1,'1'),(2,'2'),(3,'3'),(4,'4');
data branch diff sub3.t1 against ldb.localt;
insert into localt values (5,'5');
data branch diff ldb.localt against sub3.t1;
-- @session
-- @session:id=1&user=tenant_a:test_account&password=111
insert into tdb.t1 values (6,'6');
-- @session
-- @session:id=2&user=tenant_b:test_account&password=111
data branch merge sub3.t1 into ldb.localt;
select * from ldb.localt order by a;
-- @regex("cannot perform DATA BRANCH MERGE into subscription database", true)
data branch merge ldb.localt into sub3.t1;
-- @regex("cannot perform DATA BRANCH PICK into subscription database", true)
data branch pick ldb.localt into sub3.t1 keys(1);
select * from ldb.localt order by a;
-- @session
-- @session:id=1&user=tenant_a:test_account&password=111
select * from tdb.t1 order by a;
-- @session

-- =====================================================================
-- B3: table-scoped publication; out-of-scope operand keeps the
-- visibility cause; a dropped publication reports its own cause. A
-- genuinely missing local table keeps the engine error.
-- =====================================================================
-- @session:id=1&user=tenant_a:test_account&password=111
create database scdb;
create table scdb.s1(a int primary key, b varchar(10));
insert into scdb.s1 values (1,'1');
create table scdb.s2(a int primary key, b varchar(10));
insert into scdb.s2 values (1,'1');
create publication pubsc database scdb table s1 account tenant_b;
-- @session
-- @session:id=2&user=tenant_b:test_account&password=111
drop database if exists subsc;
create database subsc from tenant_a publication pubsc;
data branch diff subsc.s1 against subsc.s1;
create snapshot sp_sc for table subsc s1;
-- @regex("outside the publication's scope", true)
data branch diff subsc.s2 against subsc.s2;
-- @regex("outside the publication's scope", true)
create snapshot sp_sc2 for table subsc s2;
drop snapshot sp_sc from tenant_a publication pubsc;
-- @session
-- @session:id=1&user=tenant_a:test_account&password=111
drop publication pubsc;
-- @session
-- @session:id=2&user=tenant_b:test_account&password=111
-- @regex("there is no publication", true)
data branch diff subsc.s1 against subsc.s1;
-- @regex("there is no publication", true)
create snapshot sp_sc3 for table subsc s1;
drop database subsc;
-- @regex("does not exist", true)
data branch diff ldb.nope against ldb.localt;
-- @session
-- @session:id=1&user=tenant_a:test_account&password=111
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname in ('sp_sc','sp_sc2','sp_sc3');
-- @session

-- =====================================================================
-- B4: sys-published foreign source with a same-named publisher decoy
-- (the pub_view_scope.sql:534-568 arrangement). Reads, DIFF and the
-- alias snapshot resolve the physical source account vw02, never the
-- decoy; the record is scoped to vw02; the scope-bound drop removes
-- exactly the bound record while the decoy-side and the source owner's
-- independent snapshots are asserted intact.
-- =====================================================================
-- @session:id=4&user=vw02:test_account&password=111
drop database if exists fsrc_db;
create database fsrc_db;
create table fsrc_db.base(id int primary key, w int);
insert into fsrc_db.base values (5, 50);
create snapshot sp_src_base for table fsrc_db base;
-- @session

drop publication if exists p_fsrc;
create publication p_fsrc database fsrc_db table base account vw02,vw03;
-- the publisher (sys) now owns a same-named decoy database with different
-- objects, including a decoy-only table the publication never lists
drop database if exists fsrc_db;
create database fsrc_db;
create table fsrc_db.base(id int, w int);
insert into fsrc_db.base values (500, 5000);
create table fsrc_db.decoy_only(id int primary key);
insert into fsrc_db.decoy_only values (1);
create snapshot sp_decoy_base for table fsrc_db base;
-- @session:id=5&user=vw03:test_account&password=111
drop database if exists s_fsrc;
create database s_fsrc from sys publication p_fsrc;
select id, w from s_fsrc.base;
data branch diff s_fsrc.base against s_fsrc.base;
create snapshot sp_fsrc_base for table s_fsrc base;
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname = 'sp_fsrc_base';
-- @session

-- the record is visible to the physical source owner alone; the scan is
-- name-scoped so snapshots left behind by other suites cannot pollute it
-- @session:id=4&user=vw02:test_account&password=111
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname in ('sp_decoy_base','sp_fsrc_base','sp_src_base','sp_fsrc_decoy') order by sname;
-- @session
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname in ('sp_decoy_base','sp_fsrc_base','sp_src_base','sp_fsrc_decoy') order by sname;
-- @session:id=5&user=vw03:test_account&password=111
-- the decoy-only table is neither readable through the subscription nor
-- branch-addressable: the scope error, never decoy data
-- @regex("outside the publication's scope", true)
data branch diff s_fsrc.decoy_only against s_fsrc.decoy_only;
-- @regex("outside the publication's scope", true)
create snapshot sp_fsrc_decoy for table s_fsrc decoy_only;
drop snapshot sp_fsrc_base from sys publication p_fsrc;
-- @session
-- @session:id=4&user=vw02:test_account&password=111
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname in ('sp_decoy_base','sp_fsrc_base','sp_src_base','sp_fsrc_decoy') order by sname;
-- @session
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname in ('sp_decoy_base','sp_fsrc_base','sp_src_base','sp_fsrc_decoy') order by sname;
drop snapshot sp_decoy_base;
-- @session:id=4&user=vw02:test_account&password=111
drop snapshot sp_src_base;
-- @session

-- =====================================================================
-- B5: non-admin subscriber users. A SELECT grant on the subscription
-- database admits the routed DIFF; without the grant the privilege error
-- keeps its text; out-of-scope and genuinely missing tables keep their
-- distinguishable causes for a granted user.
-- =====================================================================
-- @session:id=1&user=tenant_a:test_account&password=111
create publication pubsc2 database scdb table s1 account tenant_b;
-- @session
-- @session:id=2&user=tenant_b:test_account&password=111
drop database if exists subsc2;
create database subsc2 from tenant_a publication pubsc2;
drop user if exists branch_ro;
drop user if exists branch_np;
drop role if exists branch_ro_role;
drop role if exists branch_np_role;
create role branch_ro_role, branch_np_role;
create user branch_ro identified by '111' default role branch_ro_role;
create user branch_np identified by '111' default role branch_np_role;
grant connect on account * to branch_ro_role;
grant connect on account * to branch_np_role;
grant select on table sub3.* to branch_ro_role;
grant select on table ldb.* to branch_ro_role;
grant select on table subsc2.* to branch_ro_role;
-- @session
-- @session:id=6&user=tenant_b:branch_ro:branch_ro_role&password=111
data branch diff sub3.t1 against sub3.t1;
-- @regex("outside the publication's scope", true)
data branch diff subsc2.s2 against subsc2.s2;
-- @regex("does not exist", true)
data branch diff ldb.nope against ldb.localt;
-- @session
-- @session:id=7&user=tenant_b:branch_np:branch_np_role&password=111
-- @regex("do not have privilege to execute the statement", true)
data branch diff sub3.t1 against sub3.t1;
-- @session

-- =====================================================================
-- B6: clone-from-subscription lineage regression (zero-history semantics
-- identical to local clones)
-- =====================================================================
-- @session:id=2&user=tenant_b:test_account&password=111
data branch create table c1 from sub3.t1;
data branch create table c2 from sub3.t1;
data branch diff c2 against c1;
data branch diff c1 against c2;
insert into c1 values (7,'7');
data branch diff c1 against c2;
-- @session

-- =====================================================================
-- B8: committed foreign lineage + independent updates report the exact
-- mixed change sets in both orders; MERGE equals the local-local result;
-- MERGE into the subscription stays rejected.
-- =====================================================================
-- @session:id=1&user=tenant_a:test_account&password=111
update tdb.t1 set b='1a' where a=1;
delete from tdb.t1 where a=2;
insert into tdb.t1 values (8,'8');
-- @session
-- @session:id=2&user=tenant_b:test_account&password=111
update c2 set b='3c' where a=3;
update c2 set b='4c' where a=4;
data branch diff c2 against sub3.t1;
data branch diff sub3.t1 against c2;
data branch merge sub3.t1 into c2;
select * from c2 order by a;
-- @regex("cannot perform DATA BRANCH MERGE into subscription database", true)
data branch merge c2 into sub3.t1;
-- @session
-- @session:id=1&user=tenant_a:test_account&password=111
select * from tdb.t1 order by a;
-- @session

-- =====================================================================
-- B10: DATABASE * publication lifecycle through the explicit forms
-- =====================================================================
drop database if exists sysdb;
create database sysdb;
create table sysdb.st1(a int primary key, b varchar(10));
insert into sysdb.st1 values (1,'1');
drop publication if exists puball;
create publication puball database * account tenant_b;
-- @session:id=1&user=tenant_a:test_account&password=111
create snapshot sp_ta_acc for account;
-- @session
-- @session:id=2&user=tenant_b:test_account&password=111
create snapshot sp_star_tab for table sysdb st1 from sys publication puball;
create snapshot sp_star_db for database sysdb from sys publication puball;
create snapshot sp_star_acc for account from sys publication puball;
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname in ('sp_star_tab','sp_star_db','sp_star_acc');
-- @session
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname in ('sp_star_tab','sp_star_db','sp_star_acc') order by sname;
-- @session:id=2&user=tenant_b:test_account&password=111
-- a third account's account-level record cannot load under the routed
-- publication owner, so the name-absence cause rejects the drop and the
-- record stays intact under tenant_a (asserted below)
-- @regex("does not exist", true)
drop snapshot sp_ta_acc from sys publication puball;
-- a sys-owned account-level record of a different target account loads but
-- binds objId != P_pub: rejected with the scope cause and intact
-- @session
create snapshot sp_sys_b_acc for account tenant_b;
-- @session:id=2&user=tenant_b:test_account&password=111
-- @regex("outside the publication's scope", true)
drop snapshot sp_sys_b_acc from sys publication puball;
drop snapshot sp_star_tab from sys publication puball;
drop snapshot sp_star_db from sys publication puball;
drop snapshot sp_star_acc from sys publication puball;
-- @regex("does not exist", true)
drop snapshot not_here from sys publication puball;
drop snapshot if exists not_here from sys publication puball;
-- @session
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname in ('sp_star_tab','sp_star_db','sp_star_acc','sp_ta_acc','sp_sys_b_acc') order by sname;
-- @session:id=1&user=tenant_a:test_account&password=111
-- both rejected records survived: sp_ta_acc under its owner (dropped below
-- through the ordinary retained path) and sp_sys_b_acc under sys
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname = 'sp_ta_acc';
drop snapshot sp_ta_acc;
-- @session
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname = 'sp_sys_b_acc';
drop snapshot sp_sys_b_acc;

-- =====================================================================
-- B11: replaced-database retention, dropped-table historical fallback
-- (whole-database only), same-database replacement identity, table-limited
-- publications, COPY ALTER continuity, and the stale foreign-owned list.
-- =====================================================================
-- replaced database: old records keep their stale ids and stay with the
-- owner; publication-routed drops are rejected and the records intact.
-- @session:id=1&user=tenant_a:test_account&password=111
drop database if exists rdb;
create database rdb;
create table rdb.t(a int primary key, b varchar(10));
insert into rdb.t values (1,'1');
create snapshot sp_rdb for database rdb;
create snapshot sp_rdb_t for table rdb t;
drop database rdb;
create database rdb;
create table rdb.t(a int primary key, b varchar(10));
insert into rdb.t values (9,'9');
create publication pubrdb database rdb account tenant_b;
-- @session
-- @session:id=2&user=tenant_b:test_account&password=111
-- @regex("outside the publication's scope", true)
drop snapshot sp_rdb from tenant_a publication pubrdb;
-- @regex("outside the publication's scope", true)
drop snapshot sp_rdb_t from tenant_a publication pubrdb;
-- @session
-- @session:id=1&user=tenant_a:test_account&password=111
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname in ('sp_rdb','sp_rdb_t') order by sname;
drop snapshot sp_rdb;
drop snapshot sp_rdb_t;
-- @session

-- dropped table under whole-database coverage deletes through the
-- historical fallback; the same final state under a table-limited list
-- fails closed.
-- @session:id=1&user=tenant_a:test_account&password=111
drop database if exists hdb;
create database hdb;
create table hdb.t1(a int primary key, b varchar(10));
insert into hdb.t1 values (1,'1');
create snapshot sp_hdb_t1 for table hdb t1;
create publication pubhdb database hdb account tenant_b;
drop table hdb.t1;
-- @session
-- @session:id=2&user=tenant_b:test_account&password=111
drop snapshot sp_hdb_t1 from tenant_a publication pubhdb;
-- @session
-- @session:id=1&user=tenant_a:test_account&password=111
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname = 'sp_hdb_t1';
create table hdb.t2(a int primary key, b varchar(10));
insert into hdb.t2 values (1,'1');
create publication pubhdb2 database hdb table t2 account tenant_b;
create snapshot sp_hdb_t2 for table hdb t2;
drop table hdb.t2;
-- @session
-- @session:id=2&user=tenant_b:test_account&password=111
-- @regex("outside the publication's scope", true)
drop snapshot sp_hdb_t2 from tenant_a publication pubhdb2;
-- @session
-- @session:id=1&user=tenant_a:test_account&password=111
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname = 'sp_hdb_t2';
drop snapshot sp_hdb_t2;
-- @session

-- same-database drop/recreate with only the replacement published: the old
-- record binds a different logical identity and survives; the replacement's
-- own record deletes; the '*' control deletes the old record through the
-- historical fallback once the table is gone.
-- @session:id=1&user=tenant_a:test_account&password=111
drop database if exists lidb;
create database lidb;
create table lidb.t(a int primary key, b varchar(10));
insert into lidb.t values (1,'1');
create snapshot sp_li_old for table lidb t;
drop table lidb.t;
create table lidb.t(a int primary key, b varchar(10));
insert into lidb.t values (2,'2');
create publication publi database lidb table t account tenant_b;
create snapshot sp_li_new for table lidb t;
-- @session
-- @session:id=2&user=tenant_b:test_account&password=111
-- @regex("outside the publication's scope", true)
drop snapshot sp_li_old from tenant_a publication publi;
drop snapshot sp_li_new from tenant_a publication publi;
-- @session
-- @session:id=1&user=tenant_a:test_account&password=111
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname in ('sp_li_old','sp_li_new') order by sname;
drop table lidb.t;
create publication publi_all database lidb account tenant_b;
-- @session
-- @session:id=2&user=tenant_b:test_account&password=111
drop snapshot sp_li_old from tenant_a publication publi_all;
-- @session
-- @session:id=1&user=tenant_a:test_account&password=111
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname in ('sp_li_old','sp_li_new') order by sname;
-- @session

-- COPY ALTER continuity: the logical id survives the physical replacement
-- (rel_id changes, rel_logical_id does not), so pre-ALTER, alias
-- post-ALTER and explicit post-ALTER records all delete.
-- @session:id=1&user=tenant_a:test_account&password=111
drop database if exists cadb;
create database cadb;
create table cadb.t(a int primary key, b varchar(10));
insert into cadb.t values (1,'1');
create publication pubca database cadb account tenant_b;
create snapshot sp_ca_pre for table cadb t;
-- @session
-- @session:id=2&user=tenant_b:test_account&password=111
create database sub_ca from tenant_a publication pubca;
-- @session
-- @session:id=1&user=tenant_a:test_account&password=111
alter table cadb.t modify column b varchar(20), algorithm=copy;
select rel_id <> rel_logical_id as physical_replaced_logical_kept from mo_catalog.mo_tables where reldatabase = 'cadb' and relname = 't';
-- @session
-- @session:id=2&user=tenant_b:test_account&password=111
create snapshot sp_ca_alias for table sub_ca t;
create snapshot sp_ca_expl for table cadb t from tenant_a publication pubca;
drop snapshot sp_ca_pre from tenant_a publication pubca;
drop snapshot sp_ca_alias from tenant_a publication pubca;
drop snapshot sp_ca_expl from tenant_a publication pubca;
-- @session
-- @session:id=1&user=tenant_a:test_account&password=111
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname in ('sp_ca_pre','sp_ca_alias','sp_ca_expl');
-- @session

-- stale foreign-owned list: a sys-owned publication over tenant_a's
-- database keeps the dropped name in its table_list (DROP TABLE scrubs
-- only the dropping account's own publications); every table-limited
-- record whose authorized name no longer resolves fails closed and the
-- records stay byte-identical (owner cleanup retained), while the '*'
-- control deletes through the historical fallback.
-- the target list names the physical owner tenant_a plus the subscriber:
-- the create-time source resolution finds a foreign source only among the
-- publisher and target accounts (resolvePublicationDatabaseByName), the
-- pub_view_scope.sql:534-568 arrangement this section mirrors.
-- @session:id=1&user=tenant_a:test_account&password=111
drop database if exists fsdb;
create database fsdb;
create table fsdb.t(a int primary key, b varchar(10));
insert into fsdb.t values (1,'1');
create snapshot sp_fs_old for table fsdb t;
drop table fsdb.t;
create table fsdb.t(a int primary key, b varchar(10));
insert into fsdb.t values (2,'2');
create snapshot sp_fs_new for table fsdb t;
-- @session
drop publication if exists pubsfs;
create publication pubsfs database fsdb table t account tenant_a,tenant_b;
-- @session:id=2&user=tenant_b:test_account&password=111
drop database if exists subfs;
create database subfs from sys publication pubsfs;
select count(*) from subfs.t;
-- @session
-- @session:id=1&user=tenant_a:test_account&password=111
drop table fsdb.t;
-- @session
-- the sys-owned list still names the dropped table: stale by C10
select pub_name, database_name, table_list from mo_catalog.mo_pubs where pub_name = 'pubsfs';
-- @session:id=2&user=tenant_b:test_account&password=111
-- @regex("outside the publication's scope", true)
drop snapshot sp_fs_old from sys publication pubsfs;
-- @regex("outside the publication's scope", true)
drop snapshot sp_fs_new from sys publication pubsfs;
drop database subfs;
-- @session
-- @session:id=1&user=tenant_a:test_account&password=111
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname in ('sp_fs_old','sp_fs_new') order by sname;
-- @session
create publication pubsfs_all database fsdb account tenant_a,tenant_b;
-- @session:id=2&user=tenant_b:test_account&password=111
drop snapshot sp_fs_old from sys publication pubsfs_all;
drop snapshot sp_fs_new from sys publication pubsfs_all;
-- @session
-- @session:id=1&user=tenant_a:test_account&password=111
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname in ('sp_fs_old','sp_fs_new') order by sname;
-- @session

-- =====================================================================
-- B7: revoking the publication between create and drop closes the
-- publication-routed drop with the publication-gone cause and leaves the
-- record with the owner (sequential approximation; the deterministic
-- race interleavings are covered by the unit admission/revalidation
-- tests).
-- =====================================================================
-- @session:id=1&user=tenant_a:test_account&password=111
drop database if exists rcedb;
create database rcedb;
create table rcedb.t(a int primary key, b varchar(10));
insert into rcedb.t values (1,'1');
create publication pubrc database rcedb account tenant_b;
-- @session
-- @session:id=2&user=tenant_b:test_account&password=111
create database subrc from tenant_a publication pubrc;
create snapshot sp_rc for table subrc t;
-- @session
-- @session:id=1&user=tenant_a:test_account&password=111
drop publication pubrc;
-- @session
-- @session:id=2&user=tenant_b:test_account&password=111
-- @regex("does not exist", true)
drop snapshot sp_rc from tenant_a publication pubrc;
drop database subrc;
-- @session
-- @session:id=1&user=tenant_a:test_account&password=111
select sname, level, account_name, database_name, table_name from mo_catalog.mo_snapshots where sname = 'sp_rc';
drop snapshot sp_rc;
-- @session

-- =====================================================================
-- R1 (finding 1): snapshot-pinned operands classify in their snapshot's
-- namespace. The replaced database keeps its historical LOCAL identity
-- for the pinned DIFF, which reports the row committed between the two
-- local snapshots instead of rejecting the publisher's table identity.
-- =====================================================================
-- @session:id=1&user=tenant_a:test_account&password=111
drop database if exists tsdb;
create database tsdb;
create table tsdb.tt(a int primary key, b varchar(10));
insert into tsdb.tt values (1,'pub1');
create publication pubts database tsdb account tenant_b;
-- @session
-- @session:id=2&user=tenant_b:test_account&password=111
drop database if exists rpl;
create database rpl;
create table rpl.ct(a int primary key, b varchar(10));
insert into rpl.ct values (1,'l1'),(2,'l2');
create snapshot sp_rpl_local for table rpl ct;
insert into rpl.ct values (3,'l3');
create snapshot sp_rpl_local2 for table rpl ct;
drop database rpl;
create database rpl from tenant_a publication pubts;
data branch diff rpl.ct{snapshot="sp_rpl_local"} against rpl.ct{snapshot="sp_rpl_local2"};
data branch diff rpl.ct{snapshot="sp_rpl_local"} against rpl.ct{snapshot="sp_rpl_local"};
-- @session

-- =====================================================================
-- R2 (finding 1): MO_TS-pinned subscription operands agree with the
-- ordinary read path: max-ts reads see the copied-through data, invalid
-- timestamp values reject identically, and an absent relation rejects
-- identically under the pinned view.
-- =====================================================================
-- @session:id=2&user=tenant_b:test_account&password=111
data branch diff rpl.tt {MO_TS = '9223372036854775807-0'} against rpl.tt {MO_TS = '9223372036854775807-0'};
select count(*) from rpl.tt {MO_TS = '9223372036854775807-0'};
select count(*) from rpl.tt;
-- @regex("invalid timestamp value", true)
data branch diff rpl.tt{timestamp = 9223372036854775807} against rpl.tt{timestamp = 9223372036854775807};
-- @regex("invalid timestamp value", true)
select count(*) from rpl.tt {timestamp = 9223372036854775807};
-- @regex("table \"nope\" does not exist", true)
data branch diff rpl.nope {MO_TS = '9223372036854775807-0'} against rpl.nope {MO_TS = '9223372036854775807-0'};
-- @regex("table \"nope\" does not exist", true)
select count(*) from rpl.nope {MO_TS = '9223372036854775807-0'};
-- @session

-- =====================================================================
-- B12 (issue 28026, design D220): a publisher-side committed primary-key
-- move applied through subscription-routed windows folds to a native
-- UPDATE under the publisher's physical identity. The subscriber clone's
-- ON UPDATE CASCADE child re-parents (no silent ON DELETE CASCADE loss);
-- the RESTRICT twin aborts loudly with the destination byte-preserved;
-- the same-account clone keeps identical semantics as a parity control.
-- =====================================================================
-- @session:id=1&user=tenant_a:test_account&password=111
drop database if exists b12src;
create database b12src;
create table b12src.t(a int primary key, b varchar(10));
insert into b12src.t values (1,'one'),(2,'two');
create publication pub12 database b12src account tenant_b;
-- @session
-- @session:id=2&user=tenant_b:test_account&password=111
create database b12sub from tenant_a publication pub12;
drop database if exists b12db;
create database b12db;
use b12db;
data branch create table m12p from b12sub.t;
create table m12fk(id int primary key, pid int, foreign key(pid) references m12p(a) on delete cascade on update cascade);
insert into m12fk values (100,2);
data branch create table k12p from b12sub.t;
create table k12fk(id int primary key, pid int, foreign key(pid) references k12p(a) on delete cascade on update cascade);
insert into k12fk values (300,2);
data branch create table r12p from b12sub.t;
create table r12fk(id int primary key, pid int, foreign key(pid) references r12p(a));
insert into r12fk values (200,2);
create table n12p(a int primary key, b varchar(10));
insert into n12p values (1,'one'),(2,'two');
data branch create table n12s from n12p;
create table n12fk(id int primary key, pid int, foreign key(pid) references n12p(a) on delete cascade on update cascade);
insert into n12fk values (100,2);
update n12s set a=3 where a=2;
data branch merge n12s into n12p when conflict accept;
select * from n12p order by a;
select * from n12fk order by id;
-- @session
-- @session:id=1&user=tenant_a:test_account&password=111
update b12src.t set a=3 where a=2;
select count(*) as pub_link_rows from mo_catalog.mo_data_branch_update_link;
-- @session
-- @session:id=2&user=tenant_b:test_account&password=111
data branch merge b12sub.t into b12db.m12p when conflict accept;
select * from b12db.m12p order by a;
select * from b12db.m12fk order by id;
data branch pick b12sub.t into b12db.k12p keys(2) when conflict accept;
select * from b12db.k12p order by a;
select * from b12db.k12fk order by id;
-- @regex("Cannot delete or update a parent row: a foreign key constraint fails", true)
data branch pick b12sub.t into b12db.r12p keys(2) when conflict accept;
select * from b12db.r12p order by a;
select * from b12db.r12fk order by id;
-- @session

-- =====================================================================
-- cleanup
-- =====================================================================
drop publication if exists puball;
drop publication if exists pubsfs;
drop publication if exists pubsfs_all;
drop publication if exists p_fsrc;
drop database if exists sysdb;
drop database if exists fsrc_db;
-- @session:id=1&user=tenant_a:test_account&password=111
drop publication if exists pub1;
drop publication if exists pubsc2;
drop publication if exists pubhdb;
drop publication if exists pubhdb2;
drop publication if exists publi;
drop publication if exists publi_all;
drop publication if exists pubca;
drop publication if exists pubrdb;
drop publication if exists pubts;
drop publication if exists pub12;
drop database if exists tsdb;
drop database if exists tdb;
drop database if exists b12src;
drop database if exists scdb;
drop database if exists rdb;
drop database if exists hdb;
drop database if exists lidb;
drop database if exists cadb;
drop database if exists fsdb;
drop database if exists fsrc_db;
drop database if exists rcedb;
-- @session
-- @session:id=2&user=tenant_b:test_account&password=111
drop database if exists sub3;
drop database if exists b12sub;
drop database if exists b12db;
drop database if exists subsc2;
drop database if exists sub_ca;
drop database if exists ldb;
drop snapshot sp_rpl_local;
drop snapshot sp_rpl_local2;
drop database if exists rpl;
drop user if exists branch_ro;
drop user if exists branch_np;
drop role if exists branch_ro_role;
drop role if exists branch_np_role;
-- @session
-- @session:id=4&user=vw02:test_account&password=111
drop database if exists fsrc_db;
-- @session
-- @session:id=5&user=vw03:test_account&password=111
drop database if exists s_fsrc;
-- @session
drop account if exists tenant_a;
drop account if exists tenant_b;
drop account if exists vw02;
drop account if exists vw03;
