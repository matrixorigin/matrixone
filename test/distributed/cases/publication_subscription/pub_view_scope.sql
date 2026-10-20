-- table-level publications must publish the complete relation closure of their
-- views; a view whose dependencies are outside the explicit table list cannot
-- be queried through the subscription (issue #27767)
drop account if exists vw01;
drop account if exists vw02;
drop account if exists vw03;
drop account if exists vw04;
drop account if exists vw05;

drop database if exists view_pub_db;
create database view_pub_db;
use view_pub_db;

create table base(id int primary key, v int);
insert into base values (1,10),(2,20);
create view v_only as select id,v from base where v >= 10;

create table t1(a int primary key);
insert into t1 values (1),(2);
create table t2(a int primary key);
insert into t2 values (1),(3);
create view v_join as select t1.a from t1 join t2 on t1.a = t2.a;
create view v_nested as select a from v_join where a > 0;
create view v_const as select 1 as one;

-- an ACCOUNT ALL publication over a system relation only the system account
-- owns is rejected even while no general account exists yet: accounts created
-- later could never resolve the view
create view v_account as select account_name from mo_catalog.mo_account;
drop publication if exists p_all_bad;
create publication p_all_bad database view_pub_db table v_account account all;

-- a relation the account-initialization contract provisions for every
-- account the system creates resolves for future subscribers the moment they
-- exist, so ACCOUNT ALL publishes a view over it even on this fresh instance
-- whose only account is the system account; the account created next
-- subscribes and queries it
create view v_alluser as select user_name from mo_catalog.mo_user;
drop publication if exists p_alluser;
create publication p_alluser database view_pub_db table v_alluser account all;

create account vw01 admin_name = 'test_account' identified by '111';

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_alluser;
create database s_alluser from sys publication p_alluser;
select * from s_alluser.v_alluser order by user_name;
drop database s_alluser;
-- @session

-- a view without its base table in the explicit list is rejected
drop publication if exists p_view;
create publication p_view database view_pub_db table v_only account vw01;
select pub_name, database_name, table_list from mo_catalog.mo_pubs where account_name = 'sys' and database_name = 'view_pub_db' order by pub_name;

-- rejected creation followed by corrected creation in the same session works
create publication p_view database view_pub_db table base,v_only account vw01;
select pub_name, database_name, table_list from mo_catalog.mo_pubs where account_name = 'sys' and database_name = 'view_pub_db' order by pub_name;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_view;
create database s_view from sys publication p_view;
show create view s_view.v_only;
select count(*), sum(v) from s_view.v_only;
select * from s_view.t1;
drop database s_view;
-- @session

-- database-level publication control keeps supporting views
drop publication if exists p_db_level;
create publication p_db_level database view_pub_db account vw01;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_db_level;
create database s_db_level from sys publication p_db_level;
select count(*), sum(v) from s_db_level.v_only;
drop database s_db_level;
-- @session

-- missing intermediate view of a nested view is rejected
drop publication if exists p_nested_bad;
create publication p_nested_bad database view_pub_db table t1,v_nested account vw01;

-- missing base table of a two-table join view is rejected
drop publication if exists p_join_bad;
create publication p_join_bad database view_pub_db table t1,v_join account vw01;

-- dependency on a relation of another database is rejected
create database other_db_27767;
use other_db_27767;
create table src(a int);
use view_pub_db;
create view v_other as select a from other_db_27767.src;
drop publication if exists p_other_bad;
create publication p_other_bad database view_pub_db table v_other account vw01;
drop view v_other;

-- a system relation that general accounts cannot resolve is rejected: only
-- the system account owns mo_catalog.mo_account, so a subscriber could never
-- query a published view over it
drop publication if exists p_account_bad;
create publication p_account_bad database view_pub_db table v_account account vw01;
drop view v_account;

-- a system relation that every account owns is published and queryable
create view v_sysuser as select user_name from mo_catalog.mo_user;
drop publication if exists p_sysuser;
create publication p_sysuser database view_pub_db table v_sysuser account vw01;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_sysuser;
create database s_sysuser from sys publication p_sysuser;
select * from s_sysuser.v_sysuser order by user_name;
drop database s_sysuser;
-- @session

-- a view bound to a snapshot time travel is rejected: the publisher's
-- snapshot is not available to the subscriber account
create snapshot sp_pub_view_scope for account sys;
create view v_snap as select a from t1 {snapshot = 'sp_pub_view_scope'};
drop publication if exists p_snap_bad;
create publication p_snap_bad database view_pub_db table t1,v_snap account vw01;
drop view v_snap;
drop snapshot sp_pub_view_scope;

-- the complete closure of a nested join view is accepted and queryable
drop publication if exists p_full;
create publication p_full database view_pub_db table t1,t2,v_join,v_nested account vw01;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_full;
create database s_full from sys publication p_full;
select count(*) as nested_count from s_full.v_nested;
select count(*) as join_count from s_full.v_join;
drop database s_full;
-- @session

-- a constant view needs no relation
drop publication if exists p_const;
create publication p_const database view_pub_db table v_const account vw01;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_const;
create database s_const from sys publication p_const;
select * from s_const.v_const;
drop database s_const;
-- @session

-- a published view that calls a sequence function can never bind inside a
-- subscription: the binder resolves the sequence argument at execution time
-- in the executing account, and sequence objects are not routed through
-- subscriptions, so such a view would subscribe successfully and fail on
-- every query. Detection is a syntax walk - validation never executes a
-- sequence function
create sequence sq_scope as bigint increment by 1 minvalue 1 maxvalue 10000 start with 5 no cycle;
create view v_seq as select nextval('sq_scope') as n;
create view v_seq_outer as select n from v_seq;
drop publication if exists p_seq_bad;
create publication p_seq_bad database view_pub_db table v_seq account vw01;
drop publication if exists p_seq_nested_bad;
create publication p_seq_nested_bad database view_pub_db table v_seq,v_seq_outer account vw01;

-- the rejected creations leave no publication row
select pub_name from mo_catalog.mo_pubs where pub_name in ('p_seq_bad','p_seq_nested_bad') order by pub_name;
-- and the sequence is untouched by validation: its first allocation is the
-- start value, proving no probe consumed a value
select nextval('sq_scope') as first_allocation;

-- the genuine constant-view control passes next to the sequence rejections
drop publication if exists p_const_ctl;
create publication p_const_ctl database view_pub_db table v_const account vw01;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_const_ctl;
create database s_const_ctl from sys publication p_const_ctl;
select * from s_const_ctl.v_const;
drop database s_const_ctl;
-- @session

drop publication p_const_ctl;
drop view v_seq_outer;
drop view v_seq;
drop sequence sq_scope;

-- a view whose CTE is never referenced binds no relation: the unused CTE body
-- must not force its base table into the publication list
create view v_unusedcte as with unused as (select id from base) select 1 as one;
drop publication if exists p_unusedcte;
create publication p_unusedcte database view_pub_db table v_unusedcte account vw01;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_unusedcte;
create database s_unusedcte from sys publication p_unusedcte;
select * from s_unusedcte.v_unusedcte;
drop database s_unusedcte;
-- @session

-- the control: a referenced CTE still publishes its base table requirement
create view v_usedcte as with used as (select id from base) select count(*) as c from used;
drop publication if exists p_usedcte_bad;
create publication p_usedcte_bad database view_pub_db table v_usedcte account vw01;
drop publication if exists p_usedcte;
create publication p_usedcte database view_pub_db table base,v_usedcte account vw01;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_usedcte;
create database s_usedcte from sys publication p_usedcte;
select * from s_usedcte.v_usedcte;
drop database s_usedcte;
-- @session

-- a CTE name declared after a reference does not shadow the physical table:
-- the binder binds the reference inside a's body to the physical base table,
-- so the persisted dependency requires it even though a CTE named base exists
create view v_fwd as with a as (select id from base), base as (select 1 as id) select id from a;
drop publication if exists p_fwd_bad;
create publication p_fwd_bad database view_pub_db table v_fwd account vw01;
drop publication if exists p_fwd;
create publication p_fwd database view_pub_db table base,v_fwd account vw01;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_fwd;
create database s_fwd from sys publication p_fwd;
select * from s_fwd.v_fwd order by id;
drop database s_fwd;
-- @session

-- an unused CTE inside a derived table binds no relation either: the view
-- returns its constant independently of base
create view v_nestedcte as select * from (with unused as (select id from base) select 1 as one) as d;
drop publication if exists p_nestedcte;
create publication p_nestedcte database view_pub_db table v_nestedcte account vw01;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_nestedcte;
create database s_nestedcte from sys publication p_nestedcte;
select * from s_nestedcte.v_nestedcte;
drop database s_nestedcte;
-- @session

-- the control: a referenced CTE inside a derived table requires its relation
create view v_nestedcte2 as select * from (with used as (select id from base) select * from used) as d;
drop publication if exists p_nestedcte2_bad;
create publication p_nestedcte2_bad database view_pub_db table v_nestedcte2 account vw01;
drop publication if exists p_nestedcte2;
create publication p_nestedcte2 database view_pub_db table base,v_nestedcte2 account vw01;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_nestedcte2;
create database s_nestedcte2 from sys publication p_nestedcte2;
select * from s_nestedcte2.v_nestedcte2 order by id;
drop database s_nestedcte2;
-- @session

-- a later-declared sibling inside a derived table masks the outer CTE of the
-- same name: the binder stops the outward lookup, so the view depends on the
-- physical table even though both scopes declare a CTE with its name
create table src(id int);
insert into src values (4);
create view v_mask as with src as (select 9 as id) select * from (with a as (select id from src), src as (select 1 as id) select id from a) as d;
drop publication if exists p_mask_bad;
create publication p_mask_bad database view_pub_db table v_mask account vw01;
drop publication if exists p_mask;
create publication p_mask database view_pub_db table src,v_mask account vw01;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_mask;
create database s_mask from sys publication p_mask;
select * from s_mask.v_mask;
drop database s_mask;
-- @session

-- the binder resolves an unqualified dual to a value scan before any CTE
-- lookup, so a CTE named dual never captures the reference and the view
-- stays publishable on its own
create view v_dualcte as with dual as (select id from base) select 1 as one from dual;
drop publication if exists p_dualcte;
create publication p_dualcte database view_pub_db table v_dualcte account vw01;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_dualcte;
create database s_dualcte from sys publication p_dualcte;
select * from s_dualcte.v_dualcte;
drop database s_dualcte;
-- @session

-- the control: a qualified db.dual reference names a real table, but a
-- subscriber routes only unqualified references through the subscription, so
-- the view is rejected before catalog writes however the list is formed
create table `dual`(id int);
insert into `dual` values (3);
create view v_dualtab as select id from view_pub_db.`dual`;
drop publication if exists p_dualtab_bad;
create publication p_dualtab_bad database view_pub_db table v_dualtab account vw01;
drop publication if exists p_dualtab;
create publication p_dualtab database view_pub_db table `dual`,v_dualtab account vw01;
select pub_name, database_name, table_list from mo_catalog.mo_pubs where account_name = 'sys' and pub_name = 'p_dualtab' order by pub_name;

-- the corrected control: an unqualified reference publishes with its table
-- and answers through the subscription
create table dualctl(id int);
insert into dualctl values (3);
create view v_dualtab2 as select id from dualctl;
drop publication if exists p_dualtab2;
create publication p_dualtab2 database view_pub_db table dualctl,v_dualtab2 account vw01;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_dualtab2;
create database s_dualtab2 from sys publication p_dualtab2;
select * from s_dualtab2.v_dualtab2;
drop database s_dualtab2;
-- @session

-- duplicate requested names are accepted once the closure is complete
drop publication if exists p_dup;
create publication p_dup database view_pub_db table v_only,v_only,base account vw01;
select pub_name, database_name, table_list from mo_catalog.mo_pubs where account_name = 'sys' and database_name = 'view_pub_db' order by pub_name;
drop publication p_dup;

-- narrowing ALTER to an incomplete list is rejected and preserves the
-- publication, while the working subscription keeps answering queries
drop publication if exists p_narrow;
create publication p_narrow database view_pub_db table t1,t2,v_join,v_nested account vw01;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_narrow;
create database s_narrow from sys publication p_narrow;
select count(*) as nested_count from s_narrow.v_nested;
-- @session

alter publication p_narrow table t1,v_nested;
select pub_name, database_name, table_list from mo_catalog.mo_pubs where account_name = 'sys' and pub_name = 'p_narrow' order by pub_name;

-- @session:id=1&user=vw01:test_account&password=111
select count(*) as nested_count from s_narrow.v_nested;
drop database s_narrow;
-- @session

-- an accounts-only ALTER keeps validating the inherited table list with the
-- final account set, and widening the account set of a complete list works
create account vw02 admin_name = 'test_account' identified by '111';
alter publication p_narrow account vw01,vw02;
select pub_name, account_list from mo_catalog.mo_pubs where account_name = 'sys' and pub_name = 'p_narrow' order by pub_name;

-- @session:id=2&user=vw02:test_account&password=111
drop database if exists s_narrow2;
create database s_narrow2 from sys publication p_narrow;
select count(*) as nested_count from s_narrow2.v_nested;
drop database s_narrow2;
-- @session

-- switching the source database validates the inherited explicit list
-- against the new database
create database other_view_db;
use other_view_db;
create table base(id int primary key, v int);
insert into base values (3,30);
alter publication p_view database other_view_db;
select pub_name, database_name, table_list from mo_catalog.mo_pubs where account_name = 'sys' and pub_name = 'p_view' order by pub_name;

-- with the missing relation present in the new database the alter succeeds
create view v_only as select id,v from base where v >= 10;
alter publication p_view database other_view_db;
select pub_name, database_name, table_list from mo_catalog.mo_pubs where account_name = 'sys' and pub_name = 'p_view' order by pub_name;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_switch;
create database s_switch from sys publication p_view;
select count(*), sum(v) from s_switch.v_only;
drop database s_switch;
-- @session

-- an ACCOUNT ALL publication over a per-account system relation stays
-- subscribable by an account created after the publication
drop publication if exists p_all_good;
create publication p_all_good database view_pub_db table v_sysuser account all;
create account vw03 admin_name = 'test_account' identified by '111';

-- @session:id=3&user=vw03:test_account&password=111
drop database if exists s_all_good;
create database s_all_good from sys publication p_all_good;
select * from s_all_good.v_sysuser order by user_name;
drop database s_all_good;
-- @session

-- a general account publishes a view over a shared system relation (the
-- binder records the system account as the dependency owner) and another
-- general account queries it through the subscription

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists vw01_pub_db;
create database vw01_pub_db;
use vw01_pub_db;
create view v_stmt as select count(*) as c from system.statement_info where 1 = 0;
drop publication if exists p_g2g;
create publication p_g2g database vw01_pub_db table v_stmt account vw03;
-- @session

-- @session:id=3&user=vw03:test_account&password=111
drop database if exists s_g2g;
create database s_g2g from vw01 publication p_g2g;
select * from s_g2g.v_stmt;
drop database s_g2g;
-- @session

-- name membership alone does not prove a view publishable: the subscriber
-- re-binds the stored definition on every query, so a base table dropped and
-- recreated with an incompatible schema must fail validation with no
-- publication or subscription residue
use view_pub_db;
create table rbase(id int primary key);
create view v_rebind as select id from rbase;
drop table rbase;
create table rbase(other int);
insert into rbase values (42);
drop publication if exists p_rebind;
create publication p_rebind database view_pub_db table rbase,v_rebind account vw01;
select pub_name, database_name, table_list, account_list from mo_catalog.mo_pubs where account_name = 'sys' and pub_name = 'p_rebind' order by pub_name;

-- the corrected creation after the repair succeeds and the subscription is
-- queryable: the stored view definition matches the recreated base table
drop view v_rebind;
create view v_rebind as select other from rbase;
create publication p_rebind database view_pub_db table rbase,v_rebind account vw01;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_rebind;
create database s_rebind from sys publication p_rebind;
select other from s_rebind.v_rebind;
drop database s_rebind;
-- @session

-- a publication whose base table later loses the column the stored view
-- definition requires keeps every relation name in the list: account
-- expansions and subscription admission must still bind the definition
create table rbase2(id int primary key, pad int);
insert into rbase2 values (1, 7);
create view v_alive as select id from rbase2;
drop publication if exists p_alive;
create publication p_alive database view_pub_db table rbase2,v_alive account vw02;
alter table rbase2 drop column id;
alter publication p_alive account add vw03;
select pub_name, account_list from mo_catalog.mo_pubs where account_name = 'sys' and pub_name = 'p_alive' order by pub_name;

-- a new subscription of the unbindable view is rejected at admission
-- @session:id=2&user=vw02:test_account&password=111
drop database if exists s_alive;
create database s_alive from sys publication p_alive;
-- @session

-- after the repair the account expansion, subscription and query succeed
alter view v_alive as select pad from rbase2;
alter publication p_alive account add vw03;

-- @session:id=2&user=vw02:test_account&password=111
create database s_alive from sys publication p_alive;
select pad from s_alive.v_alive;
drop database s_alive;
-- @session

-- @session:id=3&user=vw03:test_account&password=111
drop database if exists s_alive3;
create database s_alive3 from sys publication p_alive;
select pad from s_alive3.v_alive;
drop database s_alive3;
-- @session

-- a view the publisher redefines after the subscription exists breaks only
-- its own queries: healthy tables in the same subscription keep answering,
-- and only new subscriptions are rejected at creation
use view_pub_db;
create table healthy(id int);
insert into healthy values (7);
create table other(id int);
create view v_late as select id from base;
drop publication if exists p_late;
create publication p_late database view_pub_db table healthy,base,v_late account vw01,vw02,vw03;

-- both early subscribers hold working subscriptions before the break
-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_late;
create database s_late from sys publication p_late;
select * from s_late.healthy;
select * from s_late.v_late order by id;
-- @session

-- @session:id=2&user=vw02:test_account&password=111
drop database if exists s_late2;
create database s_late2 from sys publication p_late;
select * from s_late2.healthy;
select * from s_late2.v_late order by id;
-- @session

-- the publisher redefines the view through ALTER VIEW, which keeps it in the
-- publication list while moving its dependency to the unpublished table
alter view v_late as select id from other;

-- the broken view fails only its own queries; healthy tables keep answering
-- @session:id=1&user=vw01:test_account&password=111
select * from s_late.healthy;
select * from s_late.v_late order by id;
-- @session

-- a fresh subscription is still rejected at creation
-- @session:id=3&user=vw03:test_account&password=111
drop database if exists s_late3;
create database s_late3 from sys publication p_late;
-- @session

-- switching an explicit subscription list that already names every existing
-- account to ACCOUNT ALL still extends subscription rights to accounts
-- created after the change, so the inherited-scope validation runs for this
-- transition: the broken scope is rejected and the publication metadata
-- stays unchanged
alter publication p_late account all;
select pub_name, account_list from mo_catalog.mo_pubs where account_name = 'sys' and pub_name = 'p_late' order by pub_name;

-- revoking an account must succeed even though a published view is broken:
-- an authorization reduction cannot expose the broken view to anyone new
alter publication p_late account drop vw01;
select pub_name, account_list from mo_catalog.mo_pubs where account_name = 'sys' and pub_name = 'p_late' order by pub_name;

-- @session:id=1&user=vw01:test_account&password=111
select * from s_late.healthy;
drop database s_late;
-- @session

-- remaining subscribers keep access to the healthy tables
-- @session:id=2&user=vw02:test_account&password=111
select * from s_late2.healthy;
drop database s_late2;
-- @session

-- a publication can source a database owned by one of its target accounts;
-- the subscription must admit against that physical source, which its
-- persisted database identity names, even after the publisher later owns a
-- same-named database with different objects
-- @session:id=2&user=vw02:test_account&password=111
drop database if exists fsrc2_db;
create database fsrc2_db;
create table fsrc2_db.base(id int primary key);
insert into fsrc2_db.base values (5);
use fsrc2_db;
create view v_x as select id from base;
-- @session

drop publication if exists p_fsrc;
create publication p_fsrc database fsrc2_db table base,v_x account vw02,vw03;

-- the publisher now owns a same-named database with different objects; the
-- publication still validates its persisted physical source
create database fsrc2_db;
create table fsrc2_db.decoy(id int);
-- the decoy also owns a same-named table with a decoy index and no primary
-- key: subscription statistics must read the physical source, or this decoy
-- would answer instead of the published table
create table fsrc2_db.base(id int, w int);
create index idx_decoy on fsrc2_db.base(w);

-- @session:id=3&user=vw03:test_account&password=111
drop database if exists s_fsrc;
create database s_fsrc from sys publication p_fsrc;
-- the subscription resolves its published objects in the account that
-- physically owns the database - vw02 - so the view and its base table answer
-- with the rows the physical source holds, and the publisher's same-named
-- decoy database stays irrelevant
select id from s_fsrc.v_x;
select id from s_fsrc.base;
-- statistics agree with the data path: the physical source's primary key is
-- reported and the publisher's decoy index stays invisible
select index_name, column_name, non_unique from information_schema.statistics
where table_schema = 's_fsrc' and table_name = 'base' order by index_name;
select count(*) as decoy_index_rows from information_schema.statistics
where table_schema = 's_fsrc' and table_name = 'base' and index_name = 'idx_decoy';
drop database s_fsrc;
-- @session

drop publication if exists p_fsrc;
drop database fsrc2_db;

-- @session:id=2&user=vw02:test_account&password=111
drop database fsrc2_db;
-- @session

-- a foreign source is reserved to the sys account: an ordinary account must
-- not publish another account's database to a third account, or the owner's
-- data would be exposed without the owner granting anything
-- @session:id=2&user=vw02:test_account&password=111
drop database if exists fsrc3_db;
create database fsrc3_db;
create table fsrc3_db.secure(id int primary key);
insert into fsrc3_db.secure values (7);
-- @session

-- @session:id=3&user=vw03:test_account&password=111
drop publication if exists p_hij;
-- the rejected publication names its account ids, which a fresh catalog
-- assigns deterministically; the regex keeps the check id-independent
-- @regex("internal error: account [0-9]+ cannot publish database 'fsrc3_db' owned by account [0-9]+: a foreign source may be published by the sys account, or to the owner account alone",true)
create publication p_hij database fsrc3_db table secure account vw02,vw01;
-- @session

-- the rejected creation leaves no publication row
select pub_name from mo_catalog.mo_pubs where pub_name = 'p_hij' order by pub_name;

-- and no subscriber can create a subscription to it
-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_hij;
create database s_hij from vw03 publication p_hij;
-- @session

-- the authorized control: the sys account publishes the same foreign source
create publication p_fsrc3 database fsrc3_db table secure account vw02,vw01;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_fsrc3;
create database s_fsrc3 from sys publication p_fsrc3;
select id from s_fsrc3.secure;
drop database s_fsrc3;
-- @session

drop publication p_fsrc3;

-- an owner-only foreign source grants no account visibility the owner
-- lacks: the ordinary publisher may re-publish the owner's database to the
-- owner alone, and the owner reads its own data through it
-- @session:id=3&user=vw03:test_account&password=111
drop publication if exists p_back;
create publication p_back database fsrc3_db table secure account vw02;
-- @session

-- @session:id=2&user=vw02:test_account&password=111
drop database if exists s_back;
create database s_back from vw03 publication p_back;
select id from s_back.secure;
drop database s_back;
-- @session

-- @session:id=3&user=vw03:test_account&password=111
drop publication p_back;
-- @session

-- @session:id=2&user=vw02:test_account&password=111
drop database fsrc3_db;
-- @session

-- a view whose expression calls a user-defined function cannot resolve
-- through a subscription: subscriber accounts bind function calls in their
-- own mo_catalog.mo_user_defined_function rows, where the function may be
-- missing or carry a different body, so such a view would create a
-- permanently broken or silently different-behavior subscribed object; the
-- binding probe is the decider - it binds the stored definition exactly as a
-- subscriber's first query does, with the publication database current, and
-- the binder's own selected user-defined function vetoes publication while
-- the binding marker keeps every function of the definition unexecuted
use view_pub_db;
create function f27767priv() returns int language sql as 'select 7';
create view v_udfp as select f27767priv() as n;
drop publication if exists p_udf_bad;
create publication p_udf_bad database view_pub_db table v_udfp account vw01;
-- the rejected creation leaves no publication row
select pub_name from mo_catalog.mo_pubs where pub_name = 'p_udf_bad' order by pub_name;

-- and no subscriber can create a subscription to it
-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_udf_bad;
create database s_udf_bad from sys publication p_udf_bad;
-- @session

-- the rejection holds when the subscriber installs the same function name
-- with a different body: the publication source account's own catalog row
-- is decisive, because the subscriber would silently execute its own body
-- instead of the published one
-- @session:id=1&user=vw01:test_account&password=111
drop database if exists sub_udf_db;
create database sub_udf_db;
use sub_udf_db;
create function f27767priv() returns int language sql as 'select 70';
select f27767priv();
-- @session

create publication p_udf_bad database view_pub_db table v_udfp account vw01;
select pub_name from mo_catalog.mo_pubs where pub_name = 'p_udf_bad' order by pub_name;

-- the builtin control is the complement: a builtin function binds
-- identically in every account, so its view publishes and the subscribed
-- result equals the publisher's own
create view v_absbi as select abs(-7) as y;
drop publication if exists p_absbi;
create publication p_absbi database view_pub_db table v_absbi account vw01;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_absbi;
create database s_absbi from sys publication p_absbi;
select y from s_absbi.v_absbi;
drop database s_absbi;
-- @session

drop publication p_absbi;
drop view v_absbi;
drop view v_udfp;
drop function f27767priv();

-- the subscriber's shadow-function database is removed by its owner
-- @session:id=1&user=vw01:test_account&password=111
drop database sub_udf_db;
-- @session

-- an in-place ALTER TABLE RENAME COLUMN keeps the relation id and bumps its
-- schema version: the binding probe proves a view still selecting the old
-- column no longer binds the renamed schema, so the complete explicit scope
-- is rejected before any catalog write
create table t_rename(a int primary key, b int);
insert into t_rename values (1,2);
create view v_ren as select a from t_rename;
alter table t_rename rename column a to c;
drop publication if exists p_ren_bad;
create publication p_ren_bad database view_pub_db table t_rename,v_ren account vw01;
-- the rejected creation leaves no publication row
select pub_name from mo_catalog.mo_pubs where pub_name = 'p_ren_bad' order by pub_name;

-- and no subscriber can create a subscription to it
-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_ren_bad;
create database s_ren_bad from sys publication p_ren_bad;
-- @session

-- the publisher repairs the view against the renamed column and republishes:
-- the same complete explicit scope now subscribes and answers with the
-- renamed schema
alter view v_ren as select c from t_rename;
drop publication if exists p_ren_fix;
create publication p_ren_fix database view_pub_db table t_rename,v_ren account vw01;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_ren;
create database s_ren from sys publication p_ren_fix;
select * from s_ren.v_ren;
select c, b from s_ren.t_rename;
drop database s_ren;
-- @session

drop publication p_ren_fix;
drop view v_ren;
drop table t_rename;

-- a same-name different-signature user-defined function never claims a
-- builtin call: function binding selects by name, arity and argument types,
-- so abs(-7) still binds the builtin abs regardless of the catalog row, the
-- view publishes, and the subscription returns the builtin's result
drop function if exists abs(int,int);
create function abs(x int, y int) returns int language sql as 'select 70';
create view v_abs2 as select abs(-7) as y;
drop publication if exists p_abs2;
create publication p_abs2 database view_pub_db table v_abs2 account vw01;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_abs2;
create database s_abs2 from sys publication p_abs2;
select y from s_abs2.v_abs2;
drop database s_abs2;
-- @session

drop publication p_abs2;
drop view v_abs2;
drop function abs(int,int);

-- validation executes nothing of the views it binds: the metadata binding of
-- enable_fault_injection() selects the builtin without running it, in the
-- projection or as a WHERE predicate, so the runtime fault-injection switch
-- stays off through a successful validation, through a failed validation and
-- its rollback, and through the subscriber admission; the views themselves are
-- never executed anywhere in this suite. The side-effect views sort before
-- the rename-broken companion (resolvePubTableNames sorts the members), so
-- the binding probes run in member order and both side-effect views bind -
-- without executing - before the broken companion fails the scope: the
-- rollback message is the real binding failure of v_zbrk.
create table ftab(id int primary key);
insert into ftab values (1);
create view v_fi as select enable_fault_injection() as r, id as c from ftab where id >= 1;
create table ftab2(id int primary key);
insert into ftab2 values (2);
create view v_fiw as select id as i from ftab2 where enable_fault_injection();
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;
drop publication if exists p_fi_ok;
create publication p_fi_ok database view_pub_db table ftab,v_fi account vw01;
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;
drop publication if exists p_fi_w;
create publication p_fi_w database view_pub_db table ftab2,v_fiw account vw01;
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;
-- a scope failing after the side-effect views were bound on the same list:
-- the broken companion view no longer binds the renamed column
create table bkt(a int primary key);
create view v_zbrk as select a from bkt;
alter table bkt rename column a to z;
drop publication if exists p_fi_bad;
create publication p_fi_bad database view_pub_db table ftab2,ftab,v_fi,v_fiw,bkt,v_zbrk account vw01;
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;
-- the failed validation leaves no publication row
select pub_name from mo_catalog.mo_pubs where pub_name = 'p_fi_bad' order by pub_name;

-- an eager LIMIT of a stored definition binds at validation without being
-- evaluated, while the subscriber's ordinary query folds the clause and
-- returns exactly the folded row count: four distinguishable rows with the
-- order inside the view make the expected two rows sensitive to a lost or
-- ignored limit
create table etab(id int primary key);
insert into etab values (1),(2),(3),(4);
create view v_eager as select id as i from etab order by id limit 1 + 1;
drop publication if exists p_eager_lim;
create publication p_eager_lim database view_pub_db table etab,v_eager account vw01;
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_fi_ok;
create database s_fi_ok from sys publication p_fi_ok;
drop database s_fi_ok;
drop database if exists s_eager;
create database s_eager from sys publication p_eager_lim;
select i from s_eager.v_eager order by i;
select count(*) from s_eager.v_eager;
drop database s_eager;
-- @session

drop publication p_fi_ok;
drop publication p_fi_w;
drop publication p_eager_lim;
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;
drop view v_zbrk;
drop table bkt;
drop view v_eager;
drop table etab;
drop view v_fiw;
drop table ftab2;
drop view v_fi;
drop table ftab;

-- comparison folding of a constant operand used to run the two executor-backed
-- comparison folders before the marker gate, so a cast-wrapped side-effect
-- builtin executed while the validator bound the stored definition: under the
-- marker both folders are skipped without changing overload selection, so
-- neither the decimal nor the float side-effect comparison executes through
-- successful validation, rejected validation, or subscriber admission, while
-- the constant-cast control stays queryable through its subscription
use view_pub_db;
create table ctab(id int primary key, d decimal(10,2), f double);
insert into ctab values (1, 1.50, 2.5);
create view v_ccast as select id as i from ctab where d = cast('1.50' as char);
create view v_fdc as select id as i from ctab where d = cast(enable_fault_injection() as char) and id >= 1;
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;
create view v_ffl as select id as i from ctab where f > cast(enable_fault_injection() as double);
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;
drop publication if exists p_cast_ok;
create publication p_cast_ok database view_pub_db table ctab,v_ccast,v_fdc,v_ffl account vw01;
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;
create table bktc(a int primary key);
create view v_zbrkc as select a from bktc;
alter table bktc rename column a to z;
drop publication if exists p_cast_bad;
-- both side-effect views sort before the broken companion, so they are bound
-- without executing even while this scope rolls back on the companion's
-- failure
create publication p_cast_bad database view_pub_db table bktc,ctab,v_ccast,v_fdc,v_ffl,v_zbrkc account vw01;
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;
-- the rejected scope leaves no publication row
select pub_name from mo_catalog.mo_pubs where pub_name = 'p_cast_bad' order by pub_name;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_cast;
create database s_cast from sys publication p_cast_ok;
select i from s_cast.v_ccast order by i;
drop database s_cast;
-- @session

-- subscriber admission and the constant-cast query leave the global switch
-- untouched
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;
drop publication p_cast_ok;
drop view v_zbrkc;
drop table bktc;
drop view v_ccast;
drop view v_fdc;
drop view v_ffl;
drop table ctab;
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;

-- a relation-resolving table function binds its source through the executing
-- account, so a view over table_changes routes that resolution through the
-- subscription where the function's own binder rejects it: even complete
-- membership is rejected before any catalog write, while a literal-driven
-- table function publishes and answers through its subscription
-- @session:id=1&user=vw01:test_account&password=111
drop database if exists tcdb_27767;
create database tcdb_27767;
use tcdb_27767;
create table base_tc(id int primary key);
insert into base_tc values (1),(2);
create view v_tc as select * from table_changes('tcdb_27767','base_tc','','1-0') c;
create view v_gs as select * from generate_series(1, 3) g;
create publication p_tc_ok database tcdb_27767 table v_gs account vw02;
-- the complete membership still cannot make the table_changes view executable
create publication p_tc_bad database tcdb_27767 table base_tc,v_tc account vw02;
-- metadata_scan and table_stats expose static schemas and capture no source
-- dependency, but resolve their argument relation at execution time through
-- the executing account: complete membership is rejected here too, or the
-- subscription would fail its first query with the publisher's database
-- missing in the subscriber's account
create view v_ms as select * from metadata_scan('tcdb_27767.base_tc','id') m;
create view v_ts as select * from table_stats('tcdb_27767.base_tc','get') t;
create publication p_ms_bad database tcdb_27767 table base_tc,v_ms,v_ts account vw02;
-- @session

-- the rejected scopes leave no publication rows while the control publishes
select pub_name from mo_catalog.mo_pubs where pub_name in ('p_tc_ok','p_tc_bad','p_ms_bad') order by pub_name;

-- @session:id=2&user=vw02:test_account&password=111
drop database if exists s_tc_ok;
create database s_tc_ok from vw01 publication p_tc_ok;
select * from s_tc_ok.v_gs order by 1;
drop database s_tc_ok;
drop database if exists s_tc_bad;
create database s_tc_bad from vw01 publication p_tc_bad;
drop database if exists s_ms_bad;
create database s_ms_bad from vw01 publication p_ms_bad;
-- @session

-- @session:id=1&user=vw01:test_account&password=111
drop publication p_tc_ok;
drop view v_tc;
drop view v_gs;
drop view v_ms;
drop view v_ts;
drop table base_tc;
drop database tcdb_27767;
-- @session

-- string-length source inference evaluates the length argument of the stored
-- definition while binding: under the marker the inference is skipped
-- without changing any bound type, so a cast-wrapped side-effect length never
-- executes through successful validation, rejected validation, or subscriber
-- admission, while the computed literal length still answers through the
-- subscription
use view_pub_db;
create table ltab(id int primary key, n varchar(20));
insert into ltab values (1, 'abcdefgh');
create view v_lens as select left(n, 1 + 1) as s2 from ltab;
create view v_lenl as select left('abc', cast(enable_fault_injection() as signed)) as s;
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;
create table bktl(a int primary key);
create view v_zbrkl as select a from bktl;
alter table bktl rename column a to z;
drop publication if exists p_len_ok;
create publication p_len_ok database view_pub_db table ltab,v_lens,v_lenl account vw01;
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;
drop publication if exists p_len_bad;
-- the side-effect length view sorts before the broken companion, so it is
-- bound without executing even while this scope rolls back on the companion's
-- failure
create publication p_len_bad database view_pub_db table bktl,ltab,v_lenl,v_lens,v_zbrkl account vw01;
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;
-- the rejected scope leaves no publication row
select pub_name from mo_catalog.mo_pubs where pub_name = 'p_len_bad' order by pub_name;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_len;
create database s_len from sys publication p_len_ok;
select s2 from s_len.v_lens;
drop database s_len;
-- @session

-- subscriber admission and the computed-length control leaves the switch
-- untouched
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;
drop publication p_len_ok;
drop view v_zbrkl;
drop table bktl;
drop view v_lenl;
drop view v_lens;
drop table ltab;
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;

-- quoted identifiers keep their exact bytes through publication persistence:
-- the persisted list is split without trimming, so a table or view whose name
-- carries leading whitespace publishes, admits subscriptions, and answers
-- through the subscription, and a widening ALTER inherits the exact list
-- unchanged
-- @session:id=1&user=vw01:test_account&password=111
drop database if exists qdb_27767;
create database qdb_27767;
use qdb_27767;
create table ` t`(id int primary key);
insert into ` t` values (1),(2);
create view ` v` as select id from ` t`;
-- the view member and the table member both keep their leading space
create publication p_q_ok database qdb_27767 table ` t`,` v` account vw02;
-- a table-only scope keeps its bytes the same way
create publication p_qt_ok database qdb_27767 table ` t` account vw02;
-- the inherited list of an account-only change preserves the exact names
alter publication p_q_ok account vw02,vw03;
-- @session

select pub_name, account_list from mo_catalog.mo_pubs where pub_name in ('p_q_ok','p_qt_ok') order by pub_name;

-- @session:id=2&user=vw02:test_account&password=111
drop database if exists s_q_ok;
drop database if exists s_qt_ok;
create database s_q_ok from vw01 publication p_q_ok;
select id from s_q_ok.` t` order by id;
select id from s_q_ok.` v` order by id;
create database s_qt_ok from vw01 publication p_qt_ok;
select id from s_qt_ok.` t` order by id;
drop database s_q_ok;
drop database s_qt_ok;
-- @session

-- @session:id=1&user=vw01:test_account&password=111
drop publication p_q_ok;
drop publication p_qt_ok;
drop view ` v`;
drop table ` t`;
drop database qdb_27767;
-- @session

-- a correlated CTE projection reaches the binder's aggregate empty-input
-- analysis, which evaluates the substituted constant expression of the stored
-- definition: under the publication binding marker the analysis refuses to
-- prove, so the correlated shape binds as pure metadata - no execution through
-- successful validation, rejected validation, or subscriber admission -
-- while the arithmetic control still answers through the subscription
create table base_cor(id int primary key, v int);
insert into base_cor values (1,10),(2,20);
create view v_corc as select o.id, (with x as (select count(*) as c from base_cor i where i.id = o.id) select c + 1 - 1 from x) as n2 from base_cor o;
create view v_corl as select o.id, (with x as (select count(*) as c from base_cor i where i.id = o.id) select c + cast(enable_fault_injection() as signed) from x) as n from base_cor o;
select disable_fault_injection();
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;
create table bkt_cor(a int primary key);
create view v_zcor as select a from bkt_cor;
alter table bkt_cor rename column a to z;
drop publication if exists p_cor_ok;
create publication p_cor_ok database view_pub_db table base_cor,v_corc,v_corl account vw01;
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;
drop publication if exists p_cor_bad;
-- the correlated side-effect view sorts before the broken companion, so it is
-- bound without executing even while this scope rolls back on the companion's
-- failure
create publication p_cor_bad database view_pub_db table bkt_cor,base_cor,v_corc,v_corl,v_zcor account vw01;
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;
-- the rejected scope leaves no publication row
select pub_name from mo_catalog.mo_pubs where pub_name = 'p_cor_bad' order by pub_name;

-- @session:id=1&user=vw01:test_account&password=111
drop database if exists s_cor;
create database s_cor from sys publication p_cor_ok;
select id, n2 from s_cor.v_corc order by id;
drop database s_cor;
-- @session

-- subscriber admission and the arithmetic control leave the switch untouched
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;
drop publication p_cor_ok;
drop view v_zcor;
drop table bkt_cor;
drop view v_corl;
drop view v_corc;
drop table base_cor;
select fault_inject('all.','status_fault_point','') like '%Fault injection is disabled%' as disabled;

-- the scalar relation-resolving builtins freeze their source as string
-- literals and resolve it through the executing account at execution: even
-- complete membership cannot repair them, and a subscriber that owns a
-- same-named database would silently read its own catalog instead of the
-- published relation, so the scope is rejected before any catalog write while
-- ordinary table publications and their subscriptions keep working;
-- internal_auto_increment makes the wrong source observable as a value: the
-- frozen literal would answer the subscriber's own auto-increment counter,
-- not the publisher's
-- @session:id=1&user=vw01:test_account&password=111
drop database if exists sdb_27767;
create database sdb_27767;
use sdb_27767;
create table base_ss(id int primary key);
insert into base_ss values (1),(2);
create view v_smax as select mo_table_col_max('sdb_27767','base_ss','id') as m;
create view v_srows as select mo_table_rows('sdb_27767','base_ss') as r;
create table ainc(id bigint auto_increment primary key, v int);
insert into ainc values (1,10),(2,20);
create view v_ai as select internal_auto_increment('sdb_27767','ainc') as n from ainc;
-- @session

-- the subscriber owns a same-named counterpart database with different rows
-- @session:id=2&user=vw02:test_account&password=111
drop database if exists sdb_27767;
drop database if exists s_ss_ok;
drop database if exists s_ss_bad;
drop database if exists s_ai_bad;
create database sdb_27767;
use sdb_27767;
create table base_ss(id int primary key);
insert into base_ss values (5),(7);
create table ainc(id bigint auto_increment primary key, v int);
insert into ainc values (90,7),(95,8);
-- @session

-- @session:id=1&user=vw01:test_account&password=111
-- complete membership still cannot make the scalar-source views executable
create publication p_ss_bad database sdb_27767 table base_ss,v_smax,v_srows account vw02;
create publication p_ss_ok database sdb_27767 table base_ss account vw02;
-- internal_auto_increment captures no relation dependency from its literals
create publication p_ai_bad database sdb_27767 table ainc,v_ai account vw02;
-- @session

-- the rejected scope leaves no publication row while the plain control
-- publishes
select pub_name from mo_catalog.mo_pubs where pub_name in ('p_ss_ok','p_ss_bad','p_ai_bad') order by pub_name;

-- @session:id=2&user=vw02:test_account&password=111
create database s_ss_ok from vw01 publication p_ss_ok;
select count(*) from s_ss_ok.base_ss;
drop database s_ss_ok;
create database s_ss_bad from vw01 publication p_ss_bad;
create database s_ai_bad from vw01 publication p_ai_bad;
-- @session

-- the rejected ALTER preserves the prior publication, its table list and the
-- working subscription
-- @session:id=1&user=vw01:test_account&password=111
alter publication p_ss_ok account vw02 table base_ss,v_smax;
-- @session

select pub_name, table_list from mo_catalog.mo_pubs where pub_name in ('p_ss_ok','p_ss_bad') order by pub_name;

-- @session:id=2&user=vw02:test_account&password=111
create database s_ss_ok from vw01 publication p_ss_ok;
select count(*) from s_ss_ok.base_ss;
select m from s_ss_ok.v_smax;
select count(*) from sdb_27767.base_ss;
drop database s_ss_ok;
-- @session

-- each side's own call answers its own counter, and the subscriber's
-- same-named database would have silently absorbed the frozen literal
-- pre-fix
-- @session:id=1&user=vw01:test_account&password=111
select internal_auto_increment('sdb_27767','ainc') as pub_direct;
-- @session

-- @session:id=2&user=vw02:test_account&password=111
select internal_auto_increment('sdb_27767','ainc') as sub_own;
select count(*) from sdb_27767.ainc;
-- @session

-- the rejected ALTER cannot introduce the auto-increment view either: the
-- prior list stays and the prior subscription keeps answering
-- @session:id=1&user=vw01:test_account&password=111
alter publication p_ss_ok account vw02 table base_ss,ainc,v_ai;
-- @session

select pub_name, table_list from mo_catalog.mo_pubs where pub_name in ('p_ss_ok','p_ai_bad') order by pub_name;

-- @session:id=2&user=vw02:test_account&password=111
drop database if exists s_ss_ok;
create database s_ss_ok from vw01 publication p_ss_ok;
select count(*) from s_ss_ok.base_ss;
drop database s_ss_ok;
-- @session

-- @session:id=1&user=vw01:test_account&password=111
drop publication p_ss_ok;
drop view v_smax;
drop view v_srows;
drop view v_ai;
drop table base_ss;
drop table ainc;
drop database sdb_27767;
-- @session

-- @session:id=2&user=vw02:test_account&password=111
drop database sdb_27767;
-- @session

-- mo_ctl's table-extra command freezes its source as a 'database.table'
-- string literal and resolves it through the executing account's engine only
-- at execution (pkg/sql/plan/function/ctl/cmd_table_extra.go): its access
-- gate (pkg/sql/plan/function/ctl/ctl.go) only stops non-sys accounts, while
-- the sys account with the moadmin role may itself subscribe to another
-- account's publication, so a published constant-projection view would
-- execute the frozen literal for the sys subscriber - silently reading the
-- subscriber's own same-named relation when one exists, or failing a
-- database lookup that the executing account cannot satisfy; the scope is
-- rejected before any catalog write while a plain table publication to the
-- same sys subscriber keeps working
-- @session:id=1&user=vw01:test_account&password=111
drop database if exists mdb_27767;
create database mdb_27767;
use mdb_27767;
create table base_t(id int primary key, v int);
insert into base_t values (1,1),(2,2);
create view v_ctl_t as select mo_ctl('cn','table-extra','mdb_27767.base_t') as r;
-- @session

-- the sys subscriber owns a same-named counterpart database whose
-- table-extra answer differs from the publisher's relation, making the
-- pre-fix wrong-source read observable as a different json answer
drop database if exists mdb_27767;
drop database if exists s_ctl_ok;
drop database if exists s_ctl_bad;
create database mdb_27767;
create table mdb_27767.base_t(id bigint auto_increment primary key, v int) auto_id_cache = 3200;
insert into mdb_27767.base_t values (90,7),(95,8);
select mo_ctl('cn','table-extra','mdb_27767.base_t') as sys_direct;
-- @session

-- the frozen literal sees complete membership yet cannot name the
-- publisher's relation from the sys subscriber's execution context
-- @session:id=1&user=vw01:test_account&password=111
create publication p_ctl_bad database mdb_27767 table base_t,v_ctl_t account sys;
create publication p_ctl_ok database mdb_27767 table base_t account sys;
-- @session

-- the rejected scope leaves no publication row while the plain control
-- publishes to the same sys subscriber
select pub_name from mo_catalog.mo_pubs where pub_name in ('p_ctl_ok','p_ctl_bad') order by pub_name;

create database s_ctl_ok from vw01 publication p_ctl_ok;
select count(*) from s_ctl_ok.base_t;
create database s_ctl_bad from vw01 publication p_ctl_bad;

-- the rejected ALTER cannot introduce the table-extra view either: the
-- prior list stays and the prior subscription keeps answering
-- @session:id=1&user=vw01:test_account&password=111
alter publication p_ctl_ok account sys table base_t,v_ctl_t;
-- @session

select pub_name, table_list from mo_catalog.mo_pubs where pub_name in ('p_ctl_ok','p_ctl_bad') order by pub_name;

drop database if exists s_ctl_ok;
create database s_ctl_ok from vw01 publication p_ctl_ok;
select count(*) from s_ctl_ok.base_t;
select r from s_ctl_ok.v_ctl_t;
drop database s_ctl_ok;

-- @session:id=1&user=vw01:test_account&password=111
drop publication p_ctl_ok;
drop view v_ctl_t;
drop table base_t;
drop database mdb_27767;
-- @session

drop database mdb_27767;

-- geometry constructors record the SRID argument inside the geometry type,
-- and the binding decode requires a constant integer: an ordinary compile
-- folds the constant expression 4325 + 1 into the literal 4326 during the
-- nested binding of that argument, so a view with a constant-expression
-- SRID is valid and creatable by its owner. The publication probe binds the
-- persisted definition with its fold stages gated, so before this round
-- the same valid definition reached the decode still un-folded and was
-- rejected as broken; the probe now folds the SRID argument through the
-- same executor-backed constant fold ordinary binding uses, guarded to
-- literals of that switch's own operator set so nothing else of a stored
-- definition executes during validation. The arithmetic twin publishes,
-- subscribes and answers exactly like the literal 4326 twin
-- @session:id=1&user=vw01:test_account&password=111
drop database if exists gdb_27767;
create database gdb_27767;
use gdb_27767;
create table base_t(id int primary key, v int);
insert into base_t values (1,1),(2,2);
create view v_srid_a as select id, st_geomfromtext('POINT(1 2)', 4325 + 1) as g from base_t;
create view v_srid_l as select id, st_geomfromtext('POINT(1 2)', 4326) as g from base_t;
create view v_srid_s as select id, st_srid(st_geomfromtext('POINT(1 2)', 0), 4325 + 1) as g from base_t;
-- @session

-- the owner's ordinary compile folds the arithmetic SRID into the literal's
-- own value: every control answers the same result on this binary
-- @session:id=1&user=vw01:test_account&password=111
select id, st_srid(g) from gdb_27767.v_srid_a order by id;
select id, st_srid(g) from gdb_27767.v_srid_l order by id;
select id, st_srid(g) from gdb_27767.v_srid_s order by id;
select id, st_astext(g) from gdb_27767.v_srid_a order by id;
-- @session

-- the arithmetic SRID publishes like the literal twin
-- @session:id=1&user=vw01:test_account&password=111
create publication p_srid_a database gdb_27767 table base_t,v_srid_a account vw02;
create publication p_srid_all database gdb_27767 table base_t,v_srid_l,v_srid_s,v_srid_a account vw02;
-- @session

-- residue: both publications exist with their complete members
select pub_name, table_list from mo_catalog.mo_pubs where pub_name like 'p_srid_%' order by pub_name;

-- the subscriber answers the same SRID and WKT through the arithmetic
-- twin and the literal twin alike
-- @session:id=2&user=vw02:test_account&password=111
drop database if exists s_srid_a;
drop database if exists s_srid_all;
create database s_srid_a from vw01 publication p_srid_a;
select id, st_srid(g) from s_srid_a.v_srid_a order by id;
create database s_srid_all from vw01 publication p_srid_all;
select id, st_srid(g) from s_srid_all.v_srid_a order by id;
select id, st_srid(g) from s_srid_all.v_srid_l order by id;
select id, st_srid(g) from s_srid_all.v_srid_s order by id;
select id, st_astext(g) from s_srid_all.v_srid_a order by id;
-- @session

-- the widening ALTER admits the arithmetic member the way ordinary binding
-- does, and the retained publication keeps answering after re-subscribing
-- @session:id=1&user=vw01:test_account&password=111
alter publication p_srid_a account vw02 table base_t,v_srid_a,v_srid_l;
-- @session

select pub_name, table_list from mo_catalog.mo_pubs where pub_name like 'p_srid_%' order by pub_name;

-- @session:id=2&user=vw02:test_account&password=111
drop database s_srid_a;
create database s_srid_a from vw01 publication p_srid_a;
select id, st_srid(g) from s_srid_a.v_srid_a order by id;
select id, st_srid(g) from s_srid_a.v_srid_l order by id;
drop database s_srid_a;
drop database s_srid_all;
-- @session

-- @session:id=1&user=vw01:test_account&password=111
drop publication p_srid_a;
drop publication p_srid_all;
drop view v_srid_a;
drop view v_srid_l;
drop view v_srid_s;
drop table base_t;
drop database gdb_27767;
-- @session

-- the implicit-cast SRID arithmetic: a string operand inside the constant
-- expression ('4325' + 1) makes the resolver wrap the operands in
-- conversion nodes whose trailing argument is target-type metadata; an
-- ordinary compile folds the whole tree into the same literal 4326, so the
-- probe has to meet that literal as well or every complete-scope
-- publication of the view is rejected as broken. The explicit CAST target
-- keeps the ordinary compile's own constant-integer rejection: such a
-- definition cannot even be created, on either binary, so only the
-- arithmetic-with-implicit-cast shape reaches the publication membership
-- here
-- @session:id=1&user=vw01:test_account&password=111
drop database if exists gdb_27767;
create database gdb_27767;
use gdb_27767;
create table base_t(id int primary key, v int);
insert into base_t values (1,1),(2,2);
create view v_cast_c as select id, st_geomfromtext('POINT(1 2)', '4325' + 1) as g from base_t;
create view v_cast_cl as select id, st_geomfromtext('POINT(1 2)', 4326) as g from base_t;
create view v_cast_cs as select id, st_srid(st_geomfromtext('POINT(1 2)', 0), '4325' + 1) as g from base_t;
create view v_cast_x as select id, st_geomfromtext('POINT(1 2)', cast('4326' as signed)) as g from base_t;
-- @session

-- the owner's ordinary compile folds the implicit-cast arithmetic into the
-- literal twin's own value: every control answers the same result
-- @session:id=1&user=vw01:test_account&password=111
select id, st_srid(g) from gdb_27767.v_cast_c order by id;
select id, st_srid(g) from gdb_27767.v_cast_cl order by id;
select id, st_srid(g) from gdb_27767.v_cast_cs order by id;
select id, st_astext(g) from gdb_27767.v_cast_c order by id;
-- @session

-- the implicit-cast arithmetic publishes like the literal twin
-- @session:id=1&user=vw01:test_account&password=111
create publication p_cast_c database gdb_27767 table base_t,v_cast_c,v_cast_cs account vw02;
create publication p_cast_cl database gdb_27767 table base_t,v_cast_cl account vw02;
-- @session

-- residue: both publications exist with their complete members
select pub_name, table_list from mo_catalog.mo_pubs where pub_name like 'p_cast_%' order by pub_name;

-- the subscriber answers the same SRID and WKT through the implicit-cast
-- views and the literal twin alike
-- @session:id=2&user=vw02:test_account&password=111
drop database if exists s_cast_c;
drop database if exists s_cast_cl;
create database s_cast_c from vw01 publication p_cast_c;
select id, st_srid(g) from s_cast_c.v_cast_c order by id;
select id, st_srid(g) from s_cast_c.v_cast_cs order by id;
select id, st_astext(g) from s_cast_c.v_cast_c order by id;
create database s_cast_cl from vw01 publication p_cast_cl;
select id, st_srid(g) from s_cast_cl.v_cast_cl order by id;
-- @session

-- the widening ALTER admits the literal twin to the implicit-cast
-- publication, and the re-subscription answers both members
-- @session:id=1&user=vw01:test_account&password=111
alter publication p_cast_c account vw02 table base_t,v_cast_c,v_cast_cs,v_cast_cl;
-- @session

select pub_name, table_list from mo_catalog.mo_pubs where pub_name like 'p_cast_%' order by pub_name;

-- @session:id=2&user=vw02:test_account&password=111
drop database s_cast_c;
create database s_cast_c from vw01 publication p_cast_c;
select id, st_srid(g) from s_cast_c.v_cast_c order by id;
select id, st_srid(g) from s_cast_c.v_cast_cl order by id;
drop database s_cast_c;
drop database s_cast_cl;
-- @session

-- @session:id=1&user=vw01:test_account&password=111
drop publication p_cast_c;
drop publication p_cast_cl;
drop view v_cast_c;
drop view v_cast_cl;
drop view v_cast_cs;
drop table base_t;
drop database gdb_27767;
-- @session

-- the nested pure-function SRID arithmetic: a deterministic builtin inside
-- the constant expression (abs(4325) + 1) folds during any ordinary compile
-- - the '+' root reaches the post-bind fold switch and its recursion folds
-- the resolved ABS overload into its literal - so a view with this shape is
-- valid, creatable and subscribable, and the publication probe has to meet
-- the same literal or every complete-scope publication of the view is
-- rejected as broken. The abs ROOT and the volatile subtree keep the
-- ordinary compile's own constant-integer rejection: those definitions
-- cannot even be created, on either binary
-- @session:id=1&user=vw01:test_account&password=111
drop database if exists gdb_27767;
create database gdb_27767;
use gdb_27767;
create table base_t(id int primary key, v int);
insert into base_t values (1,1),(2,2);
create view v_nabs_c as select id, st_geomfromtext('POINT(1 2)', abs(4325) + 1) as g from base_t;
create view v_nabs_cl as select id, st_geomfromtext('POINT(1 2)', 4326) as g from base_t;
create view v_nabs_cs as select id, st_srid(st_geomfromtext('POINT(1 2)', 0), abs(4325) + 1) as g from base_t;
create view v_nabs_x as select id, st_geomfromtext('POINT(1 2)', abs(4326)) as g from base_t;
create view v_nabs_r as select id, st_geomfromtext('POINT(1 2)', 1 + rand() * 0) as g from base_t;
-- @session

-- the owner's ordinary compile folds the nested pure-function arithmetic
-- into the literal twin's own value: every control answers the same result
-- @session:id=1&user=vw01:test_account&password=111
select id, st_srid(g) from gdb_27767.v_nabs_c order by id;
select id, st_srid(g) from gdb_27767.v_nabs_cl order by id;
select id, st_srid(g) from gdb_27767.v_nabs_cs order by id;
select id, st_astext(g) from gdb_27767.v_nabs_c order by id;
-- @session

-- the nested pure-function arithmetic publishes like the literal twin
-- @session:id=1&user=vw01:test_account&password=111
create publication p_nabs_c database gdb_27767 table base_t,v_nabs_c,v_nabs_cs account vw02;
create publication p_nabs_cl database gdb_27767 table base_t,v_nabs_cl account vw02;
-- @session

select pub_name, table_list from mo_catalog.mo_pubs where pub_name like 'p_nabs%' order by pub_name;

-- the subscriber answers the same SRID and WKT through the nested-abs
-- views and the literal twin alike
-- @session:id=2&user=vw02:test_account&password=111
drop database if exists s_nabs_c;
drop database if exists s_nabs_cl;
create database s_nabs_c from vw01 publication p_nabs_c;
select id, st_srid(g) from s_nabs_c.v_nabs_c order by id;
select id, st_srid(g) from s_nabs_c.v_nabs_cs order by id;
select id, st_astext(g) from s_nabs_c.v_nabs_c order by id;
create database s_nabs_cl from vw01 publication p_nabs_cl;
select id, st_srid(g) from s_nabs_cl.v_nabs_cl order by id;
-- @session

-- the widening ALTER admits the literal twin to the nested-abs
-- publication, and the re-subscription answers both members
-- @session:id=1&user=vw01:test_account&password=111
alter publication p_nabs_c account vw02 table base_t,v_nabs_c,v_nabs_cs,v_nabs_cl;
-- @session

select pub_name, table_list from mo_catalog.mo_pubs where pub_name like 'p_nabs%' order by pub_name;

-- @session:id=2&user=vw02:test_account&password=111
drop database s_nabs_c;
create database s_nabs_c from vw01 publication p_nabs_c;
select id, st_srid(g) from s_nabs_c.v_nabs_c order by id;
select id, st_srid(g) from s_nabs_c.v_nabs_cl order by id;
drop database s_nabs_c;
drop database s_nabs_cl;
-- @session

-- @session:id=1&user=vw01:test_account&password=111
drop publication p_nabs_c;
drop publication p_nabs_cl;
drop view v_nabs_c;
drop view v_nabs_cl;
drop view v_nabs_cs;
drop table base_t;
drop database gdb_27767;
-- @session

drop account if exists vw04;
drop account if exists vw05;
create account vw04 admin_name = 'test_account' identified by '111';
create account vw05 admin_name = 'test_account' identified by '111';

-- the lower_case_table_names context of a table-level publication: a session
-- that switches its account's global read resolves identifier case at the
-- switched mode on its NEXT fresh connection, and the validating probe must
-- inherit exactly that context instead of assuming mode 1 - a mode-0
-- publisher session with case-distinct tables Base/base and a view over
-- Base publishes its matching-case scope, the incomplete wrong-case scope
-- keeps its must-include rejection from the definition's own recorded
-- mode, and a mode-0 subscriber session resolves the published
-- source-case relation through the subscription. Each session id below is
-- used exactly once so every block runs on its own fresh connection
-- @session:id=30&user=vw04:test_account&password=111
set global lower_case_table_names = 0;
-- @session

-- @session:id=32&user=vw04:test_account&password=111
select @@lower_case_table_names;
drop database if exists cdb21b;
create database cdb21b;
use cdb21b;
create table Base(id int primary key, x int);
insert into Base values (1,10),(2,20);
create table base(id int primary key, x int);
insert into base values (9,90);
create view v_Base as select id, x from Base;
select id, x from cdb21b.Base order by id;
select id, x from cdb21b.base order by id;
select id, x from cdb21b.v_Base order by id;
create publication p_case_ok database cdb21b table Base,v_Base account vw05;
create publication p_case_wrong database cdb21b table base,v_Base account vw05;
-- @session

select pub_name, table_list from mo_catalog.mo_pubs where pub_name like 'p_case%' order by pub_name;

-- @session:id=33&user=vw05:test_account&password=111
set global lower_case_table_names = 0;
-- @session

-- the mode-0 subscriber resolves the published source-case relation through
-- the subscription
-- @session:id=34&user=vw05:test_account&password=111
select @@lower_case_table_names;
drop database if exists s_case_ok;
create database s_case_ok from vw04 publication p_case_ok;
select id, x from s_case_ok.v_Base order by id;
drop database if exists s_case_ok;
-- @session

-- restore both accounts' default case mode, then clean up on fresh
-- connections that resolve at the restored default
-- @session:id=35&user=vw04:test_account&password=111
set global lower_case_table_names = 1;
-- @session

-- @session:id=36&user=vw05:test_account&password=111
set global lower_case_table_names = 1;
-- @session

-- @session:id=37&user=vw04:test_account&password=111
select @@lower_case_table_names;
drop publication p_case_ok;
drop publication p_case_wrong;
drop database cdb21b;
-- @session

-- a session temporary table shadows the persistent schema of an ordinary
-- compile, so validation binds persisted objects only: a healthy view over
-- a healthy persistent table stays publishable while a same-name temporary
-- shadows the base table, the subscription answers with the persisted
-- schema, and a broken persistent schema under a same-name compatible
-- temporary is still rejected
-- @session:id=1&user=vw01:test_account&password=111
drop database if exists tp_shadow;
create database tp_shadow;
use tp_shadow;
create table base_sh(id int primary key, v int);
insert into base_sh values (1,10),(2,20);
create view v_sh as select id from base_sh;
create temporary table base_sh(other int);
create publication p_sh_ok database tp_shadow table base_sh,v_sh account vw02;
-- @session

-- @session:id=2&user=vw02:test_account&password=111
drop database if exists s_sh_ok;
create database s_sh_ok from vw01 publication p_sh_ok;
select id from s_sh_ok.v_sh;
drop database s_sh_ok;
-- @session

-- @session:id=1&user=vw01:test_account&password=111
use tp_shadow;
drop publication p_sh_ok;
drop temporary table base_sh;
alter table base_sh rename column id to newid;
create temporary table base_sh(id int);
-- the compatible temporary cannot mask the broken persistent definition
create publication p_sh_bad database tp_shadow table base_sh,v_sh account vw02;
drop temporary table base_sh;
drop view v_sh;
drop table base_sh;
drop database tp_shadow;
-- @session

-- the masked-broken publication leaves no row, and no subscriber can
-- subscribe to it
select pub_name from mo_catalog.mo_pubs where pub_name = 'p_sh_bad' order by pub_name;

-- @session:id=2&user=vw02:test_account&password=111
drop database if exists s_sh_bad;
create database s_sh_bad from vw01 publication p_sh_bad;
-- @session

-- teardown
-- @session:id=1&user=vw01:test_account&password=111
drop publication if exists p_g2g;
drop database if exists vw01_pub_db;
-- @session
drop publication p_view;
drop publication p_db_level;
drop publication p_full;
drop publication p_const;
drop publication p_unusedcte;
drop publication p_usedcte;
drop publication p_fwd;
drop publication p_nestedcte;
drop publication p_nestedcte2;
drop publication p_mask;
drop publication p_dualcte;
drop publication p_dualtab2;
drop publication p_narrow;
drop publication p_sysuser;
drop publication p_all_good;
drop publication p_alluser;
drop publication p_rebind;
drop publication p_alive;
drop publication p_late;
drop view v_late;
drop view v_rebind;
drop view v_alive;
drop view v_alluser;
drop table healthy;
drop table other;
drop table src;
drop table `dual`;
drop table dualctl;
drop table rbase;
drop table rbase2;
drop database view_pub_db;
drop database other_db_27767;
drop database other_view_db;
drop account vw01;
drop account vw02;
drop account vw03;
drop account vw04;
drop account vw05;
