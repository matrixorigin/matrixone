-- test tenant visibility of mo_locks and mo_transactions

drop account if exists sv_tenant;
drop database if exists sv_sys_db;
create account sv_tenant admin_name = 'admin' identified by '111';

create database sv_sys_db;
create table sv_sys_db.t1(a int primary key, b int);
insert into sv_sys_db.t1 values (1, 10);

-- @session:id=1&user=sv_tenant:admin&password=111{
create database sv_tenant_db;
create table sv_tenant_db.t1(a int primary key, b int);
insert into sv_tenant_db.t1 values (1, 20);
-- @session}

use sv_sys_db;
begin;
update t1 set b = b + 1 where a = 1;

-- @session:id=1&user=sv_tenant:admin&password=111{
use sv_tenant_db;
begin;
update t1 set b = b + 1 where a = 1;
-- @session}

-- The tenant session is asynchronous. Wait until both transactions are active
-- before checking their identities and the locks they hold.
-- @wait_expect(1, 10)
select count(distinct txn_id) = 2 from mo_transactions() t where t.user_txn = 'true';
-- The tenant session is asynchronous. Poll the public lock view until both
-- intended table locks have been acquired and published across the CNs.
-- @wait_expect(1, 10)
with target_table_ids as (
    select rel_id from mo_catalog.mo_tables
    where reldatabase in ('sv_sys_db', 'sv_tenant_db') and relname = 't1'
), visible_txns as (
    select txn_id from mo_locks() l join target_table_ids t on l.table_id = t.rel_id
    where l.txn_id <> ''
    union
    select lock_wait from mo_locks() l join target_table_ids t on l.table_id = t.rel_id
    where l.lock_wait <> ''
)
select count(distinct t.txn_id) = 2
from mo_transactions() t
join visible_txns v on t.txn_id = v.txn_id
where t.user_txn = 'true';

-- A regular tenant sees only its own transaction through both table functions
-- and catalog views.
-- @session:id=1&user=sv_tenant:admin&password=111{
select count(distinct txn_id) = 1 from mo_transactions() t where t.user_txn = 'true';
select count(*) = 1 from (
    select txn_id from mo_locks() l where l.txn_id <> ''
    union
    select lock_wait from mo_locks() l where l.lock_wait <> ''
) as visible_txns;
-- Prove that the single visible transaction/lock is this tenant's, rather than
-- merely proving that some transaction was returned.
select count(distinct t.txn_id) = 1
from mo_transactions() t
join mo_locks() l on t.txn_id = l.txn_id
where l.table_id = (
    select rel_id from mo_catalog.mo_tables
    where reldatabase = 'sv_tenant_db' and relname = 't1'
);
select count(distinct txn_id) = 1 from mo_catalog.mo_transactions where user_txn = 'true';
select count(*) = 1 from (
    select txn_id from mo_catalog.mo_locks where txn_id <> ''
    union
    select lock_wait from mo_catalog.mo_locks where lock_wait <> ''
) as visible_txns;
rollback;
select b from sv_tenant_db.t1 where a = 1;
drop database sv_tenant_db;
-- @session}

rollback;
select b from sv_sys_db.t1 where a = 1;
drop database sv_sys_db;
drop account sv_tenant;
