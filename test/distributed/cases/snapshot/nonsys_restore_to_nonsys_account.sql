-- Explicit-account and implicit-current-account snapshots resolve to the same
-- snapshot owner in doCreateSnapshot. Keep this differential contract here;
-- nonsys_create_snapshot_without_account.sql owns the shared downstream restore
-- matrix instead of repeating it for both spellings.
drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

-- @session:id=1&user=acc01:test_account&password=111
drop database if exists snapshot_explicit_contract;
create database snapshot_explicit_contract;
use snapshot_explicit_contract;
create table t (id int primary key, value int);
insert into t values (1, 10), (2, 20);

drop snapshot if exists sp_implicit;
drop snapshot if exists sp_explicit;
create snapshot sp_implicit for account;
create snapshot sp_explicit for account acc01;

-- Both forms must expose the same point-in-time state.
select 'implicit' as snapshot_form, count(*) as rows_read from t {snapshot = 'sp_implicit'}
union all
select 'explicit', count(*) from t {snapshot = 'sp_explicit'};
select 'implicit' as snapshot_form, id, value from t {snapshot = 'sp_implicit'}
union all
select 'explicit', id, value from t {snapshot = 'sp_explicit'} order by snapshot_form, id;

-- Retain explicit-account restore coverage at table, database and account level.
insert into t values (3, 30);
restore table snapshot_explicit_contract.t{snapshot="sp_explicit"};
select * from t order by id;

insert into t values (3, 30);
restore database snapshot_explicit_contract{snapshot="sp_explicit"};
select * from t order by id;

drop table t;
restore account acc01{snapshot="sp_explicit"};
use snapshot_explicit_contract;
select * from t order by id;
select count(*) from t {snapshot = 'sp_implicit'};

drop snapshot sp_implicit;
drop snapshot sp_explicit;
drop database snapshot_explicit_contract;
-- @session

drop account acc01;
