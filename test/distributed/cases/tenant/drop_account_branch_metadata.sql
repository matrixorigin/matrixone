-- DROP ACCOUNT must remove account-owned rows from globally stored metadata.
-- The account is recreated with the same name to verify that old IDs remain isolated.

drop account if exists issue_27660_branch_metadata;
create account issue_27660_branch_metadata admin_name = 'admin' identified by '111';

set @issue_27660_old_id = (
  select account_id
  from mo_catalog.mo_account
  where account_name = 'issue_27660_branch_metadata'
);

-- @session:id=1&user=issue_27660_branch_metadata:admin&password=111
create database issue_27660_branch_db;
create table issue_27660_branch_db.base (id int primary key);
insert into issue_27660_branch_db.base values (1);
data branch create table issue_27660_branch_db.child from issue_27660_branch_db.base;

-- @session
select count(*) as branch_rows_before_drop
from mo_catalog.mo_branch_metadata
where creator = @issue_27660_old_id;
select count(*) as feature_limit_rows_before_drop
from mo_catalog.mo_feature_limit
where account_id = @issue_27660_old_id;
select feature_code, scope, quota
from mo_catalog.mo_feature_limit
where account_id = @issue_27660_old_id;

drop account issue_27660_branch_metadata;

select count(*) as account_rows_after_drop
from mo_catalog.mo_account
where account_id = @issue_27660_old_id;
select count(*) as branch_rows_after_drop
from mo_catalog.mo_branch_metadata
where creator = @issue_27660_old_id;
select count(*) as feature_limit_rows_after_drop
from mo_catalog.mo_feature_limit
where account_id = @issue_27660_old_id;

create account issue_27660_branch_metadata admin_name = 'admin' identified by '111';
set @issue_27660_new_id = (
  select account_id
  from mo_catalog.mo_account
  where account_name = 'issue_27660_branch_metadata'
);
select count(*) as recreated_with_new_id
from mo_catalog.mo_account
where account_name = 'issue_27660_branch_metadata'
  and account_id <> @issue_27660_old_id;

-- @session:id=2&user=issue_27660_branch_metadata:admin&password=111
create database issue_27660_recreated_db;
create table issue_27660_recreated_db.base (id int primary key);
data branch create table issue_27660_recreated_db.child from issue_27660_recreated_db.base;

-- @session
select count(*) as new_branch_rows_before_second_drop
from mo_catalog.mo_branch_metadata
where creator = @issue_27660_new_id;
select count(*) as old_branch_rows_after_recreate
from mo_catalog.mo_branch_metadata
where creator = @issue_27660_old_id;
select count(*) as new_feature_limit_rows_before_second_drop
from mo_catalog.mo_feature_limit
where account_id = @issue_27660_new_id;
select count(*) as old_feature_limit_rows_after_recreate
from mo_catalog.mo_feature_limit
where account_id = @issue_27660_old_id;

drop account issue_27660_branch_metadata;

select count(*) as recreated_account_rows_after_drop
from mo_catalog.mo_account
where account_id = @issue_27660_new_id;
select count(*) as recreated_branch_rows_after_drop
from mo_catalog.mo_branch_metadata
where creator = @issue_27660_new_id;
select count(*) as recreated_feature_limit_rows_after_drop
from mo_catalog.mo_feature_limit
where account_id = @issue_27660_new_id;
