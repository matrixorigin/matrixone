-- Regression for issue #26114.
-- Cross-account DATA BRANCH quota and metadata ownership use the target account.
drop account if exists bvt_issue_26114_acc;
create account bvt_issue_26114_acc admin_name = 'admin' identified by '111';

-- @ignore:0
select mo_feature_registry_upsert(
    'branch',
    'Branch feature',
    '{"allowed_scope":[]}',
    true
);
-- @ignore:0
select mo_feature_registry_upsert(
    'snapshot',
    'Snapshot feature',
    '{"allowed_scope":["account","database","table"]}',
    true
);
set @target_id = (
    select account_id from mo_catalog.mo_account
    where account_name = 'bvt_issue_26114_acc'
);
-- @ignore:0
select mo_feature_limit_upsert(@target_id, 'snapshot', 'table', -1);
-- @ignore:0
select mo_feature_limit_upsert(@target_id, 'snapshot', 'database', -1);
-- @ignore:0
select mo_feature_limit_upsert(@target_id, 'branch', '', 0);

-- @session:id=1&user=bvt_issue_26114_acc:admin&password=111{
drop database if exists bvt_issue_26114_dst;
create database bvt_issue_26114_dst;
-- @session}

drop snapshot if exists bvt_issue_26114_table_sp;
drop snapshot if exists bvt_issue_26114_db_sp;
drop database if exists bvt_issue_26114_src;
create database bvt_issue_26114_src;
create table bvt_issue_26114_src.base(
    id int primary key,
    val varchar(20)
);
insert into bvt_issue_26114_src.base values (1, 'one');
create snapshot bvt_issue_26114_table_sp
for table bvt_issue_26114_src base;
create snapshot bvt_issue_26114_db_sp
for database bvt_issue_26114_src;

-- @regex("feature BRANCH with scope .* has disabled for account bvt_issue_26114_acc",true)
data branch create table bvt_issue_26114_dst.blocked_table
from bvt_issue_26114_src.base{snapshot='bvt_issue_26114_table_sp'}
to account bvt_issue_26114_acc;

-- @regex("feature BRANCH with scope .* has disabled for account bvt_issue_26114_acc",true)
data branch create database bvt_issue_26114_blocked_db
from bvt_issue_26114_src{snapshot='bvt_issue_26114_db_sp'}
to account bvt_issue_26114_acc;

-- @session:id=1&user=bvt_issue_26114_acc:admin&password=111{
select count(*) as blocked_table_rows
from mo_catalog.mo_tables
where reldatabase = 'bvt_issue_26114_dst'
  and relname = 'blocked_table';
select count(*) as blocked_database_rows
from mo_catalog.mo_database
where datname = 'bvt_issue_26114_blocked_db';
-- @session}

select count(*) as active_metadata_after_rejection
from mo_catalog.mo_branch_metadata b
join mo_catalog.mo_tables t on t.rel_id = b.table_id
where t.account_id = @target_id
  and b.table_deleted = false;

-- Raise quota and cover both table- and database-level success.
-- @ignore:0
select mo_feature_limit_upsert(@target_id, 'branch', '', 3);
data branch create table bvt_issue_26114_dst.allowed_table
from bvt_issue_26114_src.base{snapshot='bvt_issue_26114_table_sp'}
to account bvt_issue_26114_acc;
data branch create database bvt_issue_26114_allowed_db
from bvt_issue_26114_src{snapshot='bvt_issue_26114_db_sp'}
to account bvt_issue_26114_acc;

-- @session:id=1&user=bvt_issue_26114_acc:admin&password=111{
select id, val from bvt_issue_26114_dst.allowed_table order by id;
select id, val from bvt_issue_26114_allowed_db.base order by id;
-- @session}

select count(*) as target_owned_active_metadata
from mo_catalog.mo_branch_metadata b
join mo_catalog.mo_tables t on t.rel_id = b.table_id
where t.account_id = @target_id
  and b.creator = @target_id
  and b.table_deleted = false;

-- Exhaust the remaining target quota with a second table, then reject the next.
data branch create table bvt_issue_26114_dst.allowed_table_2
from bvt_issue_26114_src.base{snapshot='bvt_issue_26114_table_sp'}
to account bvt_issue_26114_acc;
-- @regex("exceeds.*limit of 3|limit of 3",true)
data branch create table bvt_issue_26114_dst.rejected_after_limit
from bvt_issue_26114_src.base{snapshot='bvt_issue_26114_table_sp'}
to account bvt_issue_26114_acc;

-- @session:id=1&user=bvt_issue_26114_acc:admin&password=111{
select count(*) as rejected_table_rows
from mo_catalog.mo_tables
where reldatabase = 'bvt_issue_26114_dst'
  and relname = 'rejected_after_limit';
drop database bvt_issue_26114_allowed_db;
drop database bvt_issue_26114_dst;
-- @session}

drop snapshot bvt_issue_26114_table_sp;
drop snapshot bvt_issue_26114_db_sp;
drop database bvt_issue_26114_src;
drop account bvt_issue_26114_acc;
