drop publication if exists pub_secondary_idx;
drop account if exists sub_secondary_idx;
drop database if exists pub_secondary_idx_db;
create account sub_secondary_idx admin_name = 'admin' identified by '111';

create database pub_secondary_idx_db;
create table pub_secondary_idx_db.audit_events (
    id int primary key,
    event_type varchar(32),
    created_at bigint,
    index idx_event_type (event_type),
    index idx_created_at (created_at)
);
insert into pub_secondary_idx_db.audit_events
select g.result,
       if(g.result in (42, 4242), 'login_success', 'other'),
       g.result
from generate_series(1, 100000) g;
-- @ignore:0
select mo_ctl('dn', 'flush', 'pub_secondary_idx_db.audit_events');
select sleep(1);
create publication pub_secondary_idx database pub_secondary_idx_db account sub_secondary_idx;

-- @session:id=1&user=sub_secondary_idx:admin&password=111
drop database if exists sub_secondary_idx_db;
create database sub_secondary_idx_db from sys publication pub_secondary_idx;
-- @separator:table
-- @regex("Index Table Scan.*idx_event_type", true)
explain select count(*) as login_count from sub_secondary_idx_db.audit_events force index (idx_event_type)
where event_type = 'login_success';
select count(*) as login_count from sub_secondary_idx_db.audit_events force index (idx_event_type)
where event_type = 'login_success';
select count(*) as recent_count from sub_secondary_idx_db.audit_events force index (idx_created_at)
where created_at >= 99999;
drop database sub_secondary_idx_db;
-- @session

drop publication pub_secondary_idx;
drop database pub_secondary_idx_db;
drop account sub_secondary_idx;
