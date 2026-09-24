drop publication if exists mo28279_pub_table;
drop publication if exists mo28279_pub_sec;
drop account if exists mo28279_sub_a;
drop account if exists mo28279_sub_b;
drop database if exists mo28279_pub_db;
create account mo28279_sub_a admin_name = 'admin' identified by '111';
create account mo28279_sub_b admin_name = 'admin' identified by '111';

create database mo28279_pub_db;
create table mo28279_pub_db.audit_events (
    id int primary key,
    event_type varchar(32),
    created_at bigint,
    index idx_event_type (event_type),
    index idx_created_at (created_at)
);
insert into mo28279_pub_db.audit_events
select g.result,
       if(g.result in (42, 4242), 'login_success', concat('other_', g.result)),
       g.result
from generate_series(1, 100000) g;
create table mo28279_pub_db.private_events (id int primary key);
insert into mo28279_pub_db.private_events values (99);
-- @ignore:0
select mo_ctl('dn', 'flush', 'mo28279_pub_db.audit_events');
create publication mo28279_pub_sec database mo28279_pub_db account mo28279_sub_a, mo28279_sub_b;
create publication mo28279_pub_table database mo28279_pub_db table audit_events account mo28279_sub_a;

-- @session:id=1&user=mo28279_sub_a:admin&password=111
drop database if exists mo28279_sub_a_db;
create database mo28279_sub_a_db from sys publication mo28279_pub_sec;
select count(*) as total_count from mo28279_sub_a_db.audit_events;
-- The predicate is intentionally natural: the plan must choose the regular index.
-- @regex("Index Table Scan.*idx_event_type", true)
explain select count(*) as login_count from mo28279_sub_a_db.audit_events
where event_type = 'login_success';
select count(*) as login_count from mo28279_sub_a_db.audit_events
where event_type = 'login_success';
-- @regex("Index Table Scan.*idx_created_at", true)
explain select count(*) as recent_count from mo28279_sub_a_db.audit_events
where created_at >= 99999;
select count(*) as recent_count from mo28279_sub_a_db.audit_events
where created_at >= 99999;
select count(*) as login_count from mo28279_sub_a_db.audit_events
ignore index (idx_event_type, idx_created_at)
where event_type = 'login_success';
select * from mo28279_sub_a_db.private_events;
prepare indexed_stmt from 'select count(*) from mo28279_sub_a_db.audit_events where event_type = \'login_success\'';
execute indexed_stmt;
create database mo28279_sub_a_table_db from sys publication mo28279_pub_table;
-- @regex("Index Table Scan.*idx_event_type", true)
explain select count(*) as table_pub_login_count from mo28279_sub_a_table_db.audit_events
where event_type = 'login_success';
select count(*) as table_pub_login_count from mo28279_sub_a_table_db.audit_events
where event_type = 'login_success';
select * from mo28279_sub_a_table_db.private_events;
prepare table_indexed_stmt from 'select count(*) from mo28279_sub_a_table_db.audit_events where event_type = \'login_success\'';
execute table_indexed_stmt;
-- @session

-- @session:id=2&user=mo28279_sub_b:admin&password=111
drop database if exists mo28279_sub_b_db;
create database mo28279_sub_b_db from sys publication mo28279_pub_sec;
prepare second_indexed_stmt from 'select count(*) from mo28279_sub_b_db.audit_events where created_at >= 99999';
execute second_indexed_stmt;
-- @session

-- Publisher-side membership changes must invalidate the same indexed subscription query.
-- @session:id=3&user=sys:dump&password=111
alter publication mo28279_pub_sec account mo28279_sub_b;
-- @session:id=1&user=mo28279_sub_a:admin&password=111
execute indexed_stmt;
select count(*) from mo28279_sub_a_db.audit_events where event_type = 'login_success';
-- @session:id=3&user=sys:dump&password=111
alter publication mo28279_pub_sec account mo28279_sub_a, mo28279_sub_b;
-- @session:id=1&user=mo28279_sub_a:admin&password=111
execute indexed_stmt;
-- @session:id=3&user=sys:dump&password=111
drop publication mo28279_pub_sec;
-- @session:id=2&user=mo28279_sub_b:admin&password=111
execute second_indexed_stmt;
deallocate prepare second_indexed_stmt;
drop database mo28279_sub_b_db;
-- @session:id=1&user=mo28279_sub_a:admin&password=111
deallocate prepare indexed_stmt;
drop database mo28279_sub_a_db;
-- @session

-- @session:id=3&user=sys:dump&password=111
drop publication mo28279_pub_table;
-- @session:id=1&user=mo28279_sub_a:admin&password=111
execute table_indexed_stmt;
deallocate prepare table_indexed_stmt;
drop database mo28279_sub_a_table_db;
-- @session

drop database if exists mo28279_pub_db;
drop account mo28279_sub_a;
drop account mo28279_sub_b;
