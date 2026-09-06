-- Queries through a subscription must resolve ordinary secondary-index tables
-- in the publisher account and physical database, not in the subscription alias.
drop account if exists pub_idx_28279;
drop account if exists sub_idx_28279_a;
drop account if exists sub_idx_28279_b;
create account pub_idx_28279 admin_name = 'admin' identified by '111';
create account sub_idx_28279_a admin_name = 'admin' identified by '111';
create account sub_idx_28279_b admin_name = 'admin' identified by '111';

-- @session:id=1&user=pub_idx_28279:admin&password=111
create database `pub-idx-28279`;
create table `pub-idx-28279`.events (
    id bigint primary key,
    event_type varchar(32),
    created_at bigint,
    key idx_event_type(event_type),
    key idx_created_at(created_at)
);
insert into `pub-idx-28279`.events values
    (1, 'login', 10),
    (2, 'other', 20),
    (3, 'login', 30);
create table `pub-idx-28279`.secret_events (id bigint primary key);
insert into `pub-idx-28279`.secret_events values (99);
create publication pub_idx_28279 database `pub-idx-28279` table events account sub_idx_28279_a, sub_idx_28279_b;
-- @session

-- @session:id=2&user=sub_idx_28279_a:admin&password=111
create database sub_idx_alias_a from pub_idx_28279 publication pub_idx_28279;
use sub_idx_alias_a;
-- Base-table controls.
select id from events ignore index (idx_event_type, idx_created_at) where event_type = 'login' order by id;
select id from events ignore index (idx_event_type, idx_created_at) where created_at = 20 order by id;
-- On this minimum fixture, each natural predicate selects a distinct hidden
-- ordinary secondary-index table on 4.2.
select id from events where event_type = 'login' order by id;
select id from events where created_at = 20 order by id;
-- A resolved publisher context must not broaden the publication boundary.
select * from secret_events;
-- @session

-- @session:id=3&user=sub_idx_28279_b:admin&password=111
create database sub_idx_alias_b from pub_idx_28279 publication pub_idx_28279;
use sub_idx_alias_b;
select id from events where event_type = 'login' order by id;
drop database sub_idx_alias_b;
-- @session

-- @session:id=2&user=sub_idx_28279_a:admin&password=111
drop database sub_idx_alias_a;
-- @session

-- @session:id=1&user=pub_idx_28279:admin&password=111
drop publication pub_idx_28279;
drop database `pub-idx-28279`;
-- @session

drop account sub_idx_28279_a;
drop account sub_idx_28279_b;
drop account pub_idx_28279;
