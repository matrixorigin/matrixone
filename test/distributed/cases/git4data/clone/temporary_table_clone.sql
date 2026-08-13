-- @suite
-- @case
-- @desc:temporary table clone lifecycle, isolation, and failure atomicity
-- @label:bvt

drop database if exists clone_temp_target;
create database clone_temp_target;
use clone_temp_target;

create table src (
    id int primary key,
    v varchar(20) not null,
    unique key uk_v(v),
    key idx_v(v)
);
insert into src values (1, 'one'), (2, 'two');

-- Exact issue path: the destination is immediately usable and preserves the
-- source schema and rows.
create temporary table temp_dst clone src;
show create table temp_dst;
select * from temp_dst order by id;

-- The same alias remains unavailable to later CREATE variants until dropped.
create temporary table temp_dst clone src;
create temporary table temp_dst (id int);
create temporary table if not exists temp_dst clone src;
select count(*) from temp_dst;

-- A second connection cannot see this session's temporary destination, but can
-- create an independent temporary clone under the same alias.
-- @session:id=1{
use clone_temp_target;
select * from temp_dst;
create temporary table temp_dst clone src;
insert into temp_dst values (3, 'three');
select * from temp_dst order by id;
drop temporary table temp_dst;
-- @session

select * from temp_dst order by id;

-- DROP TEMPORARY TABLE IF EXISTS must release the alias for both clone and
-- ordinary temporary-table creation.
drop temporary table if exists temp_dst;
create temporary table temp_dst (id int primary key, v varchar(20));
insert into temp_dst values (4, 'four');
select * from temp_dst;
drop temporary table if exists temp_dst;
create temporary table temp_dst clone src;
select count(*) from temp_dst;
drop temporary table temp_dst;

-- The temporary destination may shadow its permanent source. The clone must
-- write only to the generated physical destination and leave the source intact.
create temporary table src clone src;
insert into src values (3, 'temporary shadow');
select * from src order by id;
drop temporary table src;
select * from src order by id;

-- A late failure after clone execution must roll back the physical relation and
-- remove the session alias so the name is immediately reusable.
select enable_fault_injection();
select add_fault_point('fj/cn/clone_fails', ':::', 'echo', 40, 'clone_temp_target.temp_failed');
create temporary table temp_failed clone src;
select disable_fault_injection();
select count(*) from mo_catalog.mo_tables
where reldatabase = 'clone_temp_target'
  and relname like '__mo_tmp_%_clone_temp_target_temp_failed';
create temporary table temp_failed (id int primary key);
insert into temp_failed values (42);
select * from temp_failed;
drop temporary table temp_failed;

-- Unsupported temporary foreign keys fail before publishing any destination,
-- and therefore also leave the alias reusable.
create table parent (id int primary key);
create table src_fk (
    id int primary key,
    parent_id int,
    constraint fk_parent foreign key(parent_id) references parent(id)
);
create temporary table temp_fk clone src_fk;
create temporary table temp_fk (id int primary key);
insert into temp_fk values (7);
select * from temp_fk;
drop temporary table temp_fk;

-- An empty source is a valid clone boundary.
create table empty_src (id bigint primary key, payload varchar(10));
create temporary table empty_dst clone empty_src;
select count(*) from empty_dst;
drop temporary table empty_dst;

-- Unsupported partitioned temporary tables fail without reserving the alias.
create table partitioned_src (id int primary key) partition by hash(id) partitions 2;
create temporary table temp_partitioned clone partitioned_src;
create temporary table temp_partitioned (id int primary key);
insert into temp_partitioned values (8);
select * from temp_partitioned;
drop temporary table temp_partitioned;

-- The cloned allocator must continue after the source's current sequence.
create table auto_src (
    id bigint auto_increment primary key,
    payload varchar(20)
) auto_increment = 10;
insert into auto_src(payload) values ('first'), ('second');
create temporary table auto_dst clone auto_src;
insert into auto_dst(payload) values ('third');
select * from auto_dst order by id;
drop temporary table auto_dst;

-- Cloning from a temporary source to a permanent destination remains valid.
create temporary table temp_src (id int primary key, v varchar(20));
insert into temp_src values (9, 'temporary source');
create table permanent_dst clone temp_src;
select * from permanent_dst;
drop table permanent_dst;
create temporary table temp_from_temp clone temp_src;
select * from temp_from_temp;
drop temporary table temp_from_temp;
drop temporary table temp_src;

-- Existing FULLTEXT and IVF temporary-table index support must also survive the
-- clone path, including hidden physical index-table names.
create table src_ft (
    id int primary key,
    title varchar(100),
    body text,
    fulltext index ft(title, body)
);
insert into src_ft values
    (1, 'apple news', 'red apple fruit'),
    (2, 'banana news', 'yellow banana');
create temporary table temp_ft clone src_ft;
select id, title from temp_ft where match(title, body) against ('apple') order by id;
drop temporary table temp_ft;

set experimental_ivf_index = 1;
create table src_vec (
    id int primary key,
    v vecf32(3),
    index idx_ivf using ivfflat(v) lists = 1 op_type 'vector_l2_ops'
);
insert into src_vec values
    (1, '[1,2,3]'),
    (2, '[2,2,3]'),
    (3, '[5,5,5]');
create temporary table temp_vec clone src_vec;
select id from temp_vec order by l2_distance(v, '[2,2,3]') limit 2;
drop temporary table temp_vec;
set experimental_ivf_index = 0;

-- Temporary aliases and their physical relations are session/account-owned, so
-- TO ACCOUNT must be rejected before snapshot resolution, background-transaction
-- creation, or alias publication. Closing the rejected statement's session must
-- leave neither source-session state nor a target-account physical relation.
drop account if exists clone_temp_target_account;
create account clone_temp_target_account admin_name 'admin' identified by '111';
-- @session:id=4&user=clone_temp_target_account:admin&password=111{
create database clone_temp_target;
-- @session
drop snapshot if exists clone_temp_cross_account_snapshot;
create snapshot clone_temp_cross_account_snapshot for table clone_temp_target src;
-- @session:id=5{
use clone_temp_target;
create temporary table clone_temp_target.temp_cross_account
    clone clone_temp_target.src {snapshot = 'clone_temp_cross_account_snapshot'}
    to account clone_temp_target_account;
drop temporary table if exists clone_temp_target.temp_cross_account;
create temporary table clone_temp_target.temp_cross_account (id int primary key);
insert into temp_cross_account values (10);
select * from temp_cross_account;
drop temporary table temp_cross_account;
-- @session
-- @session:id=6&user=clone_temp_target_account:admin&password=111{
use clone_temp_target;
select count(*) from mo_catalog.mo_tables
where reldatabase = 'clone_temp_target'
  and relname like '__mo_tmp_%_clone_temp_target_temp_cross_account';
select * from temp_cross_account;
-- @session
drop snapshot clone_temp_cross_account_snapshot;
drop account clone_temp_target_account;

drop database clone_temp_target;

-- Suspending an account closes its live sessions. The connection cleanup must
-- asynchronously remove a temporary clone's physical table before the account
-- is reopened.
drop account if exists clone_temp_disconnect_account;
create account clone_temp_disconnect_account admin_name 'admin' identified by '111';
-- @session:id=2&user=clone_temp_disconnect_account:admin&password=111{
create database clone_temp_disconnect;
use clone_temp_disconnect;
create table src (id int primary key);
insert into src values (1);
create temporary table temp_disconnect clone src;
-- @session
alter account clone_temp_disconnect_account suspend;
alter account clone_temp_disconnect_account open;
-- @session:id=3&user=clone_temp_disconnect_account:admin&password=111{
-- @wait_expect(2, 30)
select count(*) from mo_catalog.mo_tables
where reldatabase = 'clone_temp_disconnect'
  and relname like '__mo_tmp_%_clone_temp_disconnect_temp_disconnect';
drop database clone_temp_disconnect;
-- @session
drop account clone_temp_disconnect_account;
