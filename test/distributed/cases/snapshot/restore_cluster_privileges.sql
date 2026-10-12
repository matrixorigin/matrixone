-- Cluster restore rebuilds grants after subscriptions, including recreated accounts.
drop snapshot if exists restore_cluster_priv_s;
drop account if exists restore_cluster_reader;
drop publication if exists restore_cluster_pub;
drop database if exists restore_cluster_db;
create account restore_cluster_reader admin_name 'admin' identified by '111';
create database restore_cluster_db;
create table restore_cluster_db.t (id int);
insert into restore_cluster_db.t values (1);
create publication restore_cluster_pub database restore_cluster_db account all;
-- @session:id=1&user=restore_cluster_reader:admin&password=111
create database subscribed from sys publication restore_cluster_pub;
create database app;
create table app.checkpoint (id int);
insert into app.checkpoint values (2);
create role reader;
create user u1 identified by '111' default role reader;
grant reader to u1;
grant connect on account * to reader;
grant select on table subscribed.* to reader;
grant select on table app.checkpoint to reader;
-- @session
-- @session:id=2&user=restore_cluster_reader:u1:reader&password=111
select * from subscribed.t;
select * from app.checkpoint;
-- @session
create snapshot restore_cluster_priv_s for cluster;
-- Account restore from a cluster snapshot must also bind the new table IDs.
restore account restore_cluster_reader {snapshot = 'restore_cluster_priv_s'};
-- @session:id=3&user=restore_cluster_reader:u1:reader&password=111
select * from subscribed.t;
select * from app.checkpoint;
-- @session
drop account restore_cluster_reader;
restore cluster {snapshot = 'restore_cluster_priv_s'};
-- @session:id=4&user=restore_cluster_reader:u1:reader&password=111
select * from subscribed.t;
select * from app.checkpoint;
delete from app.checkpoint;
-- @session
-- @session:id=5&user=restore_cluster_reader:admin&password=111
select count(*) as bound_subscription_grants from mo_catalog.mo_role_privs p
join mo_catalog.mo_database d on p.obj_id = d.dat_id
where p.role_name = 'reader' and p.obj_type = 'table'
and p.privilege_level = 'd.*' and d.datname = 'subscribed';
-- @session
drop snapshot restore_cluster_priv_s;
drop account restore_cluster_reader;
drop publication restore_cluster_pub;
drop database restore_cluster_db;
