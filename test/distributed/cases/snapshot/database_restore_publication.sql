-- Database replay is owned by RESTORE DATABASE, not the shared database worker
-- used by RESTORE ACCOUNT/CLUSTER. Cluster replay-once is also covered by
-- cluster/restore_pub.sql and restore_cluster_privileges.sql.
drop publication if exists restore_db_pub;
drop database if exists restore_db_source;
drop account if exists restore_db_subscriber;
create account restore_db_subscriber admin_name 'admin' identified by '111';
create database restore_db_source;
create table restore_db_source.parent_t(id int primary key);
create table restore_db_source.child_t(id int primary key, parent_id int, constraint fk_parent foreign key(parent_id) references restore_db_source.parent_t(id));
insert into restore_db_source.parent_t values(1);
insert into restore_db_source.child_t values(10,1);
create view restore_db_source.v as select c.id from restore_db_source.child_t c join restore_db_source.parent_t p on c.parent_id=p.id;
create publication restore_db_pub database restore_db_source account restore_db_subscriber comment 'owner''s copy';
-- @session:id=1&user=restore_db_subscriber:admin&password=111
create database restore_db_sub from sys publication restore_db_pub;
select id from restore_db_sub.child_t;
-- @session
create snapshot restore_db_pub_snapshot for database restore_db_source;
insert into restore_db_source.parent_t values(2);
insert into restore_db_source.child_t values(20,2);
restore database restore_db_source{snapshot='restore_db_pub_snapshot'};
select count(*) from mo_catalog.mo_pubs where pub_name='restore_db_pub' and database_name='restore_db_source';
select comment from mo_catalog.mo_pubs where pub_name='restore_db_pub';
select * from restore_db_source.v;
-- @session:id=1&user=restore_db_subscriber:admin&password=111
select id from restore_db_sub.child_t;
-- @session
-- A table restore must not replay the still-live publication.
restore table restore_db_source.child_t{snapshot='restore_db_pub_snapshot'};
select count(*) from mo_catalog.mo_pubs where pub_name='restore_db_pub';
-- Repeated database restore must replace, not duplicate, the publication.
restore database restore_db_source{snapshot='restore_db_pub_snapshot'};
select count(*) from mo_catalog.mo_pubs where pub_name='restore_db_pub';
-- @session:id=1&user=restore_db_subscriber:admin&password=111
select id from restore_db_sub.child_t;
-- @session
-- An external FK makes the database worker skip recreation. Keep both the
-- live data and live publication metadata, rather than replaying the snapshot.
create database restore_db_external;
create table restore_db_external.child_t(id int primary key, parent_id int, foreign key(parent_id) references restore_db_source.parent_t(id));
insert into restore_db_external.child_t values(1,1);
insert into restore_db_source.parent_t values(2);
alter publication restore_db_pub comment 'live comment';
restore database restore_db_source{snapshot='restore_db_pub_snapshot'};
select count(*) from restore_db_source.parent_t;
select count(*) from mo_catalog.mo_pubs where pub_name='restore_db_pub';
select comment from mo_catalog.mo_pubs where pub_name='restore_db_pub';
select * from restore_db_external.child_t;
drop database restore_db_external;
drop snapshot restore_db_pub_snapshot;
drop account restore_db_subscriber;
drop publication restore_db_pub;
drop database restore_db_source;

-- Replay must quote each identifier, not the comma-separated table list.
create account restore_named_subscriber admin_name 'admin' identified by '111';
create database `review-db`;
create table `review-db`.`review-items`(id int primary key);
create table `review-db`.`other-items`(id int primary key);
insert into `review-db`.`review-items` values(1);
insert into `review-db`.`other-items` values(1);
create publication `review-pub` database `review-db` table `review-items`,`other-items` account restore_named_subscriber comment 'owner''s copy';
-- @session:id=2&user=restore_named_subscriber:admin&password=111
create database restored_named_sub from sys publication `review-pub`;
select id from restored_named_sub.`review-items`;
-- @session
create snapshot restore_named_snapshot for database `review-db`;
insert into `review-db`.`review-items` values(2);
insert into `review-db`.`other-items` values(2);
restore database `review-db`{snapshot='restore_named_snapshot'};
select database_name,table_list,account_list,comment from mo_catalog.mo_pubs where pub_name='review-pub';
-- @session:id=2&user=restore_named_subscriber:admin&password=111
select id from restored_named_sub.`review-items`;
select id from restored_named_sub.`other-items`;
-- @session
restore database `review-db`{snapshot='restore_named_snapshot'};
select count(*) from mo_catalog.mo_pubs where pub_name='review-pub';
-- @session:id=2&user=restore_named_subscriber:admin&password=111
select id from restored_named_sub.`review-items`;
-- @session
drop snapshot restore_named_snapshot;
drop account restore_named_subscriber;
drop publication `review-pub`;
drop database `review-db`;

-- Removing the final account is a valid empty authorization set, not ALL.
create account restore_empty_subscriber admin_name 'admin' identified by '111';
create database restore_empty_db;
create table restore_empty_db.t(id int primary key);
insert into restore_empty_db.t values(1);
create publication restore_empty_pub database restore_empty_db account restore_empty_subscriber comment 'owner''s empty grant';
alter publication restore_empty_pub account drop restore_empty_subscriber;
create snapshot restore_empty_snapshot for database restore_empty_db;
insert into restore_empty_db.t values(2);
restore database restore_empty_db{snapshot='restore_empty_snapshot'};
select count(*) from restore_empty_db.t;
select count(*) from mo_catalog.mo_pubs where pub_name='restore_empty_pub' and account_list='' and comment='owner''s empty grant';
restore database restore_empty_db{snapshot='restore_empty_snapshot'};
select count(*) from mo_catalog.mo_pubs where pub_name='restore_empty_pub' and account_list='';
alter publication restore_empty_pub account add restore_empty_subscriber;
-- @session:id=3&user=restore_empty_subscriber:admin&password=111
create database restored_empty_sub from sys publication restore_empty_pub;
select id from restored_empty_sub.t;
-- @session
drop snapshot restore_empty_snapshot;
drop account restore_empty_subscriber;
drop publication restore_empty_pub;
drop database restore_empty_db;
