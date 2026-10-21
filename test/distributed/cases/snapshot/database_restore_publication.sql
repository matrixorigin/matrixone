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
