drop snapshot if exists recreated_pub_s;
drop account if exists recreated_pub;
drop account if exists recreated_sub;
create account recreated_pub admin_name 'admin' identified by '111';
create account recreated_sub admin_name 'admin' identified by '111';
set @old_pub_id = (select account_id from mo_catalog.mo_account where account_name = 'recreated_pub');
set @old_sub_id = (select account_id from mo_catalog.mo_account where account_name = 'recreated_sub');

-- @session:id=1&user=recreated_pub:admin&password=111
create database app;
create table app.t(id int);
insert into app.t values (17);
create publication p database app account recreated_sub;
-- @session
-- @session:id=2&user=recreated_sub:admin&password=111
create database subdb from recreated_pub publication p;
create role reader;
create user u1 identified by '111' default role reader;
grant reader to u1;
grant connect on account * to reader;
grant select on table subdb.* to reader;
-- @session
create snapshot recreated_pub_s for cluster;
set @old_owner = (select owner from mo_catalog.mo_pubs where account_name = 'recreated_pub' and pub_name = 'p');
set @old_creator = (select creator from mo_catalog.mo_pubs where account_name = 'recreated_pub' and pub_name = 'p');
drop account recreated_sub;
drop account recreated_pub;
restore cluster {snapshot='recreated_pub_s'};
select account_id <> @old_pub_id as publisher_recreated from mo_catalog.mo_account where account_name = 'recreated_pub';
select account_id <> @old_sub_id as subscriber_recreated from mo_catalog.mo_account where account_name = 'recreated_sub';
select count(*) as bound_publication from mo_catalog.mo_pubs p
join mo_catalog.mo_account a on p.account_id = a.account_id
where a.account_name = 'recreated_pub' and p.pub_name = 'p';
select count(*) as publication_principals from mo_catalog.mo_pubs
where account_name = 'recreated_pub' and pub_name = 'p'
and owner = @old_owner and creator = @old_creator;
-- @session:id=3&user=recreated_pub:admin&password=111
select * from app.t;
-- @session
-- @session:id=4&user=recreated_sub:u1:reader&password=111
select * from subdb.t;
delete from subdb.t;
-- @session
-- @session:id=5&user=recreated_sub:admin&password=111
select count(*) as bound_subscription_grants from mo_catalog.mo_role_privs p
join mo_catalog.mo_database d on p.obj_id = d.dat_id
where p.role_name = 'reader' and p.privilege_level = 'd.*' and d.datname = 'subdb';
-- @session
drop snapshot recreated_pub_s;
-- A deleted publication leaves a valid but inactive subscription in the snapshot.
-- Restoring that snapshot must not treat expected absence as a catalog query error.
-- @session:id=6&user=recreated_pub:admin&password=111
create publication expired_p database app account recreated_sub;
-- @session
-- @session:id=7&user=recreated_sub:admin&password=111
create database expired_sub from recreated_pub publication expired_p;
-- @session
-- @session:id=8&user=recreated_pub:admin&password=111
drop publication expired_p;
-- @session
create snapshot recreated_pub_s for cluster;
restore cluster {snapshot='recreated_pub_s'};
select count(*) as live_publications from mo_catalog.mo_pubs
where account_name = 'recreated_pub' and pub_name = 'p';
select count(*) as expired_publications from mo_catalog.mo_pubs
where account_name = 'recreated_pub' and pub_name = 'expired_p';
-- @session:id=9&user=recreated_sub:u1:reader&password=111
select * from subdb.t;
-- @session
drop snapshot recreated_pub_s;
drop account recreated_sub;
drop account recreated_pub;
