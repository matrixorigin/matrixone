-- Subscription is a non-reserved identifier, including native application aliases.
--- @metacmp(true)
drop database if exists subscription_identifier;
create database subscription_identifier;
use subscription_identifier;
create table dmz_org (id int primary key, c_authconfig varchar(200), c_sub varchar(200));
insert into dmz_org values (1,'{"method":"password"}','中文 😀𠮷'),(2,null,null);
select id,c_sub as subscription from dmz_org order by id;
select id,c_sub subscription from dmz_org order by id;
select id,c_sub as `subscription` from dmz_org order by id;
select id,c_sub as subscriptions from dmz_org order by id;
-- Preserve the unchanged Documize lookup projection and NULL fallback.
select coalesce(c_authconfig,JSON_UNQUOTE('{}')) as authconfig,
       coalesce(c_sub,JSON_UNQUOTE('{}')) as subscription from dmz_org order by id;
prepare subscription_alias_prepare from 'select id,c_sub as subscription from dmz_org order by id';
execute subscription_alias_prepare;
deallocate prepare subscription_alias_prepare;
-- Rejection leaves the session usable and the two-row fixture intact.
insert into dmz_org(id) values(1);
select 1;
select id,c_sub as subscription from dmz_org order by id;
drop database subscription_identifier;
show databases like 'subscription_identifier';
