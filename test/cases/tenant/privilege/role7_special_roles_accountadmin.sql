-- cases for accountadmin
drop account if exists accx;
create account accx admin_name 'xcca' identified by '111';

-- @session:id=3&user=accx:xcca&password=111
drop role if exists rolex;
create role rolex;
drop user if exists userx,root,dump;
create user userx identified by '111',root identified by '111',dump identified by '111';
grant accountadmin to xcca;
grant accountadmin to userx;
grant accountadmin to root;
grant accountadmin to dump;
grant accountadmin to rolex;
grant rolex to accountadmin;
grant public to xcca;
grant public to userx;
grant public to rolex;
grant rolex to public;

revoke accountadmin from xcca;
revoke accountadmin from userx;
revoke accountadmin from root;
revoke accountadmin from dump;
revoke accountadmin from rolex;
revoke rolex from accountadmin;
revoke public from xcca;
revoke public from userx;
revoke public from root;
revoke public from dump;
revoke public from rolex;
revoke rolex from public;

grant show databases,create database on account * to accountadmin;
revoke show databases,create database on account * from accountadmin;
revoke connect on account * from public;
revoke show databases on account * from public;

-- create special role
create role accountadmin;
create role moadmin;

drop role rolex;
drop user userx,root,dump;
-- @session
drop account if exists accx;