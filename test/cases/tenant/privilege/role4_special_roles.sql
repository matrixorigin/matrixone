drop role if exists rolex;
create role rolex;
drop user if exists userx;
create user userx identified by '111';
-- TODO: add cases for accountadmin
grant moadmin to root,dump;
grant moadmin to userx;
grant moadmin to rolex;
grant rolex to moadmin;
grant public to root,dump;
grant public to userx;
grant public to rolex;
grant rolex to public;

revoke moadmin from root;
revoke moadmin from dump;
revoke moadmin from userx;
revoke moadmin from rolex;
revoke rolex from moadmin;
revoke public from root;
revoke public from dump;
revoke public from userx;
revoke public from rolex;
revoke rolex from public;

grant show databases,create database on account * to moadmin;
revoke show databases,create database on account * from moadmin;
revoke connect on account * from public;
revoke show databases on account * from public;
drop role rolex;
drop user userx;