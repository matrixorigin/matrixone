-- cases for moadmin
drop role if exists rolex;
create role rolex;
drop user if exists userx;
create user userx identified by '111';
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
grant create account on account * to rolex;
grant drop account on account * to rolex;
grant alter account on account * to rolex;

drop user root;
drop user dump;
drop user root,dump;

drop role moadmin;
drop role public;
drop role moadmin,public;
drop role accountadmin;
drop role if exists accountadmin;
drop account if exists acc1;
create account acc1 admin_name 'admin' identified by '111';

-- @session:id=2&user=acc1:admin&password=111
drop user admin;
drop role accountadmin;
drop role public;
-- @session
drop account acc1;
drop account sys;
drop role rolex;
drop user userx;