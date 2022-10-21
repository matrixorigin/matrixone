drop user if exists anne,bill;
create user anne identified by '111' , bill identified by '111';
drop role if exists intern,lead,newrole,rolex,dev,test,rx;
create role intern,lead,newrole,rolex,dev,test,rx;

-- basci privilege
grant connect on account * to intern,lead,newrole,rolex;

grant intern to anne;
grant dev to intern;
grant test to dev;
grant create table on database * to intern with grant option;
grant create database on account * to dev;
grant drop database on account * to dev with grant option;
grant drop table on database * to test with grant option;

grant lead to anne with grant option;
grant dev to lead with grant option;
grant create table on database * to lead with grant option;

grant newrole to anne;
grant create table on database * to newrole with grant option;
grant dev to newrole with grant option;
grant newrole to lead with grant option;
grant newrole to anne;
grant newrole to rolex with grant option;
grant rolex to anne;

-- @session:id=2&user=sys:anne:intern&password=111
drop database if exists t;
create database t;
use t;
create table A(a int);
drop table A;

-- intern
grant create table on database * to rx;
grant create database on account * to rx;
grant drop database on account * to rx;
grant drop table on database * to rx;

grant test to rx,bill;
grant dev to rx,bill;
grant intern to rx,bill;
grant lead to rx,bill;
grant newrole to rx,bill;
grant rolex to rx,bill;

-- intern with secondary all
set secondary role all;
grant test to rx,bill;
grant dev to rx,bill;
grant intern to rx,bill;
grant lead to rx,bill;
grant newrole to rx,bill;
grant rolex to rx,bill;

set role lead;

drop database if exists t;
create database t;
use t;
create table A(a int);
drop table A;

-- lead
grant create table on database * to rx;
grant create database on account * to rx;
grant drop database on account * to rx;
grant drop table on database * to rx;

grant test to rx,bill;
grant dev to rx,bill;
grant intern to rx,bill;
grant lead to rx,bill;
grant newrole to rx,bill;
grant rolex to rx,bill;

-- lead with secondary all
set secondary role all;
grant test to rx,bill;
grant dev to rx,bill;
grant intern to rx,bill;
grant lead to rx,bill;
grant newrole to rx,bill;
grant rolex to rx,bill;

set role newrole;

drop database if exists t;
create database t;
use t;
create table A(a int);
drop table A;

-- newrole
grant create table on database * to rx;
grant create database on account * to rx;
grant drop database on account * to rx;
grant drop table on database * to rx;

grant test to rx,bill;
grant dev to rx,bill;
grant intern to rx,bill;
grant lead to rx,bill;
grant newrole to rx,bill;
grant rolex to rx,bill;

-- newrole with secondary all
set secondary role all;
grant test to rx,bill;
grant dev to rx,bill;
grant intern to rx,bill;
grant lead to rx,bill;
grant newrole to rx,bill;
grant rolex to rx,bill;

set role rolex;

drop database if exists t;
create database t;
use t;
create table A(a int);
drop table A;

-- rolex
grant create table on database * to rx;
grant create database on account * to rx;
grant drop database on account * to rx;
grant drop table on database * to rx;

grant test to rx,bill;
grant dev to rx,bill;
grant intern to rx,bill;
grant lead to rx,bill;
grant newrole to rx,bill;
grant rolex to rx,bill;

-- rolex with secondary all
set secondary role all;
grant test to rx,bill;
grant dev to rx,bill;
grant intern to rx,bill;
grant lead to rx,bill;
grant newrole to rx,bill;
grant rolex to rx,bill;

drop database t;
-- @session

drop user anne,bill;
drop role intern,lead,newrole,rolex,dev,test,rx;