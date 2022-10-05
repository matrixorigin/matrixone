drop user if exists anne;
create user anne identified by '111';
drop role if exists intern,lead,newrole,rolex,dev,test;
create role intern,lead,newrole,rolex,dev,test;

grant intern to anne;
grant dev to intern;
grant test to dev;
grant create table on database * to intern with grant option;
grant create database on account * to dev;
grant drop database on account * to dev with grant option;
grant drop table on database * to test with grant option;

grant lead to anne with grant option;
grant dev to lead with grant option;
grant create database on account * to lead with grant option;

grant newrole to anne;
grant dev to newrole;

grant newrole to lead with grant option;

grant newrole to anne;
grant newrole to rolex with grant option;

drop user anne;
drop role intern,lead,newrole,rolex,dev,test;