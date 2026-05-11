drop database if exists test;
create database test;

create table test.t1 (a int);
create table test.t2 (a int);

create snapshot sp1 for cluster;
create table test.t3 clone test.t1 {snapshot = "sp1"};

drop snapshot if exists acc1;
create account acc1 admin_name "root1" identified by "111";
-- @session:id=1&user=acc1:root1&password=111
create database if not exists test;
create table test.t1(a int);
-- @session
create snapshot sp2 for account acc1;
create table test.t4 clone test.t1 {snapshot = "sp2"};

create database if not exists test2;
create snapshot sp3 for database test2;
create table test.t5 clone test.t1 {snapshot = "sp3"};

create snapshot sp4 for table test t1;
create table test.t6 clone test.t2 {snapshot = "sp4"};

create database test3 clone test {snapshot = "sp4"};

drop snapshot sp1;
drop snapshot sp2;
drop snapshot sp3;
drop snapshot sp4;
drop database test;
drop database test2;
drop account acc1;
