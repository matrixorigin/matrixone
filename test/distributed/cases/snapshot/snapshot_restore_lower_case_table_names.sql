create account a1 ADMIN_NAME 'admin1' IDENTIFIED BY 'test123';

-- @session:id=1&user=a1:admin1&password=test123
# default value is 1
select @@lower_case_table_names;

# set to 0
set global lower_case_table_names = 0;
-- @session

-- @session:id=2&user=a1:admin1&password=test123
# it's 0 now
select @@lower_case_table_names;

create database test;
use test;
create table TT (c1 int);
insert into TT values(1);
create table tt(a1 int);
insert into tt values(2);
create table Tt(b1 int);
insert into Tt values(3);
create table tT(d1 int);
insert into tT values(4);
show create table TT;
show create table tt;
show create table Tt;
show create table tT;
show tables;
select * from TT;
select * from tt;
select * from Tt;
select * from tT;

drop snapshot if exists sp_lower_case_table_names;
create snapshot sp_lower_case_table_names for account a1;

drop database test;

restore account a1{snapshot="sp_lower_case_table_names"};

use test;
show tables;
show create table TT;
show create table tt;
show create table Tt;
show create table tT;
select * from TT;
select * from tt;
select * from Tt;
select * from tT;
-- @session

drop snapshot if exists sp_lower_case_table_a1;
create snapshot sp_lower_case_table_a1 for account a1;

restore account a1{snapshot="sp_lower_case_table_a1"};

-- @session:id=3&user=a1:admin1&password=test123
use test;
show tables;
show create table TT;
show create table tt;
show create table Tt;
show create table tT;
select * from TT;
select * from tt;
select * from Tt;
select * from tT;
-- @session

drop account if exists a1;
drop snapshot if exists sp_lower_case_table_names;
drop snapshot if exists sp_lower_case_table_a1;
