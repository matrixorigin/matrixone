drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';
drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db09;
create database db09;
use db09;
drop table if exists index01;
create table index01(
                        col1 int not null,
                        col2 date not null,
                        col3 varchar(16) not null,
                        col4 int unsigned not null,
                        primary key (col1)
);
insert into index01 values(1, '1980-12-17','Abby', 21);
insert into index01 values(2, '1981-02-20','Bob', 22);
insert into index01 values(3, '1981-02-20','Bob', 22);
select count(*) from index01;

drop table if exists index02;
create table index02(col1 char, col2 int, col3 binary);
insert into index02 values('a', 33, 1);
insert into index02 values('c', 231, 0);
alter table index02 add key pk(col1) comment 'primary key';
select count(*) from index02;

drop database if exists db10;
create database db10;
use db10;
drop table if exists index03;
create table index03 (
                         emp_no      int             not null,
                         birth_date  date            not null,
                         first_name  varchar(14)     not null,
                         last_name   varchar(16)     not null,
                         gender      varchar(5)      not null,
                         hire_date   date            not null,
                         primary key (emp_no)
) partition by range columns (emp_no)(
    partition p01 values less than (100001),
    partition p02 values less than (200001),
    partition p03 values less than (300001),
    partition p04 values less than (400001)
);

insert into index03 values (9001,'1980-12-17', 'SMITH', 'CLERK', 'F', '2008-12-17'),
                           (9002,'1981-02-20', 'ALLEN', 'SALESMAN', 'F', '2008-02-20');

-- @session

drop snapshot if exists pub_sp;
create snapshot pub_sp for account acc01;

-- @session:id=1&user=acc01:test_account&password=111
drop publication if exists pub05;
create publication pub05 database db09 account acc02 comment 'publish db09';
drop publication if exists pub06;
create publication pub06 database db10 account acc02 comment 'publish db10';
-- @ignore:5,6
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
drop database if exists sub05;
create database sub05 from acc01 publication pub05;
show databases;
use sub05;
show create table index01;
select * from index02;
-- @session

restore account acc01 from snapshot pub_sp;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,6
show publications;
show databases like 'db%';
-- @session

drop account if exists acc03;
create account acc03 admin_name 'test_account' identified by '111';
-- @session:id=36&user=acc03:test_account&password=111
-- @ignore:5,6
show publications;
show databases like 'db%';
-- @session

restore account acc01 from snapshot pub_sp to account acc03;

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,6
show publications;
show databases like 'db%';
-- @session

drop snapshot pub_sp;
drop account acc01;
drop account acc02;
drop account acc03;
-- @ignore:1
show snapshots;
