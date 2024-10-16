create account acc1 ADMIN_NAME 'admin1' IDENTIFIED BY 'test123';
create account acc2 ADMIN_NAME 'admin2' IDENTIFIED BY 'test456';
create account acc3 ADMIN_NAME 'admin3' IDENTIFIED BY 'test789';

drop database if exists db1;
create database db1;
use db1;
create table t1 (a int,b int);
insert into t1 values (1, 1), (2, 2), (3, 3);
select * from t1;
create table t2 (a text);

drop database if exists db2;
create database db2;
use db2;
create table t1 (a float);
insert into t1 values (1.0), (1.1), (2.0);


########### create -> sub -> alter -> drop -> recreate ###########
# create publication
create publication pub1 database db1 account acc1,acc2;
-- @ignore:5,6
show publications;
-- @ignore:5,6
show publications like 'pub%';
-- @ignore:5,6
show publications like '%1';
show create publication pub1;

-- @session:id=1&user=acc1:admin1&password=test123
-- @ignore:5,7
show subscriptions;
-- @ignore:5,7
show subscriptions all;
-- @ignore:5,7
show subscriptions all like '%1';
-- @ignore:5,7
show subscriptions all like 'pub%';

create database syssub1 from sys publication pub1;
-- @ignore:5,7
show subscriptions;
-- @ignore:5,7
show subscriptions all;

use syssub1;
show tables;
-- @ignore:5,7
show subscriptions all;
-- @ignore:10
show table status;
-- @ignore:10
show table status like 't1';
desc t1;
show create table t1;
select * from t1;
-- @session

# alter authorized account
-- @ignore:5,6
show publications;
alter publication pub1 account acc3 comment 'this is a pub';
-- @session:id=1&user=acc1:admin1&password=test123
-- @ignore:5,7
show subscriptions all;
use syssub1;
-- @session
-- @session:id=2&user=acc2:admin2&password=test456
-- @ignore:5,7
show subscriptions all;
-- @session
-- @session:id=3&user=acc3:admin3&password=test789
-- @ignore:5,7
show subscriptions all;
-- @session

# drop
drop publication pub1;
-- @session:id=1&user=acc1:admin1&password=test123
-- @ignore:5,7
show subscriptions all;
use syssub1;
-- @session
-- @session:id=2&user=acc2:admin2&password=test456
-- @ignore:5,7
show subscriptions all;
-- @session
-- @session:id=3&user=acc3:admin3&password=test789
-- @ignore:5,7
show subscriptions all;
-- @session

# recreate with another db
create publication pub1 database db2 account acc1,acc2 comment 'this is a recreated pub';
-- @session:id=1&user=acc1:admin1&password=test123
-- @ignore:5,7
show subscriptions all;
use syssub1;
show tables;
-- @ignore:5,7
show subscriptions all;
-- @ignore:10
show table status;
-- @ignore:10
show table status like 't1';
desc t1;
show create table t1;
select * from t1;
-- @session
-- @session:id=2&user=acc2:admin2&password=test456
-- @ignore:5,7
show subscriptions all;
-- @session
-- @session:id=3&user=acc3:admin3&password=test789
-- @ignore:5,7
show subscriptions all;
-- @session


########### pub all ###########
# create publication to all accounts
create publication pub_all database db1 account all;
-- @ignore:5,7
show subscriptions all;
-- @session:id=1&user=acc1:admin1&password=test123
-- @ignore:5,7
show subscriptions all;
use syssub1;
-- @session
-- @session:id=2&user=acc2:admin2&password=test456
-- @ignore:5,7
show subscriptions all;
-- @session
-- @session:id=3&user=acc3:admin3&password=test789
-- @ignore:5,7
show subscriptions all;
-- @session

alter publication pub_all account acc1,acc2;
-- @session:id=1&user=acc1:admin1&password=test123
-- @ignore:5,7
show subscriptions all;
use syssub1;
-- @session
-- @session:id=2&user=acc2:admin2&password=test456
-- @ignore:5,7
show subscriptions all;
-- @session
-- @session:id=3&user=acc3:admin3&password=test789
-- @ignore:5,7
show subscriptions all;
-- @session


########### illegal pub ###########
create publication pub_self database db1 account sys;
create publication pub_to_not_exist database db1 account not_exist;


########### duplicate sub ###########
-- @session:id=1&user=acc1:admin1&password=test123
create database syssub1dup from sys publication pub1;
-- @session


########### pub part of the tables ###########
create publication pub_part_tbls database db1 table t1 account acc1;
-- @session:id=1&user=acc1:admin1&password=test123
-- @ignore:5,7
show subscriptions all;
create database sys_sub_part_tbls from sys publication pub_part_tbls;
-- @ignore:5,7
show subscriptions;

show full tables from sys_sub_part_tbls;
-- @ignore:10
show table status from sys_sub_part_tbls;
desc sys_sub_part_tbls.t1;
show create table sys_sub_part_tbls.t1;
select * from sys_sub_part_tbls.t1;
desc sys_sub_part_tbls.t2;
show create table sys_sub_part_tbls.t2;
select * from sys_sub_part_tbls.t2;

use sys_sub_part_tbls;
show full tables;
-- @ignore:10
show table status;
desc t1;
show create table t1;
select * from t1;
desc t2;
show create table t2;
select * from t2;
-- @session


########### normal tenant pub all ###########
# acc1 create publication to all accounts, should be failed
-- @session:id=1&user=acc1:admin1&password=test123
create database db1;
create publication pub_all database db1 account all;
-- @session


# reset
drop publication pub1;
drop publication pub_all;
drop publication pub_part_tbls;
drop database db1;
drop database db2;


########### remain a pub when drop account ###########
-- @session:id=1&user=acc1:admin1&password=test123
# should be dropped when drop account, no error
create publication pub_exists_when_drop_account database db1 account acc2;
-- @session

drop account acc1;
drop account acc2;
drop account acc3;
