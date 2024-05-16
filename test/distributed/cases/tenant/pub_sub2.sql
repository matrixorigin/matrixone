set global enable_privilege_cache = off;
create account a1 ADMIN_NAME 'admin1' IDENTIFIED BY 'test123';
create account a2 ADMIN_NAME 'admin2' IDENTIFIED BY 'test456';

create database sub1;
create table sub1.t1 (a int,b int);
insert into sub1.t1 values (1, 1), (2, 2), (3, 3);

create publication pub1 database sub1;
create publication pub3 database sub1;
-- @ignore:2,3
show publications;
-- @ignore:2,3
show publications like 'pub%';
-- @ignore:2,3
show publications like '%1';

create database sub2;
create table sub2.t1 (a float);

create publication pub2 database sub2 account a1;
-- @ignore:2,3
show publications;
-- @ignore:2,3
show publications like 'pub%';
-- @ignore:2,3
show publications like '%1';

-- @session:id=1&user=a1:admin1&password=test123
-- @ignore:3,5
show subscriptions;
-- @ignore:3,5
show subscriptions all;
-- @ignore:3,5
show subscriptions all like '%1';

create database syssub1 from sys publication pub1;
-- @ignore:3,5
show subscriptions;
-- @ignore:3,5
show subscriptions all;

use syssub1;
show tables;
-- @ignore:3,5
show subscriptions all;
-- @ignore:10
show table status;
-- @ignore:10
show table status like 't1';
desc t1;
show create table t1;

select * from t1;
-- @session

-- @session:id=2&user=a2:admin2&password=test456
-- @ignore:3,5
show subscriptions all;
-- @session

alter publication pub2 account all;
-- @ignore:2,3
show publications;

-- @session:id=2&user=a2:admin2&password=test456
-- @ignore:3,5
show subscriptions all;
-- @session

-- alter db
alter publication pub1 database sub2;
-- @ignore:2,3
show publications;

-- @session:id=1&user=a1:admin1&password=test123
-- @ignore:3,5
show subscriptions;
use syssub1;
show tables;
select * from t1;
-- @session

-- @session:id=1&user=a1:admin1&password=test123
drop database syssub1;
-- @session

drop publication pub1;
drop publication pub2;
drop publication pub3;
drop database sub1;
drop database sub2;
drop account a1;
drop account a2;