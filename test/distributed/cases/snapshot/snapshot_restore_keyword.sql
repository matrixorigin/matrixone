drop account if exists `01929da4-89af-7ed8-8f32-8df9ed5ddd71`;
create account `01929da4-89af-7ed8-8f32-8df9ed5ddd71` admin_name = 'test_account' identified by '111';

drop account if exists `0192fbbb-bfb0-7d54-a2fe-b7fd26dbdb14`;
create account `0192fbbb-bfb0-7d54-a2fe-b7fd26dbdb14` admin_name = 'test_account' identified by '111';

-- @session:id=1&user=01929da4-89af-7ed8-8f32-8df9ed5ddd71:test_account&password = 111
create database if not exists `mocloud_meta`;
use `mocloud_meta`;
create table if not exists `lock` (id int, name varchar(100));
insert into `lock` values(1, 'test');
select * from `mocloud_meta`.`lock`;
-- @session



create snapshot metadb202411181350 for account `01929da4-89af-7ed8-8f32-8df9ed5ddd71`;

-- @session:id=1&user=01929da4-89af-7ed8-8f32-8df9ed5ddd71:test_account&password = 111drop database if exists `mocloud_meta`;
-- @session

restore account `01929da4-89af-7ed8-8f32-8df9ed5ddd71` from snapshot metadb202411181350 to account `0192fbbb-bfb0-7d54-a2fe-b7fd26dbdb14`;

-- @session:id=2&user=0192fbbb-bfb0-7d54-a2fe-b7fd26dbdb14:test_account&password = 111
use `mocloud_meta`;
select * from `mocloud_meta`.`lock`;
-- @session

drop snapshot if exists metadb202411181350;
drop account if exists `0192fbbb-bfb0-7d54-a2fe-b7fd26dbdb14`;
drop account if exists `01929da4-89af-7ed8-8f32-8df9ed5ddd71`;


-- db_name,table_name is keywords
drop database if exists `select`;
create database `select`;
use `select`;
drop table if exists `_binary`;
create table `_binary` (`add` int, `all` bigint, `alter` smallint, `analyze` decimal, `and` char, `as` varchar, `asc` int, `begin` float);
show create table `_binary`;

drop snapshot if exists sp01;
create snapshot sp01 for account sys;

drop table `_binary`;
restore account sys from snapshot sp01;
show databases;
use `select`;
show tables;
show create table `_binary`;

drop database `select`;
drop snapshot sp01;




-- db_name,table_name,snapshot_name is keywords
drop database if exists `column`;
create database `column`;
use `column`;
drop table if exists `current_date`;
create table `current_date` (`current_role` int, `current_time` bigint, `current_timestamp` smallint, `current_user` decimal, `database` char, `databases` varchar, `day_hour` int, `day_microsecond` float);
show create table `current_date`;
drop table `current_date`;

drop snapshot if exists `div`;
create snapshot `div` for account sys;

drop database `column`;
restore account sys from snapshot `div`;

use `column`;
show tables;
show create table `current_date`;

drop database `column`;
drop snapshot `div`;