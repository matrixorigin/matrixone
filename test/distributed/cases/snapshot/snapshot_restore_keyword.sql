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
