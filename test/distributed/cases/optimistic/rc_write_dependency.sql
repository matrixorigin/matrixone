-- RC read/transfer snapshot advancement must retain old UPDATE dependencies.
drop database if exists rc_write_dependency;
create database rc_write_dependency;
use rc_write_dependency;
create table inventory(account_id int unsigned,database_id bigint unsigned,epoch bigint unsigned,primary key(account_id,database_id));
insert into inventory values(0,42,1);
set session transaction isolation level read committed;
begin;
update inventory set epoch=epoch+1 where account_id=0 and database_id=42 and epoch=1;
-- @session:id=1{
use rc_write_dependency;
set session transaction isolation level read committed;
begin;
update inventory set epoch=epoch+1 where account_id=0 and database_id=42 and epoch=1;
-- @session}
commit;
-- @session:id=1{
-- A statement advance must validate the preceding write even before COMMIT.
-- @regex("^w-w conflict: tableID: [0-9]+, optimistic RC snapshot advance$", true)
select epoch from inventory;
rollback;
-- @session}
select account_id,database_id,epoch from inventory;
set session transaction isolation level repeatable read;
drop database rc_write_dependency;
