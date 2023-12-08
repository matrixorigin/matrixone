drop table if exists t1;
create table t1 (a int primary key, b int);
begin;
alter table t1 modify b bigint;
-- @session:id=1{
use `prepare`;
prepare stmt1 from select * from t1 where a > ? for update;
set @a=1;
-- @wait:0:commit
execute stmt1 using @a;
-- @session}
commit;