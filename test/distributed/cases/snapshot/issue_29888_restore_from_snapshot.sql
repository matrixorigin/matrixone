drop snapshot if exists issue_29888_s;
drop database if exists issue_29888_restore;
create database issue_29888_restore;
create table issue_29888_restore.t(id int primary key, v varchar(10));
insert into issue_29888_restore.t values (7, 'before');
create snapshot issue_29888_s for database issue_29888_restore;
update issue_29888_restore.t set v = 'after' where id = 7;
-- @regex("FROM SNAPSHOT is not supported; use RESTORE DATABASE", true)
restore database issue_29888_restore from snapshot issue_29888_s;
-- @regex("FROM SNAPSHOT is not supported; use RESTORE DATABASE", true)
restore account sys database issue_29888_restore from snapshot issue_29888_s;
restore database sys.issue_29888_restore{snapshot="issue_29888_s"};
select * from issue_29888_restore.t order by id;
drop snapshot issue_29888_s;
drop database issue_29888_restore;
