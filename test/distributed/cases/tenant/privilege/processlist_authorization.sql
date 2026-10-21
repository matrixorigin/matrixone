drop user if exists processlist_low;
drop role if exists processlist_reader;
create role processlist_reader;
grant connect on account * to processlist_reader;
create user processlist_low identified by '111' default role processlist_reader;

-- @session:id=1&user=sys:processlist_low:processlist_reader&password=111
select count(*) as other_users from information_schema.processlist where `user` <> 'processlist_low';
select count(*) as other_users from processlist() p where `user` <> 'processlist_low';
select count(*) > 0 as own_session from information_schema.processlist where `user` = 'processlist_low';
prepare pl from 'select count(*) as other_users from information_schema.processlist where `user` <> ''processlist_low''';
execute pl;
set role processlist_reader;
execute pl;
deallocate prepare pl;
-- @session

drop user processlist_low;
drop role processlist_reader;
