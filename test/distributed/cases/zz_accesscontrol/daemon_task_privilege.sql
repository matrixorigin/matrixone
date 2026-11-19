-- Daemon task management requires an active administrator role. Use an
-- impossible task ID so this case never pauses or cancels service daemons.
drop user if exists daemon_acl_public;
create user daemon_acl_public identified by '111';

-- @session:id=1&user=sys:daemon_acl_public:public&password=111
pause daemon task 0;
resume daemon task 0;
cancel daemon task 0;
-- @session

-- Authorized callers reach task lookup rather than privilege denial.
pause daemon task 0;
resume daemon task 0;
cancel daemon task 0;
drop user daemon_acl_public;
