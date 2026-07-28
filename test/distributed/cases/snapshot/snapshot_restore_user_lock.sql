drop snapshot if exists issue21685_user_lock_acc_sp;
drop snapshot if exists issue21685_admin_lock_acc_sp;
drop account if exists issue21685_restore_acc;

create account issue21685_restore_acc admin_name = 'admin' identified by '111';
-- @session:id=1&user=issue21685_restore_acc:admin&password=111
create user issue21685_restore_user identified by '111';
alter user issue21685_restore_user lock;
select user_name,status from mo_catalog.mo_user where user_name='issue21685_restore_user';
-- @session
create snapshot issue21685_user_lock_acc_sp for account issue21685_restore_acc;
-- @session:id=1&user=issue21685_restore_acc:admin&password=111
alter user issue21685_restore_user unlock;
select user_name,status from mo_catalog.mo_user where user_name='issue21685_restore_user';
-- @session
restore account issue21685_restore_acc {snapshot = "issue21685_user_lock_acc_sp"};
-- @session:id=1&user=issue21685_restore_acc:admin&password=111
select user_name,status from mo_catalog.mo_user where user_name='issue21685_restore_user';
alter user issue21685_restore_user unlock;
drop user if exists issue21685_restore_user;

alter user admin lock;
select user_name,status from mo_catalog.mo_user where user_name='admin';
-- @session
create snapshot issue21685_admin_lock_acc_sp for account issue21685_restore_acc;
-- @session:id=1&user=issue21685_restore_acc:admin&password=111
alter user admin unlock;
select user_name,status from mo_catalog.mo_user where user_name='admin';
-- @session
restore account issue21685_restore_acc {snapshot = "issue21685_admin_lock_acc_sp"};
-- @session:id=1&user=issue21685_restore_acc:admin&password=111
select user_name,status from mo_catalog.mo_user where user_name='admin';
alter user admin unlock;
select user_name,status from mo_catalog.mo_user where user_name='admin';
-- @session
drop account if exists issue21685_restore_acc;
drop snapshot if exists issue21685_user_lock_acc_sp;
drop snapshot if exists issue21685_admin_lock_acc_sp;
