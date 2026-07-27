-- @suit

-- @case
-- @desc: The global prepared statement limit is shared by sessions in one account.
-- @label:bvt
drop account if exists prepared_stmt_quota_acc;
create account prepared_stmt_quota_acc admin_name 'admin' identified by '111';

-- @session:id=1&user=prepared_stmt_quota_acc:admin&password=111
set global max_prepared_stmt_count = 2;
select @@global.max_prepared_stmt_count;
prepare quota_stmt_1 from 'select 1';
-- @session

-- @session:id=2&user=prepared_stmt_quota_acc:admin&password=111
prepare quota_stmt_2 from 'select 2';
-- @session

-- @session:id=3&user=prepared_stmt_quota_acc:admin&password=111
prepare quota_stmt_3 from 'select 3';
-- @session

-- @session:id=1&user=prepared_stmt_quota_acc:admin&password=111
deallocate prepare quota_stmt_1;
-- @session

-- @session:id=3&user=prepared_stmt_quota_acc:admin&password=111
prepare quota_stmt_3 from 'select 3';
execute quota_stmt_3;
deallocate prepare quota_stmt_3;
-- @session

-- @session:id=2&user=prepared_stmt_quota_acc:admin&password=111
deallocate prepare quota_stmt_2;
-- @session

-- @session:id=1&user=prepared_stmt_quota_acc:admin&password=111
set global max_prepared_stmt_count = 0;
prepare quota_stmt_disabled from 'select 4';
set global max_prepared_stmt_count = 16382;
-- @session

drop account prepared_stmt_quota_acc;
