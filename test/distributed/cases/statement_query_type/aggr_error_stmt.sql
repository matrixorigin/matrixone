-- prepare
drop account if exists bvt_aggr_error_stmt;
create account if not exists `bvt_aggr_error_stmt` ADMIN_NAME 'admin' IDENTIFIED BY '123456';

-- Login bvt_aggr_error_stmt account
-- @session:id=1&user=bvt_aggr_error_stmt:admin:accountadmin&password=123456

/* cloud_nonuser */ select * from system.statement_not_exist;
/* cloud_nonuser */ select * from system.statement_not_exist;
/* cloud_nonuser */ select * from system.statement_not_exist;

/* cloud_nonuser */ select * from system.statement_not_exist_2;
/* cloud_nonuser */ select * from system.statement_not_exist_2;
/* cloud_nonuser */ select * from system.statement_not_exist_2;

/* cloud_nonuser */ select * from system.statement_not_exist;
/* cloud_nonuser */ select * from system.statement_not_exist;
/* cloud_nonuser */ select * from system.statement_not_exist;

/* cloud_nonuser */ select * from system.statement_not_exist_3;
/* cloud_nonuser */ select * from system.statement_not_exist_3;
/* cloud_nonuser */ select * from system.statement_not_exist_3;

/* cloud_nonuser */ select * from system.statement_not_exist;
/* cloud_nonuser */ select * from system.statement_not_exist;
/* cloud_nonuser */ select * from system.statement_not_exist;

-- @session
-- END

-- cleanup
drop account if exists bvt_aggr_error_stmt;
