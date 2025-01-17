drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

-- sys creates pitr for sys, show pitr
drop pitr if exists p_01;
create pitr p_01 range 1 'd';
-- @ignore:1,2
show pitr;
-- @ignore:0,2,3,4,6,7,10,14
select * from mo_catalog.mo_pitr Where pitr_name = 'p_01';
alter pitr p_01 range 100 'd';
-- @ignore:1,2
show pitr;
-- @ignore:0,2,3,4,6,7,10,14
select * from mo_catalog.mo_pitr Where pitr_name = 'p_01';
drop pitr p_01;
-- @ignore:1,2
show pitr;
-- @ignore:0,2,3,4,6,7,10,14
select * from mo_catalog.mo_pitr Where pitr_name = 'p_01';


-- nonsys creates pitr for current account
-- @session:id=1&user=acc01:test_account&password=111
drop pitr if exists `select`;
create pitr `select` range 10 'd';
-- @ignore:1,2
show pitr;
-- @session
-- @ignore:0,2,3,4,6,7,10,14
select * from mo_catalog.mo_pitr Where pitr_name = 'select';
-- @session:id=1&user=acc01:test_account&password=111
alter pitr `select` range 30 'd';
-- @ignore:1,2
show pitr;
-- @session
-- @ignore:0,2,3,4,6,7,10,14
select * from mo_catalog.mo_pitr Where pitr_name = 'select';
-- @session:id=1&user=acc01:test_account&password=111
drop pitr `select`;
-- @session
-- @ignore:0,2,3,4,6,7,10,14
select * from mo_catalog.mo_pitr Where pitr_name = 'select';

drop account acc01;
show pitr;