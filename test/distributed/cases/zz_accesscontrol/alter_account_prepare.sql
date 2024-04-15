--prepared
drop account if exists test;
create account test ADMIN_NAME 'admin' IDENTIFIED BY '111';
PREPARE alter_ac_1 FROM alter account ? admin_name= ? IDENTIFIED BY ?;
set @a_var = 'test';
set @b_var = 'admin';
set @c_var = '222';
EXECUTE alter_ac_1 USING @a_var, @b_var, @c_var;
DEALLOCATE PREPARE alter_ac_1;
-- @session:id=2&user=test:admin&password=222
select 1;
-- @session
PREPARE alter_ac_2 FROM "alter account ? admin_name 'admin' IDENTIFIED BY ?";
set @a_var = 'test';
set @c_var = '333';
EXECUTE alter_ac_2 USING @a_var, @c_var;
DEALLOCATE PREPARE alter_ac_2;
-- @session:id=3&user=test:admin&password=333
select 1;
-- @session
PREPARE alter_ac_3 FROM "alter account test admin_name 'admin' IDENTIFIED BY ?";
set @c_var = '444';
EXECUTE alter_ac_3 USING @c_var;
DEALLOCATE PREPARE alter_ac_3;
-- @session:id=4&user=test:admin&password=444
select 1;
-- @session
drop account if exists test;
