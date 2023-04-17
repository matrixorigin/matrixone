create account acc_idx ADMIN_NAME 'root' IDENTIFIED BY '123456';
-- @session:id=1&user=acc_idx:root&password=123456
alter user 'root' identified by '111';
create user admin_1 identified by '123456';
create user admin_2 identified by '123456';
alter user 'admin_1' identified by '111111';
alter user 'admin_2' identified by '111111';
alter user 'root' identified by '111', 'admin_1' identified by '123456';
alter user 'admin_1' identified by '123456', admin_2 identified by '123456';
alter user 'admin_3' identified by '111111';
alter user if exists 'admin_2' identified by '111111';
alter user 'root' identified by '111' LOCK;
alter user 'root' identified by '111' PASSWORD HISTORY DEFAULT;
alter user 'root' identified by '111' comment 'alter user test';
alter user 'root' identified by '111' attribute 'test';
-- @session
drop account acc_idx; 