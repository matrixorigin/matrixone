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
alter user root identified by 'UI235_ace';
-- @session:id=2&user=sys:root&password=UI235_ace
select user_name,status from mo_catalog.mo_user where user_name="root";
-- @session
create user  efg identified by '111';
alter user `efg` identified by 'eee中文';
-- @session:id=3&user=sys:efg:public&password=eee中文
show databases;
-- @session
create user hjk identified by '123456';
alter user hjk identified by 'abcddddddfsfafaffsefsfsefljofiseosfjosisssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss244kkkkkkkkkkkkkkkkkkkkkkkkkkkkkk';
-- @session:id=4&user=sys:hjk&password=abcddddddfsfafaffsefsfsefljofiseosfjosisssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss244kkkkkkkkkkkkkkkkkkkkkkkkkkkkkk
show databases;
-- @session

--anormal test
alter user hjk identified by '';
alter user hjk identified by uuu;
alter user aaa identified by '111';

--privs
create user opp identified by '12345';
create role if not exists role1;
grant CONNECT,create user,drop user on account * to role1;
grant role1 to opp;
-- @session:id=5&user=sys:opp:role1&password=12345
create user rst identified by '12345';
alter user rst identified by 'Nm_092324';
drop user rst;
-- @session
drop user if exists efg;
drop user if exists hjk;
drop user if exists opp;
drop role if exists role1;
alter user root identified by '111';
