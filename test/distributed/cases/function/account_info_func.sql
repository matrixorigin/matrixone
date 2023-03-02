select current_account_name();
select current_account_id();
select current_user_name();
select current_user_id();
select current_role_name();
select current_role_id();
select * from current_account() as t;

drop account if exists abc;
create account abc ADMIN_NAME 'admin' IDENTIFIED BY '123456';
-- @session:id=1&user=abc:admin&password=123456
drop role if exists test_role;
create role test_role;
grant test_role to admin;
set role test_role;

select current_account_name();
-- account_id is auto_increment column, execute current_account_id() function return result error;
-- select current_account_id();
select current_user_name();
select current_user_id();
select current_role_name();
-- select * from current_account() as t;
-- @session
drop account if exists abc;