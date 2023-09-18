-- prepare account
create account if not exists `sql_statement_hotspot` ADMIN_NAME 'admin' IDENTIFIED BY '123456';

-- @session:id=1&user=sql_statement_hotspot:admin:accountadmin&password=123456
-- check no sql_statement_hotspot in normal account
desc system.sql_statement_hotspot;
-- @session

desc system.sql_statement_hotspot;

-- clean
drop account `sql_statement_hotspot`;
