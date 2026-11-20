-- Prepared SHOW must use the current subscription state on every execution.
drop account if exists ps_show_subscriber;
create account ps_show_subscriber admin_name = 'admin' identified by '111';
drop database if exists ps_show_source;
create database ps_show_source;
create table ps_show_source.a (id int);
create table ps_show_source.b (id int);
create publication ps_show_pub database ps_show_source account ps_show_subscriber;

-- @session:id=1&user=ps_show_subscriber:admin&password=111
create database ps_show_sub from sys publication ps_show_pub;
prepare ps_tables from 'show tables from ps_show_sub';
execute ps_tables;
-- @session

alter publication ps_show_pub account sys;

-- @session:id=1&user=ps_show_subscriber:admin&password=111
execute ps_tables;
show tables from ps_show_sub;
-- @session

alter publication ps_show_pub account ps_show_subscriber;

-- @session:id=1&user=ps_show_subscriber:admin&password=111
execute ps_tables;
-- @session

alter publication ps_show_pub account ps_show_subscriber database ps_show_source table a;

-- @session:id=1&user=ps_show_subscriber:admin&password=111
execute ps_tables;
deallocate prepare ps_tables;
drop database ps_show_sub;
-- @session

drop publication ps_show_pub;
drop database ps_show_source;
drop account ps_show_subscriber;
