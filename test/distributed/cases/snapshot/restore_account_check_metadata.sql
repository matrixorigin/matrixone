-- Repeated account restore must accept catalog SQL produced by an earlier clone.
drop snapshot if exists restore_check_s1;
drop snapshot if exists restore_check_s2;
drop account if exists restore_check;
drop account if exists restore_check_copy;
create account restore_check admin_name 'admin' identified by '111';
create account restore_check_copy admin_name 'admin' identified by '111';

-- @session:id=1&user=restore_check:admin&password=111
create database app;
create table app.checkpoint (id int primary key);
insert into app.checkpoint values (1);
create table app.checked_values (id int, constraint positive_id check (id > 0));
insert into app.checked_values values (1);
-- @session

create snapshot restore_check_s1 for account restore_check;
restore account restore_check {snapshot = 'restore_check_s1'};
create snapshot restore_check_s2 for account restore_check;

-- @session:id=2&user=restore_check:admin&password=111
insert into app.checkpoint values (2);
select * from app.checkpoint order by id;
insert into app.checked_values values (-1);
-- @session

restore account restore_check {snapshot = 'restore_check_s2'};
-- @session:id=3&user=restore_check:admin&password=111
select * from app.checkpoint order by id;
select * from app.checked_values;
insert into app.checked_values values (-1);
create table app.checkpoint_like like app.checkpoint;
insert into app.checkpoint_like values (3);
select * from app.checkpoint_like;
create table app.checked_like like app.checked_values;
insert into app.checked_like values (-1);
-- @session

restore account restore_check {snapshot = 'restore_check_s2'} to account restore_check_copy;
-- @session:id=4&user=restore_check_copy:admin&password=111
select * from app.checkpoint;
select * from app.checked_values;
insert into app.checked_values values (-1);
-- @session
drop account restore_check_copy;
drop snapshot restore_check_s2;
drop snapshot restore_check_s1;
drop account restore_check;
