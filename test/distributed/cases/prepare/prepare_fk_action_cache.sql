-- @suit

-- @case
-- @desc: Prepared parent DML with FK actions should recompile per EXECUTE.
-- @label:bvt
drop database if exists prepare_fk_action_cache;
create database prepare_fk_action_cache;
use prepare_fk_action_cache;

create table parent_update(id int primary key, v int);
create table child_update(id int primary key, parent_id int, foreign key(parent_id) references parent_update(id) on update cascade);
insert into parent_update values (1, 10), (2, 20);
insert into child_update values (10, 1), (20, 2);
prepare stmt_update_fk from 'update parent_update set id = id + 10 where id = ?';
set @fk_id = 1;
execute stmt_update_fk using @fk_id;
set @fk_id = 2;
execute stmt_update_fk using @fk_id;
deallocate prepare stmt_update_fk;
select id, v from parent_update order by id;
select id, parent_id from child_update order by id;

create table parent_delete(id int primary key, v int);
create table child_delete(id int primary key, parent_id int, foreign key(parent_id) references parent_delete(id) on delete set null);
insert into parent_delete values (1, 10), (2, 20);
insert into child_delete values (10, 1), (20, 2);
prepare stmt_delete_fk from 'delete from parent_delete where id = ?';
set @fk_id = 1;
execute stmt_delete_fk using @fk_id;
set @fk_id = 2;
execute stmt_delete_fk using @fk_id;
deallocate prepare stmt_delete_fk;
select id, v from parent_delete order by id;
select id, parent_id from child_delete order by id;

drop database prepare_fk_action_cache;
