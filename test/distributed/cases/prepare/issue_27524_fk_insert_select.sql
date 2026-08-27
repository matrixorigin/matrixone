-- @suit
-- @case
-- @desc: Issue #27524: 2CN+Proxy INSERT ... SELECT preserves the FK error,
--        rolls back the target atomically, and keeps the connection usable.
-- @label:bvt
drop database if exists issue_27524_fk_insert_select;
create database issue_27524_fk_insert_select;
use issue_27524_fk_insert_select;

create table parent(id int primary key);
create table child(
    id int primary key,
    parent_id int,
    constraint child_parent foreign key(parent_id) references parent(id)
);
insert into parent values (1);

insert into child select 10, id + 1 from parent;
select count(*) from child;
select 1 as reusable_connection;

drop database issue_27524_fk_insert_select;
