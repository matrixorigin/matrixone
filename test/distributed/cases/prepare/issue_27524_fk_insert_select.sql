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

insert into parent select result from generate_series(2, 9999) g;

insert into child select result, result from generate_series(1, 10000) g;
select count(*) from child;
insert into child select result, result from generate_series(1, 10000) g;
select count(*) from child;
insert into child select result, result from generate_series(1, 10000) g;
select count(*) from child;
insert into child select result, result from generate_series(1, 10000) g;
select count(*) from child;
insert into child select result, result from generate_series(1, 10000) g;
select count(*) from child;
insert into child values (1, 1);
select count(*) from child;
select count(*) from parent;

drop database issue_27524_fk_insert_select;
