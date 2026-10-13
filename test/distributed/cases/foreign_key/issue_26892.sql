drop database if exists issue_26892;
create database issue_26892;
use issue_26892;

create table parent (id int primary key);
insert into parent values (1);
create table child (
    id int primary key,
    parent_id int,
    constraint fk_parent foreign key (parent_id) references parent (id)
);

-- Fresh statements observe the current session value.
set foreign_key_checks = 0;
insert into child values (1, 101);
set foreign_key_checks = 1;
insert into child values (2, 102);
select id, parent_id from child order by id;
truncate table child;

-- Plain INSERT prepared while checks are disabled: OFF -> ON -> OFF.
set foreign_key_checks = 0;
prepare fk_plain_off from 'insert into child values (?, ?)';
set @id = 10, @parent_id = 110;
execute fk_plain_off using @id, @parent_id;
set foreign_key_checks = 1;
set @id = 11, @parent_id = 111;
execute fk_plain_off using @id, @parent_id;
set foreign_key_checks = 0;
set @id = 12, @parent_id = 112;
execute fk_plain_off using @id, @parent_id;
select id, parent_id from child order by id;
deallocate prepare fk_plain_off;
truncate table child;

-- Plain INSERT prepared while checks are enabled: ON -> OFF -> ON.
set foreign_key_checks = 1;
prepare fk_plain_on from 'insert into child values (?, ?)';
set @id = 20, @parent_id = 120;
execute fk_plain_on using @id, @parent_id;
set foreign_key_checks = 0;
set @id = 21, @parent_id = 121;
execute fk_plain_on using @id, @parent_id;
set foreign_key_checks = 1;
set @id = 22, @parent_id = 122;
execute fk_plain_on using @id, @parent_id;
select id, parent_id from child order by id;
deallocate prepare fk_plain_on;
truncate table child;

-- INSERT IGNORE uses the execution-time value too. With checks enabled the
-- invalid row is ignored; with checks disabled it is inserted.
set foreign_key_checks = 0;
prepare fk_ignore_off from 'insert ignore into child values (?, ?)';
set @id = 30, @parent_id = 130;
execute fk_ignore_off using @id, @parent_id;
set foreign_key_checks = 1;
set @id = 31, @parent_id = 131;
execute fk_ignore_off using @id, @parent_id;
set foreign_key_checks = 0;
set @id = 32, @parent_id = 132;
execute fk_ignore_off using @id, @parent_id;
select id, parent_id from child order by id;
deallocate prepare fk_ignore_off;
truncate table child;

set foreign_key_checks = 1;
prepare fk_ignore_on from 'insert ignore into child values (?, ?)';
set @id = 40, @parent_id = 140;
execute fk_ignore_on using @id, @parent_id;
set foreign_key_checks = 0;
set @id = 41, @parent_id = 141;
execute fk_ignore_on using @id, @parent_id;
set foreign_key_checks = 1;
set @id = 42, @parent_id = 142;
execute fk_ignore_on using @id, @parent_id;
select id, parent_id from child order by id;
deallocate prepare fk_ignore_on;
truncate table child;

-- ODKU rejection is atomic after checks are re-enabled.
insert into child values (50, 1);
set foreign_key_checks = 0;
prepare fk_odku_off from
    'insert into child values (?, ?) on duplicate key update parent_id = values(parent_id)';
set @id = 50, @parent_id = 150;
execute fk_odku_off using @id, @parent_id;
set foreign_key_checks = 1;
set @parent_id = 151;
execute fk_odku_off using @id, @parent_id;
set foreign_key_checks = 0;
set @parent_id = 152;
execute fk_odku_off using @id, @parent_id;
select id, parent_id from child order by id;
deallocate prepare fk_odku_off;
truncate table child;

insert into child values (60, 1);
set foreign_key_checks = 1;
prepare fk_odku_on from
    'insert into child values (?, ?) on duplicate key update parent_id = values(parent_id)';
set @id = 60, @parent_id = 160;
execute fk_odku_on using @id, @parent_id;
set foreign_key_checks = 0;
set @parent_id = 161;
execute fk_odku_on using @id, @parent_id;
set foreign_key_checks = 1;
set @parent_id = 162;
execute fk_odku_on using @id, @parent_id;
select id, parent_id from child order by id;
deallocate prepare fk_odku_on;

-- Without a real PK/UNIQUE key, ODKU degenerates to a plain INSERT through the
-- legacy planner. That fallback must retain the same execution-time dependency.
create table child_no_key (
    id int,
    parent_id int,
    constraint fk_no_key_parent foreign key (parent_id) references parent (id)
);

set foreign_key_checks = 0;
prepare fk_no_key_off from
    'insert into child_no_key values (?, ?) on duplicate key update parent_id = values(parent_id)';
set @id = 70, @parent_id = 170;
execute fk_no_key_off using @id, @parent_id;
set foreign_key_checks = 1;
set @id = 71, @parent_id = 171;
execute fk_no_key_off using @id, @parent_id;
set foreign_key_checks = 0;
set @id = 72, @parent_id = 172;
execute fk_no_key_off using @id, @parent_id;
select id, parent_id from child_no_key order by id;
deallocate prepare fk_no_key_off;
truncate table child_no_key;

set foreign_key_checks = 1;
prepare fk_no_key_on from
    'insert into child_no_key values (?, ?) on duplicate key update parent_id = values(parent_id)';
set @id = 80, @parent_id = 180;
execute fk_no_key_on using @id, @parent_id;
set foreign_key_checks = 0;
set @id = 81, @parent_id = 181;
execute fk_no_key_on using @id, @parent_id;
set foreign_key_checks = 1;
set @id = 82, @parent_id = 182;
execute fk_no_key_on using @id, @parent_id;
select id, parent_id from child_no_key order by id;
deallocate prepare fk_no_key_on;

set foreign_key_checks = 1;
drop database issue_26892;
