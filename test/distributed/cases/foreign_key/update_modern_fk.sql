drop database if exists update_modern_fk;
create database update_modern_fk;
use update_modern_fk;

create table parent_single (
    id int primary key
);

create table child_single (
    id int primary key,
    parent_id int,
    note varchar(32),
    unique key uk_note(note),
    constraint fk_single foreign key (parent_id) references parent_single(id)
);

insert into parent_single values (1), (2), (3);
insert into child_single values (10, 1, 'a'), (20, 2, null);

update child_single set note = 'b' where id = 10;
insert into child_single values (30, 1, 'a');
delete from child_single where id = 30;
update child_single set parent_id = 3 where id = 10;
select * from child_single order by id;

update child_single set parent_id = 99 where id = 10;
select * from child_single order by id;

set foreign_key_checks = 0;
update child_single set parent_id = 99 where id = 10;
set foreign_key_checks = 1;
update child_single set parent_id = parent_id where id = 10;
update child_single set note = 'orphan' where id = 10;
insert into child_single values (30, 1, 'orphan');
select * from child_single order by id;

update child_single set parent_id = 1 where id = 10;

set foreign_key_checks = 0;
prepare fk_enable_checks from
    'update child_single set parent_id = ? where id = ?';
set @fk_parent_id = 98;
set @fk_child_id = 10;
set foreign_key_checks = 1;
execute fk_enable_checks using @fk_parent_id, @fk_child_id;
select * from child_single where id = 10;
deallocate prepare fk_enable_checks;

prepare fk_disable_checks from
    'update child_single set parent_id = ? where id = ?';
set foreign_key_checks = 0;
execute fk_disable_checks using @fk_parent_id, @fk_child_id;
set foreign_key_checks = 1;
select * from child_single where id = 10;
deallocate prepare fk_disable_checks;
update child_single set parent_id = 1 where id = 10;

update child_single set parent_id = case id when 10 then 2 else 98 end;
select * from child_single order by id;

create table parent_composite (
    a int,
    b int,
    primary key (a, b)
);

create table child_composite (
    id int primary key,
    a int,
    b int,
    constraint fk_composite foreign key (a, b) references parent_composite(a, b)
);

insert into parent_composite values (1, 1), (2, 2);
insert into child_composite values (1, 1, 1), (2, 2, 2), (3, null, null);

update child_composite set a = null, b = 99 where id = 1;
update child_composite set a = 99, b = null where id = 2;
update child_composite set a = 1, b = 1 where id = 3;
select * from child_composite order by id;

update child_composite set a = 98, b = 98 where id = 3;
select * from child_composite order by id;

create table parent_auto_fk (
    id int primary key
);

create table child_auto_fk (
    parent_id int auto_increment unique,
    constraint fk_auto foreign key (parent_id) references parent_auto_fk(id)
);

insert into parent_auto_fk values (1);
insert into child_auto_fk values (0);
update child_auto_fk
set parent_id = if(parent_id = 1, null, parent_id);
select * from child_auto_fk;

create table self_ref (
    id int primary key,
    parent_id int,
    name varchar(32),
    constraint fk_self foreign key (parent_id) references self_ref(id)
);

insert into self_ref values (1, 1, 'root'), (2, 1, 'child');
update self_ref set name = 'changed' where id = 2;
update self_ref set parent_id = 2 where id = 2;
update self_ref set parent_id = 99 where id = 2;
select * from self_ref order by id;

drop table self_ref;
drop table child_auto_fk;
drop table parent_auto_fk;
drop table child_composite;
drop table parent_composite;
drop table child_single;
drop table parent_single;
drop database update_modern_fk;
