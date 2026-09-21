-- Issue #28995: COPY ALTER supports adding columns and foreign keys atomically.

drop database if exists issue_28995;
create database issue_28995;
use issue_28995;

create table parent_t (
    id int primary key
);
insert into parent_t values (1), (2);

-- The application migration shape: the new nullable column and its foreign
-- key are introduced by one COPY ALTER.
create table child_t (
    id int primary key
);
insert into child_t values (1);
alter table child_t
    add column parent_id int null,
    add constraint fk_child_parent foreign key (parent_id) references parent_t(id);
update child_t set parent_id = 1 where id = 1;
select id, parent_id from child_t order by id;
select count(*) from information_schema.key_column_usage
    where constraint_schema = 'issue_28995'
      and table_name = 'child_t'
      and constraint_name = 'fk_child_parent'
      and referenced_table_name = 'parent_t';
--ERROR 1452 (23000): Cannot add or update a child row: a foreign key constraint fails
insert into child_t values (2, 999);

-- Clause order does not change the final-schema semantics.
create table reverse_child_t (
    id int primary key
);
alter table reverse_child_t
    add constraint fk_reverse_parent foreign key (parent_id) references parent_t(id),
    add column parent_id int null;
insert into reverse_child_t values (1, 2);
--ERROR 1452 (23000): Cannot add or update a child row: a foreign key constraint fails
insert into reverse_child_t values (2, 999);

-- Existing foreign keys survive the replacement and are merged with the new
-- constraint instead of being replaced by the planned temporary definition.
create table preserved_child_t (
    id int primary key,
    existing_parent_id int,
    constraint fk_preserved_existing foreign key (existing_parent_id) references parent_t(id)
);
insert into preserved_child_t values (1, 1);
alter table preserved_child_t
    add column new_parent_id int null,
    add constraint fk_preserved_new foreign key (new_parent_id) references parent_t(id);
update preserved_child_t set new_parent_id = 2 where id = 1;
select count(*) from information_schema.key_column_usage
    where constraint_schema = 'issue_28995'
      and table_name = 'preserved_child_t'
      and referenced_table_name = 'parent_t';
--ERROR 1452 (23000): Cannot add or update a child row: a foreign key constraint fails
insert into preserved_child_t values (2, 999, 1);
--ERROR 1452 (23000): Cannot add or update a child row: a foreign key constraint fails
insert into preserved_child_t values (3, 1, 999);

-- Existing rows are validated after new-column defaults are materialized. A
-- failed constraint leaves neither the column nor the foreign key behind.
create table invalid_child_t (
    id int primary key
);
insert into invalid_child_t values (1);
--ERROR 1452 (23000): Cannot add or update a child row: a foreign key constraint fails
alter table invalid_child_t
    add column parent_id int not null default 999,
    add constraint fk_invalid_parent foreign key (parent_id) references parent_t(id);
select count(*) from information_schema.columns
    where table_schema = 'issue_28995'
      and table_name = 'invalid_child_t'
      and column_name = 'parent_id';
select count(*) from information_schema.key_column_usage
    where constraint_schema = 'issue_28995'
      and table_name = 'invalid_child_t'
      and constraint_name = 'fk_invalid_parent';
select * from invalid_child_t order by id;

-- Self references use the replacement relation's physical column IDs.
create table self_child_t (
    id int primary key
);
insert into self_child_t values (1);
alter table self_child_t
    add column parent_id int null,
    add constraint fk_self_parent foreign key (parent_id) references self_child_t(id);
update self_child_t set parent_id = 1 where id = 1;
select id, parent_id from self_child_t order by id;
--ERROR 1452 (23000): Cannot add or update a child row: a foreign key constraint fails
insert into self_child_t values (2, 999);

-- Multiple newly added columns retain distinct planner identities while a
-- composite constraint is rendered and materialized.
create table composite_parent_t (
    a int,
    b int,
    primary key (a, b)
);
insert into composite_parent_t values (1, 2);
create table composite_child_t (
    id int primary key
);
alter table composite_child_t
    add column parent_a int null,
    add column parent_b int null,
    add constraint fk_composite_parent foreign key (parent_a, parent_b)
        references composite_parent_t(a, b);
insert into composite_child_t values (1, 1, 2);
select id, parent_a, parent_b from composite_child_t order by id;
--ERROR 1452 (23000): Cannot add or update a child row: a foreign key constraint fails
insert into composite_child_t values (2, 1, 999);

-- FK validation SQL must quote decoded database, table, and column names. Both
-- external and self references exercise the generated post-copy detection SQL.
create table `parent``t` (
    `id``key` int primary key
);
insert into `parent``t` values (1);
create table escaped_external_child (
    `id``key` int primary key
);
insert into escaped_external_child values (1);
alter table escaped_external_child
    add column `parent``id` int not null default 1,
    add constraint `fk``external` foreign key (`parent``id`)
        references `parent``t`(`id``key`);
select count(*) from escaped_external_child
    where `id``key` = 1 and `parent``id` = 1;

create table escaped_self_child (
    `id``key` int primary key
);
insert into escaped_self_child values (1);
alter table escaped_self_child
    add column `parent``id` int not null default 1,
    add constraint `fk``self` foreign key (`parent``id`)
        references escaped_self_child(`id``key`);
select count(*) from escaped_self_child
    where `id``key` = 1 and `parent``id` = 1;

-- Parent reverse-reference metadata is published with the new constraint.
--ERROR 3730 (HY000): Cannot drop table 'parent_t' referenced by a foreign key constraint 'fk_child_parent' on table 'child_t'.
drop table parent_t;

drop table composite_child_t;
drop table self_child_t;
drop table invalid_child_t;
drop table reverse_child_t;
drop table preserved_child_t;
drop table child_t;
drop table composite_parent_t;
drop table parent_t;
drop database issue_28995;
