-- Issue #30260: ADD FOREIGN KEY validates and publishes the final table identity.
drop database if exists fk_alter_rename;
create database fk_alter_rename;
use fk_alter_rename;
create table p(id int primary key);
insert into p values(1);

-- Both clause orders preserve rows and publish the new child name.
create table child_before(id int primary key, v int);
insert into child_before values(1,1);
alter table child_before rename to renamed_before,
    add constraint fk_before foreign key(v) references p(id);
select id,v from renamed_before;
create table child_after(id int primary key, v int);
insert into child_after values(1,1);
alter table child_after add constraint fk_after foreign key(v) references p(id),
    rename to renamed_after;
select id,v from renamed_after;
select constraint_name,table_name,refer_table_name from mo_catalog.mo_foreign_keys
    where db_name='fk_alter_rename' order by constraint_name;
select count(*) from information_schema.tables
    where table_schema='fk_alter_rename' and table_name in ('child_before','child_after');
-- Reject a subsequent row that has no parent.
insert into renamed_after values(2,999);

-- Validation failure rolls back both the rename and the foreign key.
create table invalid_child(id int primary key, v int);
insert into invalid_child values(1,999);
--ERROR 1452 (23000): Cannot add or update a child row: a foreign key constraint fails
alter table invalid_child rename to invalid_new,
    add constraint fk_invalid foreign key(v) references p(id);
select id,v from invalid_child;
select count(*) from information_schema.tables
    where table_schema='fk_alter_rename' and table_name='invalid_new';
select count(*) from mo_catalog.mo_foreign_keys
    where db_name='fk_alter_rename' and constraint_name='fk_invalid';
alter table renamed_before drop foreign key fk_before;
alter table renamed_after drop foreign key fk_after;
select count(*) from mo_catalog.mo_foreign_keys where db_name='fk_alter_rename';
drop database fk_alter_rename;
