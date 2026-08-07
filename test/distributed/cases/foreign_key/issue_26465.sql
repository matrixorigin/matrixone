-- Issue #26465: ALTER COPY must retain all foreign-key relationships.
--
-- The production schema has hundreds of indexes and foreign keys.  Keep this
-- BVT representative but compact; the exact production cardinalities belong
-- in the Go benchmark because they would make every normal BVT expensive.

drop database if exists issue_26465;
create database issue_26465;
use issue_26465;

create table departments (
    id int primary key,
    name varchar(100) not null,
    unique key uk_departments_name (name)
);

create table users (
    id int primary key,
    department_id_1 int,
    department_id_2 int,
    department_id_3 int,
    department_id_4 int,
    department_id_5 int,
    department_id_6 int,
    constraint fk_users_department_1 foreign key (department_id_1) references departments(id),
    constraint fk_users_department_2 foreign key (department_id_2) references departments(id),
    constraint fk_users_department_3 foreign key (department_id_3) references departments(id),
    constraint fk_users_department_4 foreign key (department_id_4) references departments(id),
    constraint fk_users_department_5 foreign key (department_id_5) references departments(id),
    constraint fk_users_department_6 foreign key (department_id_6) references departments(id),
    key idx_users_department_1 (department_id_1),
    key idx_users_department_2 (department_id_2),
    key idx_users_department_3 (department_id_3),
    key idx_users_department_4 (department_id_4),
    key idx_users_department_5 (department_id_5),
    key idx_users_department_6 (department_id_6)
);

insert into departments values (1, 'engineering');
insert into users values (1, 1, 1, 1, 1, 1, 1);

-- Same-name CHANGE with only a comment update takes the INPLACE path.
alter table users change department_id_1 department_id_1 int comment 'primary department';

-- ADD COLUMN takes ALTER COPY.  The copied child must remain registered on its
-- parent after the temporary table is removed.
alter table users add column copied_child_marker int;
--ERROR 3730 (HY000): Cannot drop table 'departments' referenced by a foreign key constraint 'fk_users_department_1' on table 'users'.
drop table departments;
--ERROR 1452 (23000): Cannot add or update a child row: a foreign key constraint fails
insert into users (id, department_id_1) values (2, 999);
insert into users (id, department_id_1) values (2, 1);

-- The parent also has a large RefChildTbls set in the production schema.
alter table departments change name name varchar(100) not null comment 'department name';
alter table departments add column copied_parent_marker int;
--ERROR 1452 (23000): Cannot add or update a child row: a foreign key constraint fails
insert into users (id, department_id_1) values (3, 999);
insert into users (id, department_id_1) values (3, 1);

create table jobs (id int primary key);
create table talent_pools (id int primary key);
create table owners (id int primary key);
insert into jobs values (1);
insert into talent_pools values (1);
insert into owners values (1);

create table candidates (
    id int primary key,
    job_id_1 int,
    job_id_2 int,
    talent_pool_id_1 int,
    talent_pool_id_2 int,
    owner_id_1 int,
    owner_id_2 int,
    constraint fk_candidates_job_1 foreign key (job_id_1) references jobs(id),
    constraint fk_candidates_job_2 foreign key (job_id_2) references jobs(id),
    constraint fk_candidates_talent_pool_1 foreign key (talent_pool_id_1) references talent_pools(id),
    constraint fk_candidates_talent_pool_2 foreign key (talent_pool_id_2) references talent_pools(id),
    constraint fk_candidates_owner_1 foreign key (owner_id_1) references owners(id),
    constraint fk_candidates_owner_2 foreign key (owner_id_2) references owners(id),
    key idx_candidates_job_1 (job_id_1),
    key idx_candidates_job_2 (job_id_2),
    key idx_candidates_talent_pool_1 (talent_pool_id_1),
    key idx_candidates_talent_pool_2 (talent_pool_id_2),
    key idx_candidates_owner_1 (owner_id_1),
    key idx_candidates_owner_2 (owner_id_2)
);

insert into candidates values (1, 1, 1, 1, 1, 1, 1);
alter table candidates change job_id_1 job_id_1 int comment 'primary job';
alter table candidates add column copied_child_marker int;
--ERROR 3730 (HY000): Cannot drop table 'jobs' referenced by a foreign key constraint 'fk_candidates_job_1' on table 'candidates'.
drop table jobs;
--ERROR 1452 (23000): Cannot add or update a child row: a foreign key constraint fails
insert into candidates (id, job_id_1) values (2, 999);
insert into candidates (id, job_id_1, talent_pool_id_1, owner_id_1) values (2, 1, 1, 1);

select count(*) from users;
select count(*) from candidates;
select count(*) from information_schema.key_column_usage
    where constraint_schema = 'issue_26465'
      and table_name = 'users'
      and referenced_table_name = 'departments';
select count(*) from information_schema.key_column_usage
    where constraint_schema = 'issue_26465'
      and table_name = 'candidates'
      and referenced_table_name is not null;

drop table candidates;
drop table users;
drop table departments;
drop table jobs;
drop table talent_pools;
drop table owners;
drop database issue_26465;
