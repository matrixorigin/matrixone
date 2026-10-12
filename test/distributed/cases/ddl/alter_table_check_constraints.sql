-- @suit
-- @case
-- @desc: ALTER ADD/DROP CHECK validates rows atomically and preserves other constraints (#25387)
-- @label:bvt

drop database if exists alter_check_constraints;
create database alter_check_constraints;
use alter_check_constraints;

-- Existing rows, NULL truth values, and incoming/outgoing FK/index ownership.
create table parent_rows(id int primary key);
insert into parent_rows values (1);
create table checked_rows (
    id int primary key,
    pid int,
    v int,
    unique key uv(v),
    key idx_pid(pid),
    constraint fk_parent foreign key(pid) references parent_rows(id),
    constraint ck_limit check(v <= 100)
);
insert into checked_rows values (1,1,1),(2,null,null);
create table referring_rows(id int primary key, rid int,
    constraint fk_checked foreign key(rid) references checked_rows(id));
insert into referring_rows values (1,1);
alter table checked_rows add constraint ck_positive check(v > 0);
select id,pid,v from checked_rows order by id;
select constraint_name,check_clause from information_schema.check_constraints
    where constraint_schema=database() order by constraint_name;
insert into checked_rows values (3,1,2),(4,1,-1);
update checked_rows set v=-1 where id=1;
select id,pid,v from checked_rows order by id;
insert into checked_rows values (3,999,2);
insert into checked_rows values (3,1,1);
delete from checked_rows where id=1;
delete from parent_rows where id=1;

-- Selective DROP keeps the other CHECK, unique index, and FK relationships.
alter table checked_rows drop check ck_positive;
insert into checked_rows values (3,1,-1);
insert into checked_rows values (4,1,101);
select id,pid,v from checked_rows order by id;
select constraint_name from information_schema.check_constraints
    where constraint_schema=database() order by constraint_name;

-- ADD rejects existing violations without publishing either new constraint.
alter table checked_rows add constraint ck_nonzero check(v <> 0),
    add constraint ck_positive check(v > 0);
select id,pid,v from checked_rows order by id;
select constraint_name from information_schema.check_constraints
    where constraint_schema=database() order by constraint_name;
insert into checked_rows values (4,999,2);
insert into checked_rows values (4,1,1);
delete from checked_rows where id=1;
delete from parent_rows where id=1;
delete from checked_rows where id=3;
alter table checked_rows add constraint ck_positive check(v > 0);

-- Replace a CHECK in one statement and reject an ambiguous generic name.
alter table checked_rows drop constraint ck_positive,
    add constraint ck_positive check(v >= -10);
insert into checked_rows values (3,1,-1);
insert into checked_rows values (4,1,-11);
alter table checked_rows add constraint uv check(v <> 0);
alter table checked_rows drop constraint uv;
alter table checked_rows drop check uv;
insert into checked_rows values (4,1,1);
alter table checked_rows drop foreign key ck_limit;
alter table checked_rows drop constraint ck_limit;
insert into checked_rows values (4,1,101);
select id,pid,v from checked_rows order by id;
select constraint_name,check_clause from information_schema.check_constraints
    where constraint_schema=database() order by constraint_name;
select index_name,non_unique,column_name from information_schema.statistics
    where table_schema=database() and table_name='checked_rows'
    order by index_name,seq_in_index;
insert into checked_rows values (5,999,2);
delete from checked_rows where id=1;
delete from parent_rows where id=1;

-- DROP of the final CHECK must not replay the legacy CREATE SQL fallback.
create table last_check(id int primary key,v int,constraint ck_only check(v>=0));
insert into last_check values (1,1);
alter table last_check drop constraint ck_only;
insert into last_check values (2,-1);
select * from last_check order by id;
select count(*) from information_schema.check_constraints
    where constraint_schema=database() and constraint_name='ck_only';
alter table last_check drop check absent;
select * from last_check order by id;

-- Anonymous constraints skip occupied names after a drop.
create table anonymous_checks(v int,check(v>=0),check(v<=100));
alter table anonymous_checks drop check __mo_chk_1;
alter table anonymous_checks add check(v > -10);
select constraint_name from information_schema.table_constraints
    where table_schema=database() and table_name='anonymous_checks' and constraint_type='CHECK'
    order by constraint_name;
insert into anonymous_checks values (-11);
insert into anonymous_checks values (101);

-- Bind after all column changes, including generated columns.
create table final_columns(id int primary key);
insert into final_columns values (1);
alter table final_columns add constraint ck_added check(w>0), add column w int default 1;
insert into final_columns values (2,0);
alter table final_columns add column g int generated always as (w+1) stored,
    add constraint ck_generated check(g<5);
update final_columns set w=5 where id=1;
select * from final_columns order by id;
-- A generated-column dependency rejects the entire combined ALTER.
alter table final_columns drop check ck_added, drop column w;
insert into final_columns(id,w) values (2,0);
select * from final_columns order by id;

-- Removing a CHECK and its referenced column works in either clause order.
create table drop_columns(id int primary key,a int,b int,constraint ck_sum check(a+b>0));
insert into drop_columns values (1,1,1);
alter table drop_columns drop column a, drop check ck_sum;
select * from drop_columns order by id;

-- MLflow's migration adds an unnamed CHECK to an existing status column.
create table runs(id int primary key,status varchar(20));
insert into runs values (1,'RUNNING');
alter table runs add check(status in ('SCHEDULED','FAILED','FINISHED','RUNNING','KILLED'));
insert into runs values (2,'INVALID');
update runs set status='FINISHED' where id=1;
select * from runs order by id;
alter table runs add check(status <> '') not enforced;
alter table runs add check(status <> ''), algorithm=inplace;
select count(*) from information_schema.check_constraints
    where constraint_schema=database();

drop database alter_check_constraints;
