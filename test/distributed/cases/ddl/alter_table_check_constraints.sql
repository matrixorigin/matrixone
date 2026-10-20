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
alter table anonymous_checks drop check anonymous_checks_chk_1;
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

-- Generic DROP sees generated UNIQUE names and ordinary-index occupancy.
create table copy_namespace(id int primary key,v int,w int,key w(v),constraint w_2 check(w>0));
insert into copy_namespace values (1,1,1);
alter table copy_namespace drop column v,add unique(w),drop constraint w_2;
insert into copy_namespace values (2,-1);
select * from copy_namespace order by id;
select index_name,non_unique from information_schema.statistics
    where table_schema=database() and table_name='copy_namespace' order by index_name;
create table primary_namespace(id int primary key,w int,constraint `PRIMARY` check(w>0));
insert into primary_namespace values (1,1);
alter table primary_namespace drop column id,drop constraint `PRIMARY`;
insert into primary_namespace values (-1);
select w from primary_namespace order by w;
create table ordered_indexes(v int,key v(v));
insert into ordered_indexes values (1);
alter table ordered_indexes add unique(v), drop constraint v_2;
alter table ordered_indexes add index(v), add unique(v), drop constraint v_3;
select index_name,non_unique from information_schema.statistics
    where table_schema=database() and table_name='ordered_indexes' order by index_name;
alter table ordered_indexes drop index v, add unique(v), drop constraint v;
insert into ordered_indexes values (1);
select v from ordered_indexes order by v;
select index_name,non_unique from information_schema.statistics
    where table_schema=database() and table_name='ordered_indexes' order by index_name;

-- Generic DROP must reject inline UNIQUE/PRIMARY ambiguity atomically.
create table inline_unique(id int primary key,v int,constraint w check(v>0));
insert into inline_unique values (1,1);
alter table inline_unique add column w int unique,drop constraint w;
select * from inline_unique order by id;
select count(*) from information_schema.table_constraints
    where table_schema=database() and table_name='inline_unique' and constraint_type='CHECK';
insert into inline_unique values (2,-1);
-- Table-level spelling has the same ambiguity; explicit CHECK is selective.
alter table inline_unique add column w int,add unique(w),drop constraint w;
alter table inline_unique add column w int unique,drop check w;
insert into inline_unique values (2,-1,2);
select * from inline_unique order by id;
select index_name,non_unique from information_schema.statistics
    where table_schema=database() and table_name='inline_unique' order by index_name;

create table inline_primary(id int,v int,constraint `PRIMARY` check(v>0));
insert into inline_primary values (1,1);
alter table inline_primary add column w int primary key default 1,drop constraint `PRIMARY`;
select * from inline_primary order by id;
select count(*) from information_schema.table_constraints
    where table_schema=database() and table_name='inline_primary' and constraint_type='CHECK';
insert into inline_primary values (2,-1);
alter table inline_primary add column w int default 1,add primary key(w),drop constraint `PRIMARY`;
alter table inline_primary add column w int primary key default 1,drop check `PRIMARY`;
insert into inline_primary values (2,-1,2);
select * from inline_primary order by id;
select column_name from information_schema.key_column_usage
    where table_schema=database() and table_name='inline_primary' and constraint_name='PRIMARY';

-- A future primary replacement must not hide an existing ambiguous PRIMARY.
create table primary_order_source(id int primary key,v int,constraint `PRIMARY` check(v>0));
insert into primary_order_source values (1,1);
alter table primary_order_source drop constraint `PRIMARY`,drop primary key,add column w int primary key default 1;
select * from primary_order_source order by id;
select count(*) from information_schema.table_constraints
    where table_schema=database() and table_name='primary_order_source' and constraint_type='CHECK';
insert into primary_order_source values (2,-1);
alter table primary_order_source drop constraint `PRIMARY`,drop primary key,add column w int default 1,add primary key(w);
-- An explicit primary drop makes the following generic CHECK drop unambiguous.
alter table primary_order_source drop primary key,drop constraint `PRIMARY`,add column w int primary key default 1;
insert into primary_order_source values (2,-1,2);
select * from primary_order_source order by id;
select column_name from information_schema.key_column_usage
    where table_schema=database() and table_name='primary_order_source' and constraint_name='PRIMARY';

-- Preserve an earlier inline primary even when a later inline primary replaces it.
create table primary_order_inline(id int,v int,constraint `PRIMARY` check(v>0));
insert into primary_order_inline values (1,1);
alter table primary_order_inline add column x int primary key default 1,drop constraint `PRIMARY`,drop primary key,add column w int primary key default 1;
select * from primary_order_inline order by id;
select count(*) from information_schema.table_constraints
    where table_schema=database() and table_name='primary_order_inline' and constraint_type='CHECK';
insert into primary_order_inline values (2,-1);
alter table primary_order_inline add column x int primary key default 1,drop constraint `PRIMARY`,drop primary key,add column w int default 1,add primary key(w);
alter table primary_order_inline add column x int primary key default 1,drop primary key,drop constraint `PRIMARY`,add column w int primary key default 1;
insert into primary_order_inline values (2,-1,2,2);
select * from primary_order_inline order by id;
select column_name from information_schema.key_column_usage
    where table_schema=database() and table_name='primary_order_inline' and constraint_name='PRIMARY';

-- Future UNIQUE removals must not hide ambiguity at an earlier generic DROP.
create table unique_order_source(id int primary key,a int,v int,unique key u(a),constraint u check(v>0));
insert into unique_order_source values (1,1,1);
alter table unique_order_source drop constraint u,drop column a;
alter table unique_order_source drop constraint u,drop index u;
select * from unique_order_source order by id;
select count(*) from information_schema.table_constraints
    where table_schema=database() and table_name='unique_order_source' and constraint_type='CHECK';
insert into unique_order_source values (2,2,-1);
-- Removing the unique column first makes the generic CHECK drop unambiguous.
alter table unique_order_source drop column a,drop constraint u;
insert into unique_order_source values (2,-1);
select * from unique_order_source order by id;
select count(*) from information_schema.statistics
    where table_schema=database() and table_name='unique_order_source' and index_name='u';

create table unique_order_inline(id int primary key,v int,constraint w check(v>0));
insert into unique_order_inline values (1,1);
alter table unique_order_inline add column w int unique,drop constraint w,drop column w;
alter table unique_order_inline add column w int unique,drop constraint w,drop index w;
select * from unique_order_inline order by id;
select count(*) from information_schema.table_constraints
    where table_schema=database() and table_name='unique_order_inline' and constraint_type='CHECK';
insert into unique_order_inline values (2,-1);
alter table unique_order_inline add column w int unique,drop column w,drop constraint w;
insert into unique_order_inline values (2,-1);
select * from unique_order_inline order by id;

-- Generic DROP cannot bypass native Unicode physical-key admission.
create table unicode_drop_admission(id int primary key,v varchar(8) collate utf8mb4_unicode_ci,n int,unique key u(v),constraint ck check(n>0));
insert into unicode_drop_admission values (1,'a',1);
alter table unicode_drop_admission drop index u;
alter table unicode_drop_admission drop constraint u;
alter table unicode_drop_admission drop constraint `PRIMARY`;
select index_name,non_unique from information_schema.statistics
    where table_schema=database() and table_name='unicode_drop_admission' order by index_name;
insert into unicode_drop_admission values (2,'b',1);
insert into unicode_drop_admission values (3,'c',-1);
-- A CHECK-only drop remains supported and retains the versioned UNIQUE key.
alter table unicode_drop_admission drop constraint ck;
insert into unicode_drop_admission values (3,'c',-1);
select * from unicode_drop_admission order by id;

-- Column generations remain distinct when a renamed column name is reused.
create table identity_rename(id int primary key,a int,v int,unique key u(a),constraint u check(v>0));
insert into identity_rename values (1,1,1);
alter table identity_rename rename column a to x,add column a int,drop column a,drop constraint u;
select * from identity_rename order by id;
select count(*) from information_schema.table_constraints
    where table_schema=database() and table_name='identity_rename' and constraint_type='CHECK';
select column_name from information_schema.statistics
    where table_schema=database() and table_name='identity_rename' and index_name='u';
insert into identity_rename values (2,2,-1);
alter table identity_rename rename column a to x,add column a int,drop column a,drop check u;
select * from identity_rename order by id;
select column_name from information_schema.statistics
    where table_schema=database() and table_name='identity_rename' and index_name='u';
insert into identity_rename values (2,2,-1);
insert into identity_rename values (3,2,1);
select * from identity_rename order by id;

create table identity_change(id int primary key,a int,v int,unique key u(a),constraint u check(v>0));
insert into identity_change values (1,1,1);
alter table identity_change change column a x int,add column a int,drop column a,drop constraint u;
select * from identity_change order by id;
select count(*) from information_schema.table_constraints
    where table_schema=database() and table_name='identity_change' and constraint_type='CHECK';
select column_name from information_schema.statistics
    where table_schema=database() and table_name='identity_change' and index_name='u';
insert into identity_change values (2,2,-1);
alter table identity_change change column a x int,add column a int,drop column a,drop check u;
select * from identity_change order by id;
select column_name from information_schema.statistics
    where table_schema=database() and table_name='identity_change' and index_name='u';
insert into identity_change values (2,2,-1);
insert into identity_change values (3,2,1);
select * from identity_change order by id;

drop database alter_check_constraints;
