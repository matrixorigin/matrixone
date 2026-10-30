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

-- Issue #30956: DROP COLUMN implicitly removes the CHECKs that reference it.
create table dc_single(id int primary key,v int,constraint ck_dcs check(v>0));
insert into dc_single values (1,10);
alter table dc_single drop column v;
select column_name from information_schema.columns
    where table_schema=database() and table_name='dc_single' order by ordinal_position;
select constraint_name from information_schema.table_constraints
    where table_schema=database() and table_name='dc_single' and constraint_type='CHECK';
select * from dc_single;

-- Dropping a column no CHECK references keeps every constraint and its enforcement.
create table dc_none(id int primary key,v int,w int,constraint ck_dcn check(w>0));
insert into dc_none values (1,10,20);
alter table dc_none drop column v;
select constraint_name,check_clause from information_schema.check_constraints
    where constraint_schema=database() and constraint_name='ck_dcn' order by constraint_name;
insert into dc_none values (2,-1);
insert into dc_none values (2,30);
select * from dc_none order by id;

-- A CHECK spanning a surviving column rejects the DROP COLUMN instead
-- (MySQL ER_DEPENDENT_BY_CHECK_CONSTRAINT 3959); an explicit DROP CHECK in the
-- same statement releases the column.
create table dc_multi(id int primary key,a int,b int,c int,constraint ck_dcm_ab check(a+b>0),constraint ck_dcm_c check(c>0));
insert into dc_multi values (1,1,1,1);
alter table dc_multi drop column b;
select column_name from information_schema.columns
    where table_schema=database() and table_name='dc_multi' order by ordinal_position;
select constraint_name,check_clause from information_schema.check_constraints
    where constraint_schema=database() and constraint_name like 'ck_dcm%' order by constraint_name;
insert into dc_multi values (2,-1,0,1);
insert into dc_multi values (2,1,0,1);
select count(*) from dc_multi;
alter table dc_multi drop check ck_dcm_ab, drop column b;
select column_name from information_schema.columns
    where table_schema=database() and table_name='dc_multi' order by ordinal_position;
select constraint_name,check_clause from information_schema.check_constraints
    where constraint_schema=database() and constraint_name like 'ck_dcm%' order by constraint_name;
insert into dc_multi values (3,1,-2);
insert into dc_multi values (3,1,2);
select * from dc_multi order by id;

-- The name of an auto-dropped CHECK is released to a same-statement
-- replacement (MySQL releases dropped_cc_names before admitting new checks);
-- the replacement binds to the post-drop schema and stays enforced.
create table dc_replace(id int primary key,v int,w int,constraint ck_dcmn check(v>0));
insert into dc_replace values (1,10,20);
alter table dc_replace drop column v, add constraint ck_dcmn check(w>0);
select column_name from information_schema.columns
    where table_schema=database() and table_name='dc_replace' order by ordinal_position;
select constraint_name,check_clause from information_schema.check_constraints
    where constraint_schema=database() and constraint_name='ck_dcmn';
insert into dc_replace values (2,-5);
insert into dc_replace values (2,30);
select * from dc_replace order by id;

-- Re-adding the dropped name in one statement does not resurrect the removed CHECK.
create table dc_readd(id int primary key,v int,constraint ck_dcr check(v>0));
insert into dc_readd values (1,10);
alter table dc_readd drop column v,add column v int default -1;
select constraint_name from information_schema.table_constraints
    where table_schema=database() and table_name='dc_readd' and constraint_type='CHECK';
insert into dc_readd values (2,default);
insert into dc_readd values (3,-5);
select * from dc_readd order by id;

-- A rejected DROP COLUMN leaves the column, its CHECKs and the rows untouched.
create table dc_atomic(id int primary key,v int,w int generated always as (v+1) stored,constraint ck_dca check(v>0));
insert into dc_atomic(id,v) values (1,10);
alter table dc_atomic drop column v;
select column_name from information_schema.columns
    where table_schema=database() and table_name='dc_atomic' order by ordinal_position;
select constraint_name from information_schema.table_constraints
    where table_schema=database() and table_name='dc_atomic' and constraint_type='CHECK';
select * from dc_atomic;

-- A CHECK follows its column through an earlier rename in the same ALTER, so
-- dropping the re-added fresh column keeps the constraint on the surviving
-- renamed column and keeps it enforced.
create table dc_identity(id int primary key,v int,constraint ck_dci check(v>0));
insert into dc_identity values (1,10);
alter table dc_identity rename column v to x, add column v int, drop column v;
select column_name from information_schema.columns
    where table_schema=database() and table_name='dc_identity' order by ordinal_position;
select constraint_name,check_clause from information_schema.check_constraints
    where constraint_schema=database() and constraint_name='ck_dci';
insert into dc_identity values (2,-1);
insert into dc_identity values (2,1);
select * from dc_identity order by id;

-- The generic DROP CONSTRAINT spelling resolves the same CHECK identity and
-- co-drops with its column in either clause order.
create table dc_generic(id int primary key,v int,constraint ck_dcg check(v>0));
insert into dc_generic values (1,10);
alter table dc_generic drop constraint ck_dcg, drop column v;
select column_name from information_schema.columns
    where table_schema=database() and table_name='dc_generic' order by ordinal_position;
select * from dc_generic;
create table dc_generic2(id int primary key,v int,constraint ck_dcg2 check(v>0));
insert into dc_generic2 values (1,10);
alter table dc_generic2 drop column v, drop constraint ck_dcg2;
select column_name from information_schema.columns
    where table_schema=database() and table_name='dc_generic2' order by ordinal_position;
select * from dc_generic2;

-- A same-statement replacement CHECK must bind its own authored expression.
-- The row violates the replacement over w, so the combined ALTER is rejected
-- atomically and the original table stays intact.
create table dc_rebind(id int primary key,v int,w int,constraint ck_dcr check(v>0));
insert into dc_rebind values (1,10,-1);
alter table dc_rebind drop column v, add column v int default 1, add constraint ck_dcr check(w>0);
select column_name from information_schema.columns
    where table_schema=database() and table_name='dc_rebind' order by ordinal_position;
select constraint_name,check_clause from information_schema.check_constraints
    where constraint_schema=database() and constraint_name='ck_dcr';
select * from dc_rebind;

-- When existing rows satisfy the replacement, the combined ALTER succeeds.
-- The recreated table is (id,w,v): w=-5 violates the replacement even though
-- the same-name recreated v=9 passes, and w=5 lands together with v=-9
-- because the original v>0 was auto-dropped with the old column.
create table dc_rebind2(id int primary key,v int,w int,constraint ck_dcr2 check(v>0));
insert into dc_rebind2 values (1,10,5);
alter table dc_rebind2 drop column v, add column v int default 1, add constraint ck_dcr2 check(w>0);
select constraint_name,check_clause from information_schema.check_constraints
    where constraint_schema=database() and constraint_name='ck_dcr2';
insert into dc_rebind2 values (2,-5,9);
insert into dc_rebind2 values (2,5,-9);
select * from dc_rebind2 order by id;

-- A CHECK retained through RENAME keeps its creation-time bound semantics:
-- the division was bound with div_precision_increment 10, so the same row
-- stays valid through COPY and enforcement is unchanged afterwards even when
-- the ALTER runs under 0.
set @saved_dcp = @@div_precision_increment;
set div_precision_increment = 10;
create table dc_prec(id int primary key,a decimal(10,2),b decimal(10,2),v int,constraint ck_dcp check(a/b>0.3333333));
insert into dc_prec values (1,1,3,0);
create table dc_prec2(id int primary key,a decimal(10,2),b decimal(10,2),v int,constraint ck_dcp2 check(a/b>0.3333333));
set div_precision_increment = 0;
alter table dc_prec rename column a to x, drop column v;
select constraint_name,check_clause from information_schema.check_constraints
    where constraint_schema=database() and constraint_name='ck_dcp';
insert into dc_prec values (2,1,3);
insert into dc_prec values (3,1,4);
select * from dc_prec order by id;
-- The empty-table form persists unchanged semantics: the first inserted row
-- is still validated at the creation-time scale, so 1/3 satisfies the check.
alter table dc_prec2 rename column a to x, drop column v;
insert into dc_prec2 values (1,1,3);
select * from dc_prec2;
set div_precision_increment = @saved_dcp;
drop table dc_prec;
drop table dc_prec2;
drop table dc_single;
drop table dc_none;
drop table dc_multi;
drop table dc_replace;
drop table dc_readd;
drop table dc_atomic;
drop table dc_identity;
drop table dc_generic;
drop table dc_generic2;
drop table dc_rebind;
drop table dc_rebind2;

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
