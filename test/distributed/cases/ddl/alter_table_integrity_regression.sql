-- @suit
-- @case
-- @desc: regressions for #26853, #26839, and #26838
-- @label:bvt

drop database if exists alter_table_integrity_regression;
create database alter_table_integrity_regression;
use alter_table_integrity_regression;

-- #26853: COPY must recheck keys when a conversion changes key values.
create table pk_scale (v decimal(6,2) primary key);
insert into pk_scale values (1.21), (1.24);
alter table pk_scale modify column v decimal(6,1);
select v from pk_scale order by v;

create table uk_scale (id int primary key, v decimal(6,2), unique key uk_v(v));
insert into uk_scale values (1,1.21), (2,1.24);
alter table uk_scale modify column v decimal(6,1);
select id, v from uk_scale order by id;

-- Same-name DROP/ADD does not preserve the key's source-column identity.
-- Every replacement key receives DEFAULT 0, so COPY must reject the duplicate
-- and leave the original table unchanged.
create table pk_same_name_replace (a bigint primary key, payload int);
insert into pk_same_name_replace values (1,10), (2,20);
alter table pk_same_name_replace
    drop column a,
    add column a bigint not null default 0 primary key;
select a, payload from pk_same_name_replace order by a;

create table uk_same_name_replace (
    id int primary key,
    u bigint not null unique
);
insert into uk_same_name_replace values (1,1), (2,2);
alter table uk_same_name_replace
    drop column u,
    add column u bigint not null default 0 unique;
select id, u from uk_same_name_replace order by id;

-- #26839: rename only column references in persisted CHECK SQL.
create table check_rename (
    a int,
    b int,
    note varchar(20),
    constraint ck_ab check (a < b and note <> 'a')
);
alter table check_rename rename column a to x;
show create table check_rename;
insert into check_rename values (2,1,'ok');
insert into check_rename values (1,2,'ok');
select x, b, note from check_rename;

-- CASE has two optional AST operands: searched CASE omits its input expression,
-- and CASE without ELSE omits its fallback expression. CHECK rewriting must
-- traverse both valid forms without panicking.
create table check_case_rename (
    a int,
    constraint ck_case_search check (case when a > 0 then 1 else 0 end = 1),
    constraint ck_case_no_else check (case a when 1 then 1 end = 1)
);
alter table check_case_rename rename column a to x;
show create table check_case_rename;
insert into check_case_rename values (0);
insert into check_case_rename values (1);
select x from check_case_rename;

-- CHANGE COLUMN uses COPY but must preserve the same CHECK rename invariant.
create table check_change (
    a int,
    b int,
    constraint ck_change check (a < b)
);
alter table check_change change column a x int;
show create table check_change;
insert into check_change values (2,1);
insert into check_change values (1,2);
select x, b from check_change;

-- #26838: table ID 0 is a self-reference marker, not a parent table ID.
create table self_cascade (
    id int primary key,
    parent_id int,
    constraint fk_self_cascade foreign key (parent_id)
        references self_cascade(id) on delete cascade
);
insert into self_cascade values (1,null), (2,1);
truncate table self_cascade;
select count(*) from self_cascade;
insert into self_cascade values (3,null), (4,3);

create table self_set_null (
    id int primary key,
    parent_id int,
    constraint fk_self_set_null foreign key (parent_id)
        references self_set_null(id) on delete set null
);
insert into self_set_null values (1,null), (2,1);
truncate table self_set_null;
select count(*) from self_set_null;
insert into self_set_null values (3,null), (4,3);

create table self_restrict (
    id int primary key,
    parent_id int,
    constraint fk_self_restrict foreign key (parent_id)
        references self_restrict(id) on delete restrict
);
insert into self_restrict values (1,null), (2,1);
truncate table self_restrict;
select count(*) from self_restrict;
insert into self_restrict values (3,null), (4,3);

drop database alter_table_integrity_regression;
