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
