-- DATA BRANCH DIFF COLUMNS must retain every explicit primary-key column.
-- This covers the issue #25244 reproduction, projected LIMIT output, a
-- non-lineage DELETE, and OUTPUT AS schema materialization.

drop database if exists issue_25244_diff_columns_pk;
create database issue_25244_diff_columns_pk;
use issue_25244_diff_columns_pk;

create table base (
    org_id int,
    event_id int,
    val int,
    note varchar(64),
    primary key (org_id, event_id)
);

insert into base values
    (1, 1, 10, 'seed'),
    (1, 2, 20, 'seed'),
    (2, 1, 30, 'seed'),
    (2, 2, 40, 'seed');

data branch create table target from base;
update target
    set val = val + 9, note = 'upd'
    where org_id = 1 and event_id = 1;

-- Exact issue reproduction: COLUMNS (val) still includes the complete PK.
data branch diff target against base columns (val);

-- The PK remains in definition order, and a requested PK is not duplicated.
data branch diff target against base columns (note, event_id, note);

delete from target where org_id = 2 and event_id = 2;
insert into target values (3, 3, 50, 'new');

-- LIMIT uses the same final PK-aware projection and retains the winning row's PK.
data branch diff target against base columns (note) output limit 1;

-- The complete changed set keeps PK columns for UPDATE, DELETE, and INSERT.
data branch diff target against base columns (note);

-- The same projection contract applies to unrelated tables; a row removed from
-- the target is represented by the base-side INSERT in this diff direction.
create table unrelated_base (
    org_id int,
    event_id int,
    note varchar(64),
    primary key (org_id, event_id)
);
create table unrelated_target (
    org_id int,
    event_id int,
    note varchar(64),
    primary key (org_id, event_id)
);
insert into unrelated_base values (8, 1, 'keep'), (8, 2, 'delete');
insert into unrelated_target values (8, 1, 'keep'), (8, 2, 'delete');
delete from unrelated_target where org_id = 8 and event_id = 2;
data branch diff unrelated_target against unrelated_base columns (note);

-- OUTPUT AS uses the same effective column list as ordinary row output.
data branch diff target against base columns (note) output as diff_as;
show create table diff_as;
select __mo_diff_source, __mo_diff_flag, org_id, event_id, note
    from diff_as order by org_id, event_id;

drop table diff_as;
drop table unrelated_target;
drop table unrelated_base;
drop table target;
drop table base;
drop database issue_25244_diff_columns_pk;
