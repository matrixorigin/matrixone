-- Regression for issue #26127.
-- Repaired scope: embedded backticks in table names across CLONE/DATA BRANCH.
-- The still-open embedded-backtick dependent-view path is intentionally absent.
drop database if exists bvt_issue_26127_branch;
drop database if exists bvt_issue_26127_clone;
drop database if exists bvt_issue_26127_src;
create database bvt_issue_26127_src;

create table bvt_issue_26127_src.`src``table`(
    id int primary key,
    note varchar(32)
);
insert into bvt_issue_26127_src.`src``table` values (1, 'source-row');
create table bvt_issue_26127_src.`ordinary``clone`
clone bvt_issue_26127_src.`src``table`;
data branch create table bvt_issue_26127_src.`branch``clone`
from bvt_issue_26127_src.`src``table`;

-- Ordinary dependency names remain a control for database-level copying.
create table bvt_issue_26127_src.parent_p(
    id int primary key,
    note varchar(32)
);
create table bvt_issue_26127_src.child_c(
    id int primary key,
    parent_id int,
    constraint fk_parent foreign key(parent_id)
        references bvt_issue_26127_src.parent_p(id)
);
insert into bvt_issue_26127_src.parent_p
values (1, 'parent-one'), (2, 'parent-two');
insert into bvt_issue_26127_src.child_c values (10, 1), (20, 2);
create view bvt_issue_26127_src.view_v as
select id, note from bvt_issue_26127_src.parent_p;

data branch create database bvt_issue_26127_branch
from bvt_issue_26127_src;
create database bvt_issue_26127_clone clone bvt_issue_26127_src;

select 'ordinary_table_clone' as clone_kind, id, note
from bvt_issue_26127_src.`ordinary``clone`
union all
select 'data_branch_table', id, note
from bvt_issue_26127_src.`branch``clone`
union all
select 'data_branch_database', id, note
from bvt_issue_26127_branch.`src``table`
union all
select 'ordinary_database_clone', id, note
from bvt_issue_26127_clone.`src``table`
order by clone_kind;

select 'branch' as copy_kind, c.id, p.note
from bvt_issue_26127_branch.child_c c
join bvt_issue_26127_branch.parent_p p on c.parent_id = p.id
union all
select 'clone', c.id, p.note
from bvt_issue_26127_clone.child_c c
join bvt_issue_26127_clone.parent_p p on c.parent_id = p.id
order by copy_kind, id;

select 'branch' as copy_kind, id, note
from bvt_issue_26127_branch.view_v
union all
select 'clone', id, note
from bvt_issue_26127_clone.view_v
order by copy_kind, id;

-- @regex("foreign key constraint fails",true)
insert into bvt_issue_26127_branch.child_c values (30, 999);
-- @regex("foreign key constraint fails",true)
insert into bvt_issue_26127_clone.child_c values (30, 999);
insert into bvt_issue_26127_branch.parent_p values (3, 'branch-only');

select 'source' as location, count(*) as parent_rows
from bvt_issue_26127_src.parent_p
union all
select 'branch', count(*) from bvt_issue_26127_branch.parent_p
union all
select 'clone', count(*) from bvt_issue_26127_clone.parent_p
order by location;

drop database bvt_issue_26127_branch;
drop database bvt_issue_26127_clone;
drop database bvt_issue_26127_src;
