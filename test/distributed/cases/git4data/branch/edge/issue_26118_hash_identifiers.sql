-- Regression for issue #26118.
-- Database CLONE and DATA BRANCH must preserve # identifiers and dependencies.
drop database if exists bvt_issue_26118_branch;
drop database if exists bvt_issue_26118_clone;
drop database if exists `bvt#issue#26118`;
create database `bvt#issue#26118`;

create table `bvt#issue#26118`.`parent#p`(
    id int primary key,
    note varchar(32)
);
create table `bvt#issue#26118`.`child#c`(
    id int primary key,
    parent_id int,
    constraint `fk#parent` foreign key(parent_id)
        references `bvt#issue#26118`.`parent#p`(id)
);
insert into `bvt#issue#26118`.`parent#p`
values (1, 'one'), (2, 'two');
insert into `bvt#issue#26118`.`child#c`
values (10, 1), (20, 2);
create view `bvt#issue#26118`.`view#1` as
select id, note from `bvt#issue#26118`.`parent#p`;
create view `bvt#issue#26118`.`view#2` as
select id, note from `bvt#issue#26118`.`view#1`;

data branch create database bvt_issue_26118_branch
from `bvt#issue#26118`;
create database bvt_issue_26118_clone clone `bvt#issue#26118`;

select 'branch' as copy_kind, count(*) as joined_rows
from bvt_issue_26118_branch.`child#c` c
join bvt_issue_26118_branch.`parent#p` p on c.parent_id = p.id
union all
select 'clone', count(*)
from bvt_issue_26118_clone.`child#c` c
join bvt_issue_26118_clone.`parent#p` p on c.parent_id = p.id
order by copy_kind;

select 'branch' as copy_kind, count(*) as dependent_view_rows
from bvt_issue_26118_branch.`view#2`
union all
select 'clone', count(*)
from bvt_issue_26118_clone.`view#2`
order by copy_kind;

select db_name, constraint_name, table_name, refer_table_name,
       count(*) as metadata_rows
from mo_catalog.mo_foreign_keys
where db_name in ('bvt_issue_26118_branch', 'bvt_issue_26118_clone')
  and constraint_name = 'fk#parent'
  and table_name = 'child#c'
  and refer_table_name = 'parent#p'
group by db_name, constraint_name, table_name, refer_table_name
order by db_name;

-- Both destinations must enforce their own copied foreign key.
-- @regex("foreign key constraint fails",true)
insert into bvt_issue_26118_branch.`child#c` values (30, 999);
-- @regex("foreign key constraint fails",true)
insert into bvt_issue_26118_clone.`child#c` values (30, 999);

insert into bvt_issue_26118_branch.`parent#p` values (3, 'branch-only');
insert into bvt_issue_26118_branch.`child#c` values (30, 3);
select 'source' as location, count(*) as parent_rows, max(id) as max_parent_id
from `bvt#issue#26118`.`parent#p`
union all
select 'branch', count(*), max(id)
from bvt_issue_26118_branch.`parent#p`
union all
select 'clone', count(*), max(id)
from bvt_issue_26118_clone.`parent#p`
order by location;
select id, note from bvt_issue_26118_branch.`view#2` order by id;

-- Cleanup and same-name recreation must not reuse stale dependency metadata.
drop database bvt_issue_26118_branch;
data branch create database bvt_issue_26118_branch
from `bvt#issue#26118`;
select count(*) as recreated_joined_rows
from bvt_issue_26118_branch.`child#c` c
join bvt_issue_26118_branch.`parent#p` p on c.parent_id = p.id;

drop database bvt_issue_26118_branch;
drop database bvt_issue_26118_clone;
drop database `bvt#issue#26118`;
