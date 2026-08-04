-- Regression for issue #26078.
-- A reversed BETWEEN SNAPSHOT range must be rejected without partial changes.
drop snapshot if exists bvt_issue_26078_early;
drop snapshot if exists bvt_issue_26078_late;
drop database if exists bvt_issue_26078;
create database bvt_issue_26078;
use bvt_issue_26078;

create table base(id int primary key, val int);
insert into base values (1, 10), (2, 20), (3, 30), (4, 40);
data branch create table src from base;
data branch create table dst from base;
create snapshot bvt_issue_26078_early for table bvt_issue_26078 src;
update src set val = 200 where id = 2;
delete from src where id = 3;
insert into src values (5, 50);
create snapshot bvt_issue_26078_late for table bvt_issue_26078 src;

set @dst_id = (
    select rel_id from mo_catalog.mo_tables
    where account_id = 0
      and reldatabase = 'bvt_issue_26078'
      and relname = 'dst'
);
set @metadata_before = (
    select count(*) from mo_catalog.mo_branch_metadata
    where table_id = @dst_id and table_deleted = false
);
set @protection_before = (
    select count(*) from mo_catalog.mo_snapshots
    where sname = concat('__mo_branch_', cast(@dst_id as varchar))
);

-- @regex("invalid BETWEEN SNAPSHOT range: start snapshot 'bvt_issue_26078_late' is later than end snapshot 'bvt_issue_26078_early'",true)
data branch pick src into dst
between snapshot bvt_issue_26078_late and bvt_issue_26078_early;

select id, val from dst order by id;
select @metadata_before = (
           select count(*) from mo_catalog.mo_branch_metadata
           where table_id = @dst_id and table_deleted = false
       ) as metadata_unchanged,
       @protection_before = (
           select count(*) from mo_catalog.mo_snapshots
           where sname = concat('__mo_branch_', cast(@dst_id as varchar))
       ) as protection_unchanged;
select 1 as session_alive_after_rejection;

data branch pick src into dst
between snapshot bvt_issue_26078_early and bvt_issue_26078_late;
select id, val from dst order by id;

drop snapshot bvt_issue_26078_early;
drop snapshot bvt_issue_26078_late;
drop database bvt_issue_26078;
