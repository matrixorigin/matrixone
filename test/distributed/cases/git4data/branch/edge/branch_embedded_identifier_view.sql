-- Embedded identifier delimiters must survive view authorization, database copies, and snapshot restore.
drop snapshot if exists issue_26127_view_snapshot;
drop database if exists issue_26127_view_branch;
drop database if exists issue_26127_view_clone;
drop database if exists issue_26127_view_src;

create database issue_26127_view_src;
create table issue_26127_view_src.`base``t`(id int primary key, note varchar(20));
insert into issue_26127_view_src.`base``t` values (1, 'source');
create view issue_26127_view_src.`view``v` as select id, note from issue_26127_view_src.`base``t`;
select count(*) as source_view_rows from issue_26127_view_src.`view``v`;

data branch create database issue_26127_view_branch from issue_26127_view_src;
create database issue_26127_view_clone clone issue_26127_view_src;
select count(*) as branch_view_rows from issue_26127_view_branch.`view``v`;
select count(*) as clone_view_rows from issue_26127_view_clone.`view``v`;

create snapshot issue_26127_view_snapshot for database issue_26127_view_src;
insert into issue_26127_view_src.`base``t` values (2, 'after-snapshot');
select count(*) as changed_source_view_rows from issue_26127_view_src.`view``v`;
restore database issue_26127_view_src {snapshot="issue_26127_view_snapshot"};
select count(*) as restored_source_view_rows from issue_26127_view_src.`view``v`;

drop database issue_26127_view_branch;
drop database issue_26127_view_clone;
drop database issue_26127_view_src;
drop snapshot issue_26127_view_snapshot;
