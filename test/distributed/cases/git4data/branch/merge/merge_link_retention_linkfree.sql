-- r51 review F2: an ordinary link-free lineage retirement — create
-- parent, create branch, drop parent (parent-before-child), drop
-- branch — must mint NO retention-ledger residue. The orphan-parent
-- funnel probes link presence first; a successful empty probe ends the
-- parent's handling with no KEEP write and no reap WARN. D154-D's
-- never-auto-deleted residue stays reserved for genuine retained links.
create database branch_linkfree_lfr;
use branch_linkfree_lfr;
create table lfr_stash (k varchar(16) primary key, v bigint);

create table lfr_p (id int primary key, v varchar(32));
insert into lfr_p values (1, 'one');
data branch create table lfr_b from lfr_p;
set @lfrp1 = (select rel_id from mo_catalog.mo_tables where relname = 'lfr_p' and reldatabase = 'branch_linkfree_lfr');
replace into lfr_stash values ('p1', @lfrp1);
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @lfrp1;
drop table lfr_p;
drop table lfr_b;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = (select v from lfr_stash where k = 'p1');
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = (select v from lfr_stash where k = 'p1');

-- A second identical cycle: pre-fix each run accumulated one more
-- permanent residue row plus a WARN per owner-removal reap.
create table lfr_q (id int primary key, v varchar(32));
insert into lfr_q values (1, 'one');
data branch create table lfr_c from lfr_q;
set @lfrp2 = (select rel_id from mo_catalog.mo_tables where relname = 'lfr_q' and reldatabase = 'branch_linkfree_lfr');
replace into lfr_stash values ('p2', @lfrp2);
drop table lfr_q;
drop table lfr_c;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = (select v from lfr_stash where k = 'p2');
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = (select v from lfr_stash where k = 'p2');
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id in (select v from lfr_stash);

drop database branch_linkfree_lfr;
