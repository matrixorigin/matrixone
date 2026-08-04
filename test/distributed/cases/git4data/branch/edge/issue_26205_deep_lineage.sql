-- Regression for issue #26205.
-- Public control: a valid deep DATA BRANCH lineage remains traversable.
-- Cyclic in-memory metadata is constructed directly by the NewDAG Go test.
drop database if exists bvt_issue_26205;
create database bvt_issue_26205;
use bvt_issue_26205;

create table b0(id int primary key, val int);
insert into b0 values (1, 10), (2, 20);
data branch create table b1 from b0;
update b1 set val = 11 where id = 1;
data branch create table b2 from b1;
insert into b2 values (3, 30);
data branch create table b3 from b2;
delete from b3 where id = 2;
data branch create table b4 from b3;
data branch create table b5 from b4;
data branch create table b6 from b5;
data branch create table b7 from b6;
data branch create table b8 from b7;
data branch create table b9 from b8;
data branch create table b10 from b9;
data branch create table b11 from b10;
data branch create table b12 from b11;
data branch create table b13 from b12;
data branch create table b14 from b13;
data branch create table b15 from b14;
data branch create table b16 from b15;

data branch create table sibling from b8;
update sibling set val = 88 where id = 1;
insert into sibling values (5, 50);
update b16 set val = 33 where id = 3;
insert into b16 values (4, 40);
data branch create table merge_dst from b0;
data branch create table pick_dst from b0;

data branch diff b16 against b0 output summary;
data branch diff b16 against sibling output summary;
data branch pick b16 into pick_dst keys(1, 2, 3, 4);
data branch merge b16 into merge_dst;

select id, val from b16 order by id;
select id, val from sibling order by id;
select id, val from pick_dst order by id;
select id, val from merge_dst order by id;
select count(*) as active_lineage_nodes
from mo_catalog.mo_branch_metadata b
join mo_catalog.mo_tables t on t.rel_id = b.table_id
where t.account_id = 0
  and t.reldatabase = 'bvt_issue_26205'
  and t.relname in (
      'b1', 'b2', 'b3', 'b4', 'b5', 'b6', 'b7', 'b8',
      'b9', 'b10', 'b11', 'b12', 'b13', 'b14', 'b15', 'b16'
  )
  and b.table_deleted = false;

-- Keep stable table ids after their physical tables are deleted.  Deleting an
-- intermediate owner must retain its protect snapshot while a descendant is
-- alive, and the surviving branches must remain usable through that history.
create table lineage_ids(name varchar(32) primary key, table_id bigint unsigned);
insert into lineage_ids
select relname, rel_id
from mo_catalog.mo_tables
where account_id = 0
  and reldatabase = 'bvt_issue_26205'
  and relname in (
      'b1', 'b2', 'b3', 'b4', 'b5', 'b6', 'b7', 'b8',
      'b9', 'b10', 'b11', 'b12', 'b13', 'b14', 'b15', 'b16',
      'sibling'
  );

data branch delete table b8;
select b.table_deleted as deleted_intermediate
from lineage_ids i
join mo_catalog.mo_branch_metadata b on b.table_id = i.table_id
where i.name = 'b8';
select count(*) as protected_intermediate_snapshots
from lineage_ids i
join mo_catalog.mo_snapshots s
  on s.sname = concat('__mo_branch_', cast(i.table_id as char))
 and s.kind = 'branch'
where i.name = 'b8';

data branch diff b16 against sibling output summary;
data branch create table post_delete_dst from b0;
data branch pick sibling into post_delete_dst keys(1, 2, 3, 5);
select id, val from post_delete_dst order by id;

-- Delete the long descendant path parent-first.  b8 stays protected by its
-- live sibling, while the drained b9..b16 path is reclaimed synchronously.
data branch delete table b9;
data branch delete table b10;
data branch delete table b11;
data branch delete table b12;
data branch delete table b13;
data branch delete table b14;
data branch delete table b15;
data branch delete table b16;
select count(*) as live_split_owner_snapshots
from lineage_ids i
join mo_catalog.mo_snapshots s
  on s.sname = concat('__mo_branch_', cast(i.table_id as char))
 and s.kind = 'branch'
where i.name in ('b8', 'sibling');
select count(*) as drained_descendant_snapshots
from lineage_ids i
join mo_catalog.mo_snapshots s
  on s.sname = concat('__mo_branch_', cast(i.table_id as char))
 and s.kind = 'branch'
where i.name in ('b9', 'b10', 'b11', 'b12', 'b13', 'b14', 'b15', 'b16');
data branch diff sibling against b0 output summary;

-- Once the sibling and the final live descendant are gone, no protect
-- snapshot from the original lineage may remain.
data branch delete table sibling;
select count(*) as intermediate_snapshots_after_last_child
from lineage_ids i
join mo_catalog.mo_snapshots s
  on s.sname = concat('__mo_branch_', cast(i.table_id as char))
 and s.kind = 'branch'
where i.name = 'b8';

data branch delete table b1;
select count(*) as root_snapshots_with_live_descendant
from lineage_ids i
join mo_catalog.mo_snapshots s
  on s.sname = concat('__mo_branch_', cast(i.table_id as char))
 and s.kind = 'branch'
where i.name = 'b1';
data branch delete table b2;
data branch delete table b3;
data branch delete table b4;
data branch delete table b5;
data branch delete table b6;
data branch delete table b7;

select count(*) as remaining_lineage_snapshots
from lineage_ids i
join mo_catalog.mo_snapshots s
  on s.sname = concat('__mo_branch_', cast(i.table_id as char))
 and s.kind = 'branch';
select count(*) as deleted_lineage_rows
from lineage_ids i
join mo_catalog.mo_branch_metadata b on b.table_id = i.table_id
where b.table_deleted = true;

data branch delete table post_delete_dst;
data branch delete table merge_dst;
data branch delete table pick_dst;

drop database bvt_issue_26205;
