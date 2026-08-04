-- Regression for issue #26073.
-- Branch metadata must preserve every backslash byte in quoted identifiers.
drop database if exists `bvt\issue_26073`;
create database `bvt\issue_26073`;

create table `bvt\issue_26073`.normal_src(
    id int primary key,
    note varchar(30)
);
insert into `bvt\issue_26073`.normal_src values (1, 'normal');
data branch create table `bvt\issue_26073`.normal_dst
from `bvt\issue_26073`.normal_src;

create table `bvt\issue_26073`.`src\t`(
    id int primary key,
    note varchar(30)
);
insert into `bvt\issue_26073`.`src\t` values (1, 'source');
data branch create table `bvt\issue_26073`.`dst\t`
from `bvt\issue_26073`.`src\t`;
set @dst_id = (
    select rel_id
    from mo_catalog.mo_tables
    where account_id = 0
      and reldatabase = 'bvt\\issue_26073'
      and relname = 'dst\\t'
);
set @branch_snapshot = concat('__mo_branch_', cast(@dst_id as varchar));
insert into `bvt\issue_26073`.`dst\t` values (2, 'branch-only');

select id, note from `bvt\issue_26073`.normal_dst order by id;
select id, note from `bvt\issue_26073`.`dst\t` order by id;

select hex(dst.reldatabase) as catalog_database_hex,
       hex(s.database_name) as snapshot_database_hex,
       hex(src.relname) as catalog_table_hex,
       hex(s.table_name) as snapshot_table_hex,
       s.obj_id = src.rel_id as source_object_matches
from mo_catalog.mo_snapshots s
join mo_catalog.mo_tables dst
  on s.sname = concat('__mo_branch_', cast(dst.rel_id as varchar))
join mo_catalog.mo_tables src
  on src.account_id = dst.account_id
 and src.reldatabase = dst.reldatabase
 and src.relname = 'src\\t'
where s.kind = 'branch'
  and dst.account_id = 0
  and dst.reldatabase = 'bvt\\issue_26073'
  and dst.relname = 'dst\\t';

data branch diff `bvt\issue_26073`.`dst\t`
against `bvt\issue_26073`.`src\t` output summary;

drop table `bvt\issue_26073`.`dst\t`;
select count(*) as protection_rows_after_drop
from mo_catalog.mo_snapshots where sname = @branch_snapshot;
select count(*) as metadata_rows_after_drop
from mo_catalog.mo_branch_metadata where table_id = @dst_id;
select id, note from `bvt\issue_26073`.`src\t` order by id;

drop database `bvt\issue_26073`;
