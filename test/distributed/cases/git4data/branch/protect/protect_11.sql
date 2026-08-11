-- Branch Protect Snapshot - quoted identifiers containing backslashes.
-- The generated mo_snapshots INSERT must preserve catalog identifier bytes.

drop database if exists `protect\db`;
create database `protect\db`;
use `protect\db`;

create table `src\t` (id int primary key);
insert into `src\t` values (1);
data branch create table `dst\t` from `src\t`;

set @src_tid = (
  select rel_id from mo_catalog.mo_tables
  where reldatabase = 'protect\\db' and relname = 'src\\t'
);
set @dst_tid = (
  select rel_id from mo_catalog.mo_tables
  where reldatabase = 'protect\\db' and relname = 'dst\\t'
);
set @dst_sname = concat('__mo_branch_', cast(@dst_tid as char));

select hex(database_name) as database_hex,
       hex(table_name) as table_hex,
       obj_id = @src_tid as obj_id_matches_parent
from mo_catalog.mo_snapshots
where sname = @dst_sname and kind = 'branch';

drop table `dst\t`;
drop table `src\t`;
drop database `protect\db`;
