-- issue#27050: CREATE TABLE ... LIKE/CLONE must reject sequence sources before
-- creating an ordinary table from the sequence's internal relation.
drop database if exists clone_sequence_as_table;
create database clone_sequence_as_table;
use clone_sequence_as_table;

create sequence seq_live increment 2 start with 11 no cycle;
select nextval('seq_live');
-- @regex("is not BASE TABLE",true)
create table dst_live clone seq_live;
-- @regex("is not BASE TABLE",true)
create table dst_like like seq_live;
select count(*) as live_target_tables
from mo_catalog.mo_tables
where reldatabase = database()
  and relname in ('dst_live', 'dst_like');
select count(*) as live_target_columns
from mo_catalog.mo_columns
where att_database = database()
  and att_relname in ('dst_live', 'dst_like');
select relname, relkind
from mo_catalog.mo_tables
where reldatabase = database()
  and relname in ('seq_live', 'dst_live', 'dst_like')
order by relname;
select nextval('seq_live');
show sequences;

create sequence seq_snapshot increment 3 start with 21 no cycle;
select nextval('seq_snapshot');
drop snapshot if exists seq_snapshot_sp;
create snapshot seq_snapshot_sp for table clone_sequence_as_table seq_snapshot;
select nextval('seq_snapshot');
-- @regex("is not BASE TABLE",true)
create table dst_snapshot clone seq_snapshot {snapshot = 'seq_snapshot_sp'};
select count(*) as snapshot_target_tables
from mo_catalog.mo_tables
where reldatabase = database()
  and relname = 'dst_snapshot';
select count(*) as snapshot_target_columns
from mo_catalog.mo_columns
where att_database = database()
  and att_relname = 'dst_snapshot';
select relname, relkind
from mo_catalog.mo_tables
where reldatabase = database()
  and relname in ('seq_snapshot', 'dst_snapshot')
order by relname;
select nextval('seq_snapshot');

create table src_base (id int primary key, value varchar(20));
insert into src_base values (1, 'base-source');
create table dst_base clone src_base;
select relname, relkind
from mo_catalog.mo_tables
where reldatabase = database()
  and relname in ('src_base', 'dst_base')
order by relname;
select * from dst_base;

create view view_src as select * from src_base;
-- @regex("is not BASE TABLE",true)
create table dst_view clone view_src;

create external table ext_src (id int)
infile{"filepath"='$resources/external_table_file/extable.csv'}
fields terminated by ',' lines terminated by '\n';
-- @regex("is not BASE TABLE",true)
create table dst_external clone ext_src;
select count(*) as non_base_target_tables
from mo_catalog.mo_tables
where reldatabase = database()
  and relname in ('dst_view', 'dst_external');

drop snapshot if exists seq_snapshot_sp;
drop database clone_sequence_as_table;
