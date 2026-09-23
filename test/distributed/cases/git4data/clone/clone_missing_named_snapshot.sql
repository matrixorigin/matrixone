-- issue#28049: a missing named snapshot in CREATE TABLE ... CLONE must be
-- reported as a user input error without exposing catalog internals.
drop snapshot if exists issue28049_valid_snapshot;
drop database if exists issue28049_clone_missing_snapshot;
create database issue28049_clone_missing_snapshot;
create table issue28049_clone_missing_snapshot.src (
  id int primary key,
  doc json
);
insert into issue28049_clone_missing_snapshot.src values (1, '{"score":10}');

-- Repeat the exact failing operation to prove that failed planning/execution
-- leaves no target metadata behind.
create table issue28049_clone_missing_snapshot.missing_snapshot_clone
  clone issue28049_clone_missing_snapshot.src
  {snapshot = 'issue28049_missing_snapshot'};
create table issue28049_clone_missing_snapshot.missing_snapshot_clone
  clone issue28049_clone_missing_snapshot.src
  {snapshot = 'issue28049_missing_snapshot'};
create table issue28049_clone_missing_snapshot.missing_snapshot_clone
  clone issue28049_clone_missing_snapshot.src
  {snapshot = 'issue28049_missing_snapshot'};

-- The identifier form of the named snapshot option must use the same boundary.
create table issue28049_clone_missing_snapshot.missing_snapshot_identifier_clone
  clone issue28049_clone_missing_snapshot.src
  {snapshot = issue28049_missing_snapshot};

select count(*) as failed_clone_tables
from mo_catalog.mo_tables
where reldatabase = 'issue28049_clone_missing_snapshot'
  and relname in ('missing_snapshot_clone', 'missing_snapshot_identifier_clone');
select count(*) as failed_clone_columns
from mo_catalog.mo_columns
where att_database = 'issue28049_clone_missing_snapshot'
  and att_relname in ('missing_snapshot_clone', 'missing_snapshot_identifier_clone');

-- A valid named snapshot remains a successful JSON table clone.
create snapshot issue28049_valid_snapshot
  for table issue28049_clone_missing_snapshot src;
create table issue28049_clone_missing_snapshot.valid_snapshot_clone
  clone issue28049_clone_missing_snapshot.src
  {snapshot = 'issue28049_valid_snapshot'};
select id, doc
from issue28049_clone_missing_snapshot.valid_snapshot_clone
order by id;
select count(*) as valid_clone_tables
from mo_catalog.mo_tables
where reldatabase = 'issue28049_clone_missing_snapshot'
  and relname = 'valid_snapshot_clone';

drop snapshot issue28049_valid_snapshot;
drop database issue28049_clone_missing_snapshot;
