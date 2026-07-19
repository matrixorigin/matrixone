-- DATA BRANCH DIFF OUTPUT AS coverage.
--
-- OUTPUT AS materializes a normal persistent table.  Its two metadata columns
-- are __mo_diff_source and __mo_diff_flag; the remaining columns retain the
-- requested source-column order and types.  The result table is never itself
-- a data branch.

drop database if exists test_diff_output_as_dst;
drop database if exists test_diff_output_as;
create database test_diff_output_as;
use test_diff_output_as;

-- Case 1: full materialization preserves all visible columns and is a normal
-- persistent table.  User columns named source/flag must not collide with the
-- output metadata columns.  Covers INSERT, UPDATE, DELETE, NULL, DECIMAL,
-- JSON, and TIMESTAMP.
create table base_full(
    id int primary key,
    source varchar(20),
    flag varchar(20),
    amount decimal(12,2),
    payload json,
    event_time timestamp
);
insert into base_full values
    (1, 'base-1', 'old', 10.50, '{"v": 1}', '2024-01-01 01:02:03'),
    (2, 'base-2', 'delete', null, null, null),
    (3, 'base-3', 'keep', 30.00, '{"v": 3}', '2024-01-03 01:02:03');

data branch create table branch_full from base_full;
update branch_full
    set source = 'branch-1', flag = 'updated', amount = 11.75,
        payload = '{"v": 11}', event_time = '2024-02-01 01:02:03'
    where id = 1;
delete from branch_full where id = 2;
insert into branch_full values
    (4, 'branch-4', 'inserted', 40.25, '{"v": 4}', '2024-04-01 01:02:03');

data branch diff branch_full against base_full output as diff_full;
show create table diff_full;
select __mo_diff_source, __mo_diff_flag, id, source, flag, amount, payload, event_time
    from diff_full order by id;
select count(*) as diff_full_branch_metadata_rows
    from mo_catalog.mo_branch_metadata
    where table_id = (
        select rel_id from mo_catalog.mo_tables
        where reldatabase = 'test_diff_output_as' and relname = 'diff_full'
    );

-- A result table remains visible from a different session, so it cannot be a
-- session temporary table.
-- @session:id=1{
select __mo_diff_source, __mo_diff_flag, id
    from test_diff_output_as.diff_full order by id;
-- @session

-- Case 2: COLUMNS controls result-table schema and column order.  Duplicate
-- names are deduplicated just as they are for ordinary DIFF output.
data branch diff branch_full against base_full
    columns (flag, id, amount) output as diff_projected;
show create table diff_projected;
select __mo_diff_source, __mo_diff_flag, flag, id, amount
    from diff_projected order by id;

data branch diff branch_full against base_full
    columns (amount, amount, source) output as diff_projected_dedup;
show create table diff_projected_dedup;
select __mo_diff_source, __mo_diff_flag, amount, source
    from diff_projected_dedup order by amount, source;

-- Invalid projection fails before creating a destination table.
-- @regex("column \"missing\" not found",true)
data branch diff branch_full against base_full
    columns (missing) output as diff_missing_column;
select count(*) as diff_missing_column_tables
    from mo_catalog.mo_tables
    where reldatabase = 'test_diff_output_as' and relname = 'diff_missing_column';

-- Case 3: no changes still creates a correctly typed empty result table.
create table base_empty(id int primary key, note varchar(20), amount decimal(10,2));
insert into base_empty values (1, 'one', 1.00), (2, null, null);
data branch create table branch_empty from base_empty;
data branch diff branch_empty against base_empty output as diff_empty;
show create table diff_empty;
select count(*) as diff_empty_rows from diff_empty;

-- Case 4: source columns named like the two generated metadata columns are
-- retained without ambiguity; generated names receive deterministic suffixes.
create table base_metadata_names(
    id int primary key,
    __mo_diff_source varchar(20),
    __mo_diff_flag varchar(20)
);
insert into base_metadata_names values (1, 'before-source', 'before-flag');
data branch create table branch_metadata_names from base_metadata_names;
update branch_metadata_names
    set __mo_diff_source = 'after-source', __mo_diff_flag = 'after-flag'
    where id = 1;
data branch diff branch_metadata_names against base_metadata_names
    output as diff_metadata_names;
show create table diff_metadata_names;
select __mo_diff_source_1, __mo_diff_flag_1,
       id, __mo_diff_source, __mo_diff_flag
    from diff_metadata_names order by id;

-- Case 5: no-PK diff uses the fake-PK path internally but materializes only
-- visible columns.  The destination is fully qualified in a different DB.
create table base_no_pk(
    id int,
    note varchar(20),
    amount decimal(10,2),
    payload json
);
insert into base_no_pk values
    (1, 'before', 1.00, '{"k": "one"}'),
    (2, null, null, null),
    (3, 'delete', 3.00, '{"k": "three"}');
data branch create table branch_no_pk from base_no_pk;
update branch_no_pk set note = 'after', amount = 1.25, payload = '{"k": "updated"}' where id = 1;
delete from branch_no_pk where id = 3;
insert into branch_no_pk values (4, 'insert', 4.00, '{"k": "four"}');

create database test_diff_output_as_dst;
data branch diff branch_no_pk against base_no_pk
    columns (payload, id, note, amount)
    output as test_diff_output_as_dst.diff_no_pk;
show create table test_diff_output_as_dst.diff_no_pk;
select __mo_diff_source, __mo_diff_flag, payload, id, note, amount
    from test_diff_output_as_dst.diff_no_pk order by id;
select count(*) as diff_no_pk_branch_metadata_rows
    from mo_catalog.mo_branch_metadata
    where table_id = (
        select rel_id from mo_catalog.mo_tables
        where reldatabase = 'test_diff_output_as_dst' and relname = 'diff_no_pk'
    );

-- Case 6: OUTPUT AS never overwrites or appends to an existing table.
create table diff_existing(sentinel int);
insert into diff_existing values (7);
-- @regex("already exists",true)
data branch diff branch_full against base_full output as diff_existing;
select * from diff_existing;

-- Quoted output-table identifiers are preserved.
data branch diff branch_empty against base_empty columns (id, note)
    output as `diff-output-quoted`;
show create table `diff-output-quoted`;
select __mo_diff_source, __mo_diff_flag, id, note
    from `diff-output-quoted` order by id;

-- A destination snapshot is nonsensical for a newly created result table and
-- must fail before any table is created.
-- @regex("destination snapshot option is not supported",true)
data branch diff branch_empty against base_empty
    output as diff_snapshot_destination{snapshot="unused_snapshot"};
select count(*) as diff_snapshot_destination_tables
    from mo_catalog.mo_tables
    where reldatabase = 'test_diff_output_as' and relname = 'diff_snapshot_destination';

-- Case 7: snapshots on the source sides remain valid; only a snapshot on the
-- newly-created destination is rejected.
create table source_snapshot(id int primary key, note varchar(20));
insert into source_snapshot values (1, 'before');
create snapshot diff_output_as_source_sp for table test_diff_output_as source_snapshot;
update source_snapshot set note = 'after' where id = 1;
insert into source_snapshot values (2, 'inserted');
data branch diff source_snapshot against source_snapshot{snapshot="diff_output_as_source_sp"}
    output as diff_source_snapshot;
select __mo_diff_source, __mo_diff_flag, id, note
    from diff_source_snapshot order by id;
drop snapshot diff_output_as_source_sp;

-- Case 8: binary values use byte-preserving SQL literals during
-- materialization. HEX avoids result-file encoding ambiguity while checking
-- every byte, including NUL and quote/backslash bytes.
create table base_binary(
    id int primary key,
    text_value varchar(20),
    binary_value varbinary(8),
    blob_value blob
);
insert into base_binary values (1, 'before', x'00ff', x'00fe');
data branch create table branch_binary from base_binary;
update branch_binary
    set text_value = 'after', binary_value = x'00275c0a', blob_value = x'000102ff'
    where id = 1;
data branch diff branch_binary against base_binary output as diff_binary;
show create table diff_binary;
select __mo_diff_source, __mo_diff_flag, id,
       hex(text_value), hex(binary_value), hex(blob_value)
    from diff_binary order by id;

-- Case 9: the output schema must be derived from the exact resolved base
-- relation, not the live table name.  A historical base can have an older
-- schema, or even be dropped and recreated with a different schema.
create table source_snapshot_schema(id int primary key, legacy_note varchar(20));
insert into source_snapshot_schema values (1, 'before');
data branch create table branch_snapshot_schema from source_snapshot_schema;
create snapshot diff_output_as_schema_sp for table test_diff_output_as source_snapshot_schema;
alter table source_snapshot_schema drop column legacy_note;
alter table source_snapshot_schema add column current_version int;
update branch_snapshot_schema set legacy_note = 'branch-version' where id = 1;
data branch diff branch_snapshot_schema
    against source_snapshot_schema{snapshot="diff_output_as_schema_sp"}
    output as diff_snapshot_schema;
show create table diff_snapshot_schema;
select __mo_diff_source, __mo_diff_flag, id, legacy_note
    from diff_snapshot_schema order by id;
drop snapshot diff_output_as_schema_sp;

create table source_snapshot_recreated(id int primary key, legacy_note varchar(20));
insert into source_snapshot_recreated values (1, 'before');
data branch create table branch_snapshot_recreated from source_snapshot_recreated;
create snapshot diff_output_as_recreated_sp for table test_diff_output_as source_snapshot_recreated;
drop table source_snapshot_recreated;
create table source_snapshot_recreated(id int primary key, current_version int);
insert into source_snapshot_recreated values (1, 2);
update branch_snapshot_recreated set legacy_note = 'branch-version' where id = 1;
data branch diff branch_snapshot_recreated
    against source_snapshot_recreated{snapshot="diff_output_as_recreated_sp"}
    output as diff_snapshot_recreated;
show create table diff_snapshot_recreated;
select __mo_diff_source, __mo_diff_flag, id, legacy_note
    from diff_snapshot_recreated order by id;
drop snapshot diff_output_as_recreated_sp;

-- Both sides can independently resolve to historical snapshots.
create table source_both_snapshots(id int primary key, note varchar(20));
insert into source_both_snapshots values (1, 'base-before');
data branch create table branch_both_snapshots from source_both_snapshots;
create snapshot diff_output_as_both_base_sp for table test_diff_output_as source_both_snapshots;
update branch_both_snapshots set note = 'branch-snapshot' where id = 1;
create snapshot diff_output_as_both_branch_sp for table test_diff_output_as branch_both_snapshots;
update source_both_snapshots set note = 'base-after' where id = 1;
data branch diff branch_both_snapshots{snapshot="diff_output_as_both_branch_sp"}
    against source_both_snapshots{snapshot="diff_output_as_both_base_sp"}
    output as diff_both_snapshots;
select __mo_diff_source, __mo_diff_flag, id, note
    from diff_both_snapshots order by id;
drop snapshot diff_output_as_both_branch_sp;
drop snapshot diff_output_as_both_base_sp;

-- Case 10: every visible type whose diff value is serialized as SQL must
-- round-trip through OUTPUT AS, including the previously unsupported scalar
-- and spatial families.
create table base_special_types(
    id int primary key,
    bit_value bit(10),
    uuid_value uuid,
    enum_value enum('red', 'blue', 'green'),
    datalink_value datalink,
    geometry_value geometry
);
insert into base_special_types values
(1, b'001', '6d1b1f73-2dbf-11ed-940f-000c29847904', 'red',
'file:///tmp/mo_diff_type_base.csv', st_geomfromtext('POINT(1 1)')),
(2, b'010', 'ad9f809f-2dbd-11ed-940f-000c29847904', 'blue',
'file:///tmp/mo_diff_type_deleted.csv', st_geomfromtext('LINESTRING(0 0,1 1)'));
data branch create table branch_special_types from base_special_types;
update branch_special_types set
    bit_value = b'111',
    uuid_value = '1b50c137-2dba-11ed-940f-000c29847904',
    enum_value = 'green',
datalink_value = 'file:///tmp/mo_diff_type_updated.csv',
geometry_value = st_geomfromtext('POINT(2 2)')
    where id = 1;
delete from branch_special_types where id = 2;
insert into branch_special_types values
(3, b'101', '3ddf7b28-2dba-11ed-940f-000c29847904', 'red',
'file:///tmp/mo_diff_type_inserted.csv', st_geomfromtext('POINT(7 8)'));
data branch diff branch_special_types against base_special_types output as diff_special_types;
show create table diff_special_types;
select __mo_diff_source, __mo_diff_flag, id, cast(bit_value as int), uuid_value,
       enum_value, cast(datalink_value as varchar), st_astext(geometry_value)
    from diff_special_types order by id;

-- Case 11: timestamp literals are formatted in the user session timezone;
-- the internal executor must parse them in that same timezone.
set time_zone = '+00:00';
create table base_time_zone(id int primary key, event_time timestamp);
insert into base_time_zone values (1, '2024-01-01 01:02:03');
data branch create table branch_time_zone from base_time_zone;
update branch_time_zone set event_time = '2024-02-03 04:05:06' where id = 1;
data branch diff branch_time_zone against base_time_zone output as diff_time_zone;
select __mo_diff_source, __mo_diff_flag, id, event_time from diff_time_zone;
set time_zone = 'SYSTEM';

-- Case 12: OUTPUT AS participates in an explicit transaction.  Errors must
-- remain visible to the caller and rollbacks must remove the materialized
-- table as a single transactional operation.
begin;
data branch diff branch_empty against base_empty output as diff_txn_commit;
commit;
select count(*) as diff_txn_commit_tables
    from mo_catalog.mo_tables
    where reldatabase = 'test_diff_output_as' and relname = 'diff_txn_commit';

begin;
data branch diff branch_empty against base_empty output as diff_txn_rollback;
rollback;
select count(*) as diff_txn_rollback_tables
    from mo_catalog.mo_tables
    where reldatabase = 'test_diff_output_as' and relname = 'diff_txn_rollback';

create table diff_txn_existing(sentinel int);
insert into diff_txn_existing values (9);
begin;
-- @regex("already exists",true)
data branch diff branch_empty against base_empty output as diff_txn_existing;
rollback;
select * from diff_txn_existing;

-- Case 13: inplace ALTER can preserve the branch column ID while changing
-- logical type metadata. Reject the DIFF before OUTPUT AS creates a table;
-- otherwise a target value valid under VARCHAR(80) would be inserted into the
-- base-side VARCHAR(20) result schema.
create table base_schema_mismatch(id int primary key, payload varchar(20));
data branch create table branch_schema_mismatch from base_schema_mismatch;
alter table branch_schema_mismatch modify payload varchar(80);
insert into branch_schema_mismatch values (1, repeat(char(120), 40));
-- @regex("target table schema is not equivalent",true)
data branch diff branch_schema_mismatch against base_schema_mismatch
    output as diff_schema_mismatch;
select count(*) as diff_schema_mismatch_tables
    from mo_catalog.mo_tables
    where reldatabase = 'test_diff_output_as' and relname = 'diff_schema_mismatch';

drop database test_diff_output_as_dst;
drop database test_diff_output_as;
