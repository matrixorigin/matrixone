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

drop database test_diff_output_as_dst;
drop database test_diff_output_as;
