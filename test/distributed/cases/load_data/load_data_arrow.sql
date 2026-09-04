drop database if exists arrow_load_bvt;
create database arrow_load_bvt;
use arrow_load_bvt;

-- Arrow IPC File with multiple record batches and NULL values.
create table arrow_file(id bigint, name varchar(50));
load data infile {'filepath'='$resources/load_data/arrow_file.arrow', 'format'='arrow', 'arrow_container'='file'} into table arrow_file;
select * from arrow_file order by id;

-- Explicit LOAD columns map source order independently of physical table order.
create table arrow_column_order(name varchar(50), id bigint);
load data infile {'filepath'='$resources/load_data/arrow_file.arrow', 'format'='arrow'} into table arrow_column_order(id, name);
select id, name from arrow_column_order order by id;

-- Arrow IPC Stream is supported for one serial source.
create table arrow_stream(id bigint, name varchar(50));
load data infile {'filepath'='$resources/load_data/arrow_stream.arrow', 'format'='arrow', 'arrow_container'='stream'} into table arrow_stream;
select * from arrow_stream order by id;

-- File-container pattern expands into independently planned record shards.
create table arrow_pattern(id bigint, name varchar(50));
load data infile {'filepath'='$resources/load_data/arrow_part_*.arrow', 'format'='arrow'} into table arrow_pattern parallel 'true';
select * from arrow_pattern order by id;

-- Schema mismatch must roll back the whole statement.
create table arrow_schema_rollback(id bigint, name varchar(50));
insert into arrow_schema_rollback values (0, 'seed');
load data infile {'filepath'='$resources/load_data/arrow_mismatch_*.arrow', 'format'='arrow'} into table arrow_schema_rollback parallel 'true';
select * from arrow_schema_rollback order by id;

-- A late NOT NULL violation in another object must also roll back all shards.
create table arrow_constraint_rollback(id bigint not null, name varchar(50));
insert into arrow_constraint_rollback values (0, 'seed');
load data infile {'filepath'='$resources/load_data/arrow_notnull_*.arrow', 'format'='arrow'} into table arrow_constraint_rollback parallel 'true';
select * from arrow_constraint_rollback order by id;

-- Corrupt metadata fails without publishing rows.
create table arrow_corrupt(id bigint, name varchar(50));
load data infile {'filepath'='$resources/load_data/arrow_corrupt.arrow', 'format'='arrow'} into table arrow_corrupt;
select count(*) from arrow_corrupt;

-- Explicit transaction visibility and rollback use the ordinary LOAD path.
create table arrow_txn(id bigint, name varchar(50));
begin;
load data infile {'filepath'='$resources/load_data/arrow_file.arrow', 'format'='arrow'} into table arrow_txn;
select count(*) from arrow_txn;
rollback;
select count(*) from arrow_txn;
begin;
load data infile {'filepath'='$resources/load_data/arrow_file.arrow', 'format'='arrow'} into table arrow_txn;
commit;
select count(*) from arrow_txn;

-- Uncommitted Arrow rows are not visible in another session.
create table arrow_isolation(id bigint, name varchar(50));
begin;
load data infile {'filepath'='$resources/load_data/arrow_stream.arrow', 'format'='arrow', 'arrow_container'='stream'} into table arrow_isolation;
-- @session:id=1{
use arrow_load_bvt;
select count(*) from arrow_isolation;
-- @session}
commit;
-- @session:id=1{
select count(*) from arrow_isolation;
-- @session}

-- Unsupported SQL surfaces and options reject deterministically.
create external table arrow_external_reject(id bigint, name varchar(50))
infile {'filepath'='$resources/load_data/arrow_file.arrow', 'format'='arrow'};
create table arrow_option_reject(id bigint, name varchar(50));
load data local infile {'filepath'='$resources/load_data/arrow_file.arrow', 'format'='arrow'} into table arrow_option_reject;
load data infile {'filepath'='$resources/load_data/arrow_file.arrow', 'format'='arrow', 'compression'='gzip'} into table arrow_option_reject;
load data infile {'filepath'='$resources/load_data/arrow_file.arrow', 'format'='arrow', 'arrow_container'='flight'} into table arrow_option_reject;

-- Differential check against the ordinary INSERT path.
create table arrow_insert_reference(id bigint, name varchar(50));
insert into arrow_insert_reference values (1, 'alpha'), (2, 'beta'), (3, null);
select count(*) from (select * from arrow_file except select * from arrow_insert_reference) a;
select count(*) from (select * from arrow_insert_reference except select * from arrow_file) a;

drop database arrow_load_bvt;
