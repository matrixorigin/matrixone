drop account if exists table_changes_acc;
drop database if exists table_changes_db;
create database table_changes_db;
use table_changes_db;

-- Ordinary tables: the interval is (after, until].
create table ordinary_single_pk (
    id bigint primary key,
    name varchar(32),
    amount decimal(12, 2),
    enabled bool,
    created_on date,
    payload json
);
insert into ordinary_single_pk values
    (1, 'before-update', 1.25, true, '2026-01-01', '{"phase":"before"}'),
    (2, 'before-delete', 2.50, false, '2026-01-02', '{"phase":"before"}');
set @ordinary_after = (select watermark from change_watermark() w);
update ordinary_single_pk
set name = 'after-update', amount = 3.75, payload = '{"phase":"after"}'
where id = 1;
delete from ordinary_single_pk where id = 2;
insert into ordinary_single_pk values
    (3, 'after-insert', 4.00, true, '2026-01-03', '{"phase":"after"}');
set @ordinary_until = (select watermark from change_watermark() w);
select change_type, id, name, amount, enabled, created_on, cast(payload as varchar)
from table_changes('table_changes_db', 'ordinary_single_pk', @ordinary_after, @ordinary_until) c
order by change_type, id, name;

-- Composite primary keys exercise packed tombstone decoding.
create table ordinary_composite_pk (
    tenant_key int,
    object_key varchar(20),
    value text,
    primary key (tenant_key, object_key)
);
insert into ordinary_composite_pk values (1, 'old', 'delete-me');
set @composite_after = (select watermark from change_watermark() w);
delete from ordinary_composite_pk where tenant_key = 1 and object_key = 'old';
insert into ordinary_composite_pk values (2, 'new', 'insert-me');
set @composite_until = (select watermark from change_watermark() w);
select change_type, tenant_key, object_key, value
from table_changes('table_changes_db', 'ordinary_composite_pk', @composite_after, @composite_until) c
order by change_type, tenant_key, object_key;

-- The result schema preserves the supported scalar type families.
create table ordinary_type_families (
    id int primary key,
    signed_value bigint,
    unsigned_value bigint unsigned,
    floating_value double,
    decimal_value decimal(20, 6),
    char_value char(4),
    varchar_value varchar(32),
    binary_value varbinary(16),
    bool_value bool,
    date_value date,
    datetime_value datetime(6),
    timestamp_value timestamp(6),
    json_value json,
    uuid_value uuid
);
set @types_after = (select watermark from change_watermark() w);
insert into ordinary_type_families values (
    1, -2, 3, 4.5, 6.750000, 'mo', 'matrixone', hex('binary'), true,
    '2026-07-31', '2026-07-31 10:11:12.123456',
    '2026-07-31 10:11:12.123456', '{"ok":true}',
    '7b24d886-7ad7-4e67-a92b-03ec8b9d3c2d'
);
set @types_until = (select watermark from change_watermark() w);
select change_type, id, signed_value, unsigned_value, floating_value,
       decimal_value, char_value, varchar_value, hex(binary_value), bool_value,
       date_value, datetime_value, timestamp_value, cast(json_value as varchar),
       cast(uuid_value as varchar)
from table_changes('table_changes_db', 'ordinary_type_families', @types_after, @types_until) c;

-- System ordinary catalog tables are tenant-filtered by the table function.
set @catalog_after = (select watermark from change_watermark() w);
create database table_changes_catalog_marker;
create table table_changes_catalog_marker.marker (id int primary key);
set @catalog_until = (select watermark from change_watermark() w);
select change_type, datname
from table_changes('mo_catalog', 'mo_database', @catalog_after, @catalog_until) c
where datname = 'table_changes_catalog_marker'
order by change_type;
select change_type, reldatabase, relname
from table_changes('mo_catalog', 'mo_tables', @catalog_after, @catalog_until) c
where reldatabase = 'table_changes_catalog_marker' and relname = 'marker'
order by change_type;

-- Cluster tables are also tenant-filtered, including tombstones.
use mo_catalog;
drop table if exists table_changes_cluster;
create cluster table table_changes_cluster (
    id int,
    value varchar(20),
    primary key (id, account_id)
);
drop account if exists table_changes_acc;
create account table_changes_acc admin_name = 'admin' identified by '111';
-- @session:id=2&user=table_changes_acc:admin&password=111
use mo_catalog;
set @cluster_after = (select watermark from change_watermark() w);
-- @session
set @table_changes_account_id =
    (select account_id from mo_account where account_name = 'table_changes_acc');
insert into table_changes_cluster values (1, 'tenant-row', @table_changes_account_id);
insert into table_changes_cluster values (2, 'sys-row', 0);
-- @session:id=2&user=table_changes_acc:admin&password=111
set @cluster_until = (select watermark from change_watermark() w);
select change_type, id, value
from table_changes('mo_catalog', 'table_changes_cluster', @cluster_after, @cluster_until) c
order by change_type, id;
set @cluster_delete_after = @cluster_until;
-- @session
delete from table_changes_cluster
where (id = 1 and account_id = @table_changes_account_id)
   or (id = 2 and account_id = 0);
-- @session:id=2&user=table_changes_acc:admin&password=111
set @cluster_delete_until = (select watermark from change_watermark() w);
select change_type, id, value
from table_changes(
    'mo_catalog', 'table_changes_cluster',
    @cluster_delete_after, @cluster_delete_until
) c
order by change_type, id;
-- @session

-- Unsupported table kinds fail explicitly instead of returning incomplete data.
use table_changes_db;
create publication table_changes_pub
database table_changes_db account table_changes_acc;
-- @session:id=2&user=table_changes_acc:admin&password=111
create database table_changes_subscription
from sys publication table_changes_pub;
select * from table_changes(
    'table_changes_subscription', 'ordinary_single_pk', '', '1-0'
) c;
drop database table_changes_subscription;
-- @session
drop publication table_changes_pub;

use mo_catalog;
create cluster table unsupported_cluster_key (id int primary key);
select * from table_changes(
    'mo_catalog', 'unsupported_cluster_key', '', '1-0'
) c;
drop table unsupported_cluster_key;

use table_changes_db;
create table no_primary_key (id int);
select * from table_changes('table_changes_db', 'no_primary_key', '', '1-0') c;

create temporary table temporary_table (id int primary key);
select * from table_changes('table_changes_db', 'temporary_table', '', '1-0') c;

create table partitioned_table (
    id int primary key
) partition by key(id) partitions 2;
select * from table_changes('table_changes_db', 'partitioned_table', '', '1-0') c;

create view changes_view as select id from ordinary_single_pk;
select * from table_changes('table_changes_db', 'changes_view', '', '1-0') c;

create external table external_table (id int)
infile{"filepath"='$resources/external_table_file/cpk_table_1.csv'}
fields terminated by ',';
select * from table_changes('table_changes_db', 'external_table', '', '1-0') c;

-- Invalid intervals and non-literal relation names are rejected.
select * from table_changes('table_changes_db', 'ordinary_single_pk', '2-0', '1-0') c;
set @database_name = 'table_changes_db';
select * from table_changes(@database_name, 'ordinary_single_pk', '', '1-0') c;

drop database table_changes_catalog_marker;
drop database table_changes_db;
drop account table_changes_acc;
use mo_catalog;
drop table table_changes_cluster;
