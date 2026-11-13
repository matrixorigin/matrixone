-- @suit
-- @case
-- @desc: #32463 repeated schema rewrites do not amplify catalog properties
-- @label:bvt

drop snapshot if exists alter_catalog_before_flush;
drop database if exists alter_table_catalog_properties;
create database alter_table_catalog_properties;
use alter_table_catalog_properties;

create table parents (id int primary key);
insert into parents values (1);
create table events (
    id int primary key,
    parent_id int,
    payload int,
    u int,
    unique key uk_u(u),
    key idx_parent(parent_id),
    constraint fk_parent foreign key (parent_id) references parents(id)
) comment = 'catalog properties';
insert into events values (1,1,12,101);

-- The conversion uses COPY. Warm up the replacement's catalog metadata before
-- comparing identical later rewrites; do not freeze its encoded byte count.
alter table events modify payload varchar(25) not null;
alter table events modify payload varchar(25) not null;
set @constraint_bytes = (select length(`constraint`) from mo_catalog.mo_tables
    where reldatabase = database() and relname = 'events');
alter table events modify payload varchar(25) not null;
alter table events modify payload varchar(25) not null;
select length(`constraint`) = @constraint_bytes as bounded_properties,
    relkind = 'r' as ordinary_table,
    rel_comment = 'catalog properties' as preserved_comment
from mo_catalog.mo_tables where reldatabase = database() and relname = 'events';
show create table events;
select id, parent_id, payload, u from events where parent_id = 1 and u = 101;

-- Key and reference constraints still reject the nearest invalid writes.
insert into events values (1,1,'duplicate primary',102);
insert into events values (2,1,'duplicate unique',101);
insert into events values (2,999,'missing parent',102);
delete from parents where id = 1;
select id, parent_id, payload, u from events order by id;

-- A snapshot pinned before catalog flush must not resurrect old table/column
-- versions when its logtail is reconstructed from persisted appendable objects.
create snapshot alter_catalog_before_flush for cluster;
select count(*) as table_versions from mo_catalog.mo_tables{snapshot='alter_catalog_before_flush'}
    where account_id = 0 and reldatabase = 'alter_table_catalog_properties' and relname = 'events';
select count(*) as id_columns from mo_catalog.mo_columns{snapshot='alter_catalog_before_flush'}
    where account_id = 0 and att_database = 'alter_table_catalog_properties' and att_relname = 'events' and attname = 'id';
set @catalog_flush_result = mo_ctl('dn', 'flush', 'mo_catalog.mo_tables');
set @catalog_flush_result = mo_ctl('dn', 'flush', 'mo_catalog.mo_columns');
select count(*) as table_versions from mo_catalog.mo_tables{snapshot='alter_catalog_before_flush'}
    where account_id = 0 and reldatabase = 'alter_table_catalog_properties' and relname = 'events';
select count(*) as id_columns from mo_catalog.mo_columns{snapshot='alter_catalog_before_flush'}
    where account_id = 0 and att_database = 'alter_table_catalog_properties' and att_relname = 'events' and attname = 'id';
drop snapshot alter_catalog_before_flush;

drop database alter_table_catalog_properties;
