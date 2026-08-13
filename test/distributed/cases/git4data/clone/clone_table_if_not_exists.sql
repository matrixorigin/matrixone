-- issue#27039: an existing CREATE TABLE IF NOT EXISTS ... CLONE target is a no-op.
drop database if exists clone_table_if_not_exists;
create database clone_table_if_not_exists;
use clone_table_if_not_exists;

-- Overlapping primary keys must not bypass uniqueness by appending clone objects.
create table src_primary (id int primary key, v varchar(20));
insert into src_primary values (1, 'src1'), (2, 'src2');
create table dst_primary (id int primary key, v varchar(20));
insert into dst_primary values (1, 'keep1'), (99, 'keep99');
show create table dst_primary;
create table if not exists dst_primary clone src_primary;
select id, v from dst_primary order by id, v;
select count(*) as duplicate_primary_keys
from (
    select id
    from dst_primary
    group by id
    having count(*) > 1
) duplicate_keys;
show create table dst_primary;
select id, v from src_primary order by id, v;

-- A schema-incompatible source cannot affect an existing target either.
create table src_incompatible (src_id int primary key, src_payload varchar(20));
insert into src_incompatible values (1, 'source');
create table dst_incompatible (dst_key varchar(20) primary key, dst_count int);
insert into dst_incompatible values ('keep', 7);
show create table dst_incompatible;
create table if not exists dst_incompatible clone src_incompatible;
select dst_key, dst_count from dst_incompatible order by dst_key;
show create table dst_incompatible;

-- The destination's secondary-index metadata and physical index table stay unchanged.
create table src_secondary (
    id int primary key,
    source_key varchar(20),
    source_payload int,
    key src_secondary_idx (source_key)
);
insert into src_secondary values (1, 'source-one', 10), (2, 'source-two', 20);
create table dst_secondary (
    id int primary key,
    destination_key varchar(20),
    destination_payload int,
    key dst_secondary_idx (destination_key)
);
insert into dst_secondary values (1, 'keep-secondary', 7), (99, 'keep-secondary-99', 9);
set @dst_secondary_index_table = (
    select distinct index_table_name
    from mo_catalog.mo_indexes
    where name = 'dst_secondary_idx'
      and table_id in (
          select rel_id
          from mo_catalog.mo_tables
          where reldatabase = database()
            and relname = 'dst_secondary'
      )
    limit 1
);
select @dst_secondary_index_table is not null as secondary_index_table_exists;
set @dst_secondary_index_sql = concat(
    'select count(*) as physical_rows, ',
    'count(distinct __mo_index_idx_col) as distinct_keys, ',
    'count(distinct __mo_index_pri_col) as distinct_primary_keys ',
    'from `', database(), '`.`', @dst_secondary_index_table, '`'
);
prepare dst_secondary_index_check from @dst_secondary_index_sql;
execute dst_secondary_index_check;
show create table dst_secondary;
create table if not exists dst_secondary clone src_secondary;
select id, destination_key, destination_payload from dst_secondary order by id, destination_key;
select id, destination_key, destination_payload
from dst_secondary force index(dst_secondary_idx)
where destination_key = 'keep-secondary';
show index from dst_secondary;
show create table dst_secondary;
execute dst_secondary_index_check;
deallocate prepare dst_secondary_index_check;
select count(distinct index_table_name) as hidden_index_tables
from mo_catalog.mo_indexes
where name = 'dst_secondary_idx'
  and table_id in (
      select rel_id
      from mo_catalog.mo_tables
      where reldatabase = database()
        and relname = 'dst_secondary'
      );

-- Temporary tables take the same IF NOT EXISTS no-op path.
create temporary table src_temporary (id int primary key, v varchar(20));
insert into src_temporary values (1, 'temporary-source');
create temporary table dst_temporary (id int primary key, v varchar(20));
insert into dst_temporary values (1, 'temp-destination'), (99, 'temp-keep');
show create table dst_temporary;
create temporary table if not exists dst_temporary clone src_temporary;
select id, v from dst_temporary order by id, v;
select count(*) as duplicate_primary_keys
from (
    select id
    from dst_temporary
    group by id
    having count(*) > 1
) duplicate_keys;
show create table dst_temporary;

-- Without IF NOT EXISTS the normal exists error remains atomic.
create table dst_primary clone src_primary;
select id, v from dst_primary order by id, v;
select count(*) as duplicate_primary_keys
from (
    select id
    from dst_primary
    group by id
    having count(*) > 1
) duplicate_keys;
show create table dst_primary;

-- IF NOT EXISTS still performs a normal clone when it creates the target.
create table if not exists dst_created clone src_primary;
select id, v from dst_created order by id, v;
show create table dst_created;

drop database clone_table_if_not_exists;
