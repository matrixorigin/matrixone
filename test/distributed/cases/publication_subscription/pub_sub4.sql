create account acc1 ADMIN_NAME 'admin1' IDENTIFIED BY '111';

drop database if exists db1;
create database db1;
-- pub an empty database
create publication pub_all database db1 account all;

-- @session:id=1&user=acc1:admin1&password=111
create database syssub1 from sys publication pub_all;
-- @ignore:3,4,5,7,9,10,11,12
show table status from syssub1;
-- @session

drop publication pub_all;
drop database db1;
drop account acc1;

-- issue 27759: subscription index metadata must use the publisher catalog
drop publication if exists idx_meta_pub;
drop publication if exists idx_meta_pub_b;
drop database if exists idx_meta_src;
drop database if exists idx_meta_src_b;
drop account if exists idx_meta_sub;
drop account if exists idx_meta_other;
create account idx_meta_sub ADMIN_NAME 'admin' IDENTIFIED BY '111';
create account idx_meta_other ADMIN_NAME 'admin' IDENTIFIED BY '111';
create database idx_meta_src;
create table idx_meta_src.visible_t(
    id int primary key,
    u int,
    v varchar(20),
    unique key uk_u(u),
    key idx_v(v)
);
create table idx_meta_src.unpublished_t(id int primary key);
create publication idx_meta_pub database idx_meta_src table visible_t account idx_meta_sub;
create database idx_meta_src_b;
create table idx_meta_src_b.second_t(id int primary key);
create publication idx_meta_pub_b database idx_meta_src_b table second_t account idx_meta_sub;

-- @session:id=2&user=idx_meta_sub:admin&password=111
-- Repeating this exact ordinary COM_QUERY before and after the first
-- subscription is created must not reuse the zero-subscription branch set.
select count(*) as ordinary_cache_first_subscription_rows
from information_schema.statistics where table_name = 'visible_t';
create database idx_meta_sub_db from sys publication idx_meta_pub;
select count(*) as ordinary_cache_first_subscription_rows
from information_schema.statistics where table_name = 'visible_t';
create database idx_meta_sub_b from sys publication idx_meta_pub_b;

-- In case-sensitive identifier mode these are two distinct subscription
-- schemas. STATISTICS must retain both publisher branches instead of folding
-- their names together during metadata enumeration.
set lower_case_table_names = 0;
create database IdxMetaCase from sys publication idx_meta_pub;
create database idxmetacase from sys publication idx_meta_pub_b;
select count(*) as upper_case_subscription_index_rows
from information_schema.statistics
where table_schema = 'IdxMetaCase' and table_name = 'visible_t';
select count(*) as lower_case_subscription_index_rows
from information_schema.statistics
where table_schema = 'idxmetacase' and table_name = 'second_t';
drop database IdxMetaCase;
drop database idxmetacase;
set lower_case_table_names = 1;

create database idx_meta_local;
create table idx_meta_local.local_t(id int primary key, v int, key idx_local(v));
show index from idx_meta_sub_db.visible_t;
select table_schema, table_name, non_unique, index_name, seq_in_index, column_name, index_type, is_visible
from information_schema.statistics
where table_schema = 'idx_meta_sub_db' and table_name = 'visible_t'
order by non_unique, index_name, seq_in_index;
select count(*) as unpublished_index_count
from information_schema.statistics
where table_schema = 'idx_meta_sub_db' and table_name = 'unpublished_t';
select count(*) as nested_subscription_index_count
from information_schema.statistics s
where s.table_schema = 'idx_meta_sub_db' and s.table_name = 'visible_t'
  and exists (
      select 1 from information_schema.statistics t
      where t.table_schema = 'idx_meta_sub_b' and t.table_name = 'second_t'
  );
select count(*) as sibling_subscription_index_count
from information_schema.statistics a
join information_schema.statistics b on a.seq_in_index = b.seq_in_index
where a.table_schema = 'idx_meta_sub_db' and a.table_name = 'visible_t'
  and b.table_schema = 'idx_meta_sub_b' and b.table_name = 'second_t';
select index_name
from information_schema.statistics s
where s.table_schema = 'idx_meta_sub_db' and s.table_name = 'visible_t'
  and (s.index_name = 'PRIMARY' or s.non_unique = 1)
order by index_name;

-- Connector/J 8.0.33 uses prepared INFORMATION_SCHEMA.STATISTICS queries for
-- DatabaseMetaData.getIndexInfo() and DatabaseMetaData.getPrimaryKeys().
use idx_meta_local;
select count(*) as account_wide_subscription_rows
from information_schema.statistics where table_name = 'visible_t';
select count(*) as account_wide_local_rows
from information_schema.statistics where table_name = 'local_t';
select count(*) as join_on_subscription_rows
from information_schema.statistics s
join mo_catalog.mo_database d
  on s.table_schema = 'idx_meta_sub_db' and d.datname = s.table_schema
where s.table_name = 'visible_t';
select count(*) as derived_subscription_rows
from (select table_schema, table_name, index_name from information_schema.statistics) s
where s.table_schema = 'idx_meta_sub_db' and s.table_name = 'visible_t';
select count(*) as account_wide_unpublished_rows
from information_schema.statistics where table_name = 'unpublished_t';
set @jdbc_schema = 'idx_meta_sub_db';
set @jdbc_table = 'visible_t';
prepare jdbc_index_info from
    'select table_schema as table_cat, null as table_schem, table_name, non_unique,
            index_name, seq_in_index as ordinal_position, column_name
     from information_schema.statistics
     where table_schema = ? and table_name = ?
     order by non_unique, index_name, seq_in_index';
execute jdbc_index_info using @jdbc_schema, @jdbc_table;
set @jdbc_schema = 'idx_meta_local';
set @jdbc_table = 'local_t';
execute jdbc_index_info using @jdbc_schema, @jdbc_table;
set @jdbc_schema = 'idx_meta_sub_db';
set @jdbc_table = 'visible_t';
execute jdbc_index_info using @jdbc_schema, @jdbc_table;
deallocate prepare jdbc_index_info;

prepare jdbc_primary_keys from
    'select table_schema as table_cat, null as table_schem, table_name, column_name,
            seq_in_index as key_seq, \'PRIMARY\' as pk_name
     from information_schema.statistics
     where table_schema = ? and table_name = ? and index_name = \'PRIMARY\'
     order by table_schema, table_name, column_name, seq_in_index';
execute jdbc_primary_keys using @jdbc_schema, @jdbc_table;
set @jdbc_schema = 'idx_meta_local';
set @jdbc_table = 'local_t';
execute jdbc_primary_keys using @jdbc_schema, @jdbc_table;
deallocate prepare jdbc_primary_keys;

-- Historical STATISTICS must enumerate subscriptions from the requested
-- snapshot, not from the current session transaction.
drop snapshot if exists idx_meta_with_sub_b;
create snapshot idx_meta_with_sub_b for account idx_meta_sub;
drop database idx_meta_sub_b;
select count(*) as current_dropped_subscription_rows
from information_schema.statistics
where table_schema = 'idx_meta_sub_b' and table_name = 'second_t';
select count(*) as historical_subscription_rows
from information_schema.statistics {snapshot = 'idx_meta_with_sub_b'}
where table_schema = 'idx_meta_sub_b' and table_name = 'second_t';
drop snapshot if exists idx_meta_without_sub_b;
create snapshot idx_meta_without_sub_b for account idx_meta_sub;

-- A prepared STATISTICS plan must refresh its complete subscription set after
-- create, withdraw, reauthorize, and drop transitions.
set @membership_schema = 'idx_meta_sub_b';
set @membership_table = 'second_t';
prepare subscription_membership_stmt from
    'select count(*) as prepared_subscription_rows
     from information_schema.statistics
     where table_schema = ? and table_name = ?';
execute subscription_membership_stmt using @membership_schema, @membership_table;
create database idx_meta_sub_b from sys publication idx_meta_pub_b;
execute subscription_membership_stmt using @membership_schema, @membership_table;
select count(*) as historical_absent_subscription_rows
from information_schema.statistics {snapshot = 'idx_meta_without_sub_b'}
where table_schema = 'idx_meta_sub_b' and table_name = 'second_t';
-- @session
alter publication idx_meta_pub_b account idx_meta_other;
-- @session:id=2&user=idx_meta_sub:admin&password=111
execute subscription_membership_stmt using @membership_schema, @membership_table;
-- @session
alter publication idx_meta_pub_b account idx_meta_sub;
-- @session:id=2&user=idx_meta_sub:admin&password=111
execute subscription_membership_stmt using @membership_schema, @membership_table;
drop database idx_meta_sub_b;
execute subscription_membership_stmt using @membership_schema, @membership_table;
deallocate prepare subscription_membership_stmt;
drop snapshot idx_meta_with_sub_b;
drop snapshot idx_meta_without_sub_b;
drop database idx_meta_sub_db;
drop database idx_meta_local;
-- @session

drop publication idx_meta_pub;
drop publication idx_meta_pub_b;
drop database idx_meta_src;
drop database idx_meta_src_b;
drop account idx_meta_sub;
drop account idx_meta_other;
