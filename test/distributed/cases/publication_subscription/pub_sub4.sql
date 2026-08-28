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
create account idx_meta_sub ADMIN_NAME 'admin' IDENTIFIED BY '111';
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
create database idx_meta_sub_db from sys publication idx_meta_pub;
create database idx_meta_sub_b from sys publication idx_meta_pub_b;
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
drop database idx_meta_sub_db;
drop database idx_meta_sub_b;
drop database idx_meta_local;
-- @session

drop publication idx_meta_pub;
drop publication idx_meta_pub_b;
drop database idx_meta_src;
drop database idx_meta_src_b;
drop account idx_meta_sub;
