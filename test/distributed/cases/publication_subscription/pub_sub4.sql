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
drop database if exists idx_meta_src;
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

-- @session:id=2&user=idx_meta_sub:admin&password=111
create database idx_meta_sub_db from sys publication idx_meta_pub;
show index from idx_meta_sub_db.visible_t;
select table_schema, table_name, non_unique, index_name, seq_in_index, column_name, index_type, is_visible
from information_schema.statistics
where table_schema = 'idx_meta_sub_db' and table_name = 'visible_t'
order by non_unique, index_name, seq_in_index;
select count(*) as unpublished_index_count
from information_schema.statistics
where table_schema = 'idx_meta_sub_db' and table_name = 'unpublished_t';

-- Connector/J 8.0.33 uses prepared INFORMATION_SCHEMA.STATISTICS queries for
-- DatabaseMetaData.getIndexInfo() and DatabaseMetaData.getPrimaryKeys().
use idx_meta_sub_db;
set @jdbc_schema = 'idx_meta_sub_db';
set @jdbc_table = 'visible_t';
prepare jdbc_index_info from
    'select table_schema as table_cat, null as table_schem, table_name, non_unique,
            index_name, seq_in_index as ordinal_position, column_name
     from information_schema.statistics
     where table_schema = ? and table_name = ?
     order by non_unique, index_name, seq_in_index';
execute jdbc_index_info using @jdbc_schema, @jdbc_table;
deallocate prepare jdbc_index_info;

prepare jdbc_primary_keys from
    'select table_schema as table_cat, null as table_schem, table_name, column_name,
            seq_in_index as key_seq, \'PRIMARY\' as pk_name
     from information_schema.statistics
     where table_schema = ? and table_name = ? and index_name = \'PRIMARY\'
     order by table_schema, table_name, column_name, seq_in_index';
execute jdbc_primary_keys using @jdbc_schema, @jdbc_table;
deallocate prepare jdbc_primary_keys;
drop database idx_meta_sub_db;
-- @session

drop publication idx_meta_pub;
drop database idx_meta_src;
drop account idx_meta_sub;
