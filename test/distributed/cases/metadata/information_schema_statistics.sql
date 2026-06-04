drop account if exists stat_view_acc;
drop database if exists stat_view_db;

create database stat_view_db;
use stat_view_db;
create table t (
    a int primary key,
    b int,
    c int,
    unique key uniq_b (b),
    key idx_bc (b, c)
);

select table_schema, table_name, index_name, column_name, seq_in_index, non_unique
from information_schema.statistics
where table_schema = 'stat_view_db'
  and table_name = 't'
order by index_name, seq_in_index;

select count(*) from information_schema.statistics
where table_schema = 'stat_view_db'
  and table_name = 't';

show index from t;

create account stat_view_acc admin_name = 'root' identified by '111';

-- @session:id=2&user=stat_view_acc:root&password=111
create database stat_view_db;
use stat_view_db;
create table t (
    a int primary key,
    b int,
    c int,
    unique key uniq_b (b),
    key idx_bc (b, c)
);

select table_schema, table_name, index_name, column_name, seq_in_index, non_unique
from information_schema.statistics
where table_schema = 'stat_view_db'
  and table_name = 't'
order by index_name, seq_in_index;

select count(*) from information_schema.statistics
where table_schema = 'stat_view_db'
  and table_name = 't';

-- @session
select count(*) from information_schema.statistics
where table_schema = 'stat_view_db';

-- @session:id=2&user=stat_view_acc:root&password=111
drop database stat_view_db;
-- @session

select table_schema, table_name, index_name, column_name, seq_in_index, non_unique
from information_schema.statistics
where table_schema = 'stat_view_db'
  and table_name = 't'
order by index_name, seq_in_index;

select count(*) from information_schema.statistics
where table_schema = 'stat_view_db'
  and table_name = 't';

drop account if exists stat_view_acc;
drop database if exists stat_view_db;
