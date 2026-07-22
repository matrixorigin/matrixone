drop database if exists prepare_ddl_schema_change;
create database prepare_ddl_schema_change;
use prepare_ddl_schema_change;

create table prepared_t (
    id int primary key,
    v int
);

prepare add_column_stmt from
    'alter table prepared_t add column note varchar(20) default ''x''';
execute add_column_stmt;
execute add_column_stmt;
select column_name
from information_schema.columns
where table_schema = 'prepare_ddl_schema_change' and table_name = 'prepared_t'
order by ordinal_position;
alter table prepared_t drop column note;
execute add_column_stmt;
select column_name
from information_schema.columns
where table_schema = 'prepare_ddl_schema_change' and table_name = 'prepared_t'
order by ordinal_position;
deallocate prepare add_column_stmt;

create index idx_v on prepared_t(v);
prepare drop_column_stmt from 'alter table prepared_t drop column note';
execute drop_column_stmt;
execute drop_column_stmt;
select column_name
from information_schema.columns
where table_schema = 'prepare_ddl_schema_change' and table_name = 'prepared_t'
order by ordinal_position;
deallocate prepare drop_column_stmt;

prepare create_index_stmt from 'create index idx_created on prepared_t(v)';
execute create_index_stmt;
execute create_index_stmt;
show index from prepared_t;
deallocate prepare create_index_stmt;

prepare drop_index_stmt from 'drop index idx_created on prepared_t';
execute drop_index_stmt;
execute drop_index_stmt;
show index from prepared_t;
deallocate prepare drop_index_stmt;

drop database prepare_ddl_schema_change;
