select information_schema.REFERENTIAL_CONSTRAINTS.CONSTRAINT_SCHEMA,
       information_schema.REFERENTIAL_CONSTRAINTS.CONSTRAINT_NAME,
       information_schema.REFERENTIAL_CONSTRAINTS.TABLE_NAME,
       information_schema.REFERENTIAL_CONSTRAINTS.REFERENCED_TABLE_NAME,
       information_schema.REFERENTIAL_CONSTRAINTS.UNIQUE_CONSTRAINT_NAME,
       information_schema.REFERENTIAL_CONSTRAINTS.UNIQUE_CONSTRAINT_SCHEMA,
       information_schema.KEY_COLUMN_USAGE.COLUMN_NAME
from information_schema.REFERENTIAL_CONSTRAINTS
         join information_schema.KEY_COLUMN_USAGE
              on (information_schema.REFERENTIAL_CONSTRAINTS.CONSTRAINT_SCHEMA =
                  information_schema.KEY_COLUMN_USAGE.CONSTRAINT_SCHEMA and
                  information_schema.REFERENTIAL_CONSTRAINTS.CONSTRAINT_NAME =
                  information_schema.KEY_COLUMN_USAGE.CONSTRAINT_NAME and
                  information_schema.REFERENTIAL_CONSTRAINTS.TABLE_NAME =
                  information_schema.KEY_COLUMN_USAGE.TABLE_NAME)
where (information_schema.REFERENTIAL_CONSTRAINTS.CONSTRAINT_SCHEMA in ('plat_content') or
       information_schema.REFERENTIAL_CONSTRAINTS.CONSTRAINT_SCHEMA in ('plat_content'))
order by information_schema.KEY_COLUMN_USAGE.CONSTRAINT_SCHEMA asc,
         information_schema.KEY_COLUMN_USAGE.CONSTRAINT_NAME asc,
         information_schema.KEY_COLUMN_USAGE.ORDINAL_POSITION asc;

select table_catalog, table_schema, table_name, table_type, engine
from information_schema.tables
where table_schema = 'mo_catalog' and table_type = 'BASE TABLE'
order by table_name;

drop database if exists information_schema_data_type_case;
create database information_schema_data_type_case;
create table information_schema_data_type_case.type_probe (
    c_tiny tinyint,
    c_small smallint,
    c_int int,
    c_big bigint,
    c_tiny_unsigned tinyint unsigned,
    c_small_unsigned smallint unsigned,
    c_int_unsigned int unsigned,
    c_big_unsigned bigint unsigned,
    c_dec decimal(20, 6),
    c_float float,
    c_double double,
    c_bool bool,
    c_date date,
    c_time time,
    c_datetime datetime,
    c_timestamp timestamp,
    c_char char(8),
    c_varchar varchar(32),
    c_text text,
    c_binary binary(8),
    c_varbinary varbinary(32),
    c_blob blob,
    c_json json,
    c_enum enum('a', 'b'),
    c_set set('a', 'b')
);
select column_name, data_type
from information_schema.columns
where table_schema = 'information_schema_data_type_case'
  and table_name = 'type_probe'
  and column_name not like '__mo%'
order by ordinal_position;
select column_name, data_type, column_type
from information_schema.columns
where table_schema = 'information_schema_data_type_case'
  and table_name = 'type_probe'
  and column_name in ('c_bool', 'c_tiny_unsigned', 'c_small_unsigned', 'c_int_unsigned', 'c_big_unsigned')
order by ordinal_position;
drop database information_schema_data_type_case;
