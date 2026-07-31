-- @suite
-- @case
-- @desc: verify KEY_COLUMN_USAGE view metadata and foreign-key mappings
-- @label:bvt

drop database if exists fk_information_schema_key_column_usage;
create database fk_information_schema_key_column_usage;
use fk_information_schema_key_column_usage;
create table parent (id int, code int, primary key (id, code));
create table child (
    id int primary key,
    parent_id int,
    parent_code int,
    constraint fk_metadata_compound foreign key (parent_id, parent_code)
        references parent(id, code)
);
select CONSTRAINT_SCHEMA as constraint_schema, CONSTRAINT_NAME as constraint_name, TABLE_NAME as table_name, COLUMN_NAME as column_name, REFERENCED_TABLE_NAME as referenced_table_name, REFERENCED_COLUMN_NAME as referenced_column_name, ORDINAL_POSITION as ordinal_position
from information_schema.KEY_COLUMN_USAGE
where TABLE_SCHEMA = database() and REFERENCED_TABLE_NAME is not null
order by CONSTRAINT_NAME, ORDINAL_POSITION;
desc information_schema.KEY_COLUMN_USAGE;
show create table information_schema.KEY_COLUMN_USAGE;
drop database fk_information_schema_key_column_usage;
