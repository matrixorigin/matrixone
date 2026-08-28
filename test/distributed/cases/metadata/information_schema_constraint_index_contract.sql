-- @suite
-- @case
-- @desc: information_schema exposes logical constraints and indexes, not physical index tables
-- @label:bvt

set experimental_ivf_index = 1;

drop database if exists information_schema_constraint_index_contract;
create database information_schema_constraint_index_contract;
use information_schema_constraint_index_contract;

create table parent (
    id int primary key,
    code varchar(20),
    unique key uq_code (code)
);
create table child (
    id int primary key,
    pid int,
    a int,
    b int,
    unique key uq_ab (a, b),
    key idx_b (b),
    constraint chk_a check (a >= 0),
    constraint fk_pid foreign key (pid) references parent (id)
);
create table composite_key (
    a int,
    b int,
    c int,
    primary key (a, b),
    key idx_c (c)
);
create table abmoxindexyfoo (
    id int primary key,
    code int,
    v int,
    unique key uq_collision_code (code),
    key idx_collision_v (v)
);
create table vec (
    id int primary key,
    embedding vecf32(3)
);
create index vidx using ivfflat on vec (embedding)
    lists = 2 op_type 'vector_l2_ops';

select table_name, constraint_name, column_name, ordinal_position,
       position_in_unique_constraint, referenced_table_name, referenced_column_name
from information_schema.key_column_usage
where table_schema = 'information_schema_constraint_index_contract'
order by lower(table_name), lower(constraint_name), constraint_name, ordinal_position;

select table_name, constraint_name, constraint_type
from information_schema.table_constraints
where table_schema = 'information_schema_constraint_index_contract'
order by table_name, constraint_type, constraint_name;

select table_name, column_name, column_key
from information_schema.columns
where table_schema = 'information_schema_constraint_index_contract'
  and table_name in ('parent', 'child', 'composite_key', 'abmoxindexyfoo', 'vec')
order by table_name, ordinal_position;

select count(*) as hidden_column_rows
from information_schema.columns
where table_schema = 'information_schema_constraint_index_contract'
  and startswith(table_name, '__mo_index_');

select index_name, seq_in_index, column_name, non_unique, index_type
from information_schema.statistics
where table_schema = 'information_schema_constraint_index_contract'
  and table_name = 'vec'
  and index_name = 'vidx';

select count(*) as logical_ivf_rows
from information_schema.statistics
where table_schema = 'information_schema_constraint_index_contract'
  and table_name = 'vec'
  and index_name = 'vidx';

select index_name, seq_in_index, column_name, non_unique
from information_schema.statistics
where table_schema = 'information_schema_constraint_index_contract'
  and table_name = 'abmoxindexyfoo'
order by lower(index_name), index_name, seq_in_index;

drop database information_schema_constraint_index_contract;
