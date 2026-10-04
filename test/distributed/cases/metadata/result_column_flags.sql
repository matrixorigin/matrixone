-- @suit
-- @case
-- @desc: result-set columns retain primary, unique, not-null and auto-increment metadata
-- @label:bvt

-- The SQL-visible constraint query is the BVT control; the exact MySQL
-- ColumnDefinition flag bits are asserted by the frontend protocol unit test.

drop database if exists result_column_flags_bvt;
create database result_column_flags_bvt;
use result_column_flags_bvt;

create table result_column_flags (
    id int not null auto_increment,
    unique_value int not null,
    nullable_value varchar(32),
    plain_value int,
    primary key (id),
    unique key uk_result_column_flags (unique_value)
);

insert into result_column_flags (unique_value, nullable_value, plain_value)
values (10, 'first', 100), (20, null, 200);

-- Direct source columns after filter/sort retain their source metadata.
select id, unique_value, nullable_value, plain_value
from result_column_flags
where id > 0
order by id;

-- A projection through a derived table still exposes source-column metadata.
select id, unique_value, nullable_value
from (
    select id, unique_value, nullable_value
    from result_column_flags
) as projected
order by id;

-- Computed expressions must not inherit key flags from their input column.
select id + 1 as id_plus_one, unique_value, nullable_value
from result_column_flags
order by unique_value;

-- SQL-visible controls for the same constraints.
select column_name, column_key, is_nullable, extra
from information_schema.columns
where table_schema = 'result_column_flags_bvt'
  and table_name = 'result_column_flags'
order by ordinal_position;

drop table result_column_flags;
drop database result_column_flags_bvt;
