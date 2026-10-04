-- @suit
-- @case
-- @desc:information_schema.CHECK_CONSTRAINTS exposes decoded CHECK metadata
-- @label:bvt

drop database if exists check_constraints_metadata;
create database check_constraints_metadata;
use check_constraints_metadata;

create table no_check_values (
    id int primary key
);

create table check_values (
    id int primary key,
    amount int constraint chk_amount_nonnegative check (amount >= 0),
    constraint chk_amount_positive check (amount > 0),
    constraint chk_amount_limit check (amount < 100)
);

use information_schema;
select constraint_catalog, constraint_schema, constraint_name, check_clause
from check_constraints
where constraint_schema = 'check_constraints_metadata'
order by constraint_name;

-- LIMIT must be honored by the metadata table function before the catalog
-- stream is exhausted.
select count(*)
from (
    select constraint_name
    from check_constraints
    where constraint_schema = 'check_constraints_metadata'
    limit 1
) limited_checks;

use check_constraints_metadata;
create table partitioned_values (
    id int,
    constraint chk_partition_positive check (id >= 0)
) partition by range columns (id) (
    partition p0 values less than (10),
    partition p1 values less than (20)
);

use information_schema;
select count(*)
from check_constraints
where constraint_schema = 'check_constraints_metadata'
  and constraint_name = 'chk_partition_positive';
select count(*)
from table_constraints
where constraint_schema = 'check_constraints_metadata'
  and constraint_name = 'chk_partition_positive';

use check_constraints_metadata;
create temporary table check_values_tmp (
    id int primary key,
    amount int,
    constraint chk_tmp_positive check (amount > 0)
);

use information_schema;
select count(*)
from check_constraints
where constraint_schema = 'check_constraints_metadata'
  and constraint_name = 'chk_tmp_positive';

use check_constraints_metadata;
drop temporary table check_values_tmp;
drop database check_constraints_metadata;
