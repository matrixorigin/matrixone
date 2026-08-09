-- @suit
-- @case
-- @desc:information_schema.CHECK_CONSTRAINTS exposes decoded CHECK metadata
-- @label:bvt

drop database if exists check_constraints_metadata;
create database check_constraints_metadata;
use check_constraints_metadata;

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
